"""Repository layer for parquet-backed place queries."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from back_end.query.errors import DatasetValidationError
from back_end.query.settings import QuerySettings

logger = logging.getLogger(__name__)

REQUIRED_PLACE_COLUMNS: tuple[str, ...] = (
    "fsq_place_id",
    "name",
    "latitude",
    "longitude",
    "address",
    "locality",
    "region",
    "postcode",
    "fsq_category_ids",
    "fsq_category_labels",
    "date_closed",
)


@dataclass(frozen=True)
class DatasetHealthCheck:
    """Basic health information for the loaded places parquet."""

    path: Path
    row_count: int
    required_columns: tuple[str, ...]


def _coerce_category_ids(value: Any, place_id: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    if pd.api.types.is_list_like(value) and not isinstance(value, (str, bytes, dict)):
        return [str(item) for item in value if item is not None and not pd.isna(item)]
    raise DatasetValidationError(
        f"Place {place_id!r} has non-list-like fsq_category_ids={value!r}. "
        "The parquet schema is not in the expected shape."
    )


def _coerce_category_labels(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    if pd.api.types.is_list_like(value) and not isinstance(value, (str, bytes, dict)):
        return [str(item) for item in value if item is not None and not pd.isna(item)]
    raise DatasetValidationError(
        f"Encountered non-list-like fsq_category_labels={value!r}. "
        "The parquet schema is not in the expected shape."
    )


class PlacesRepository:
    """Loads and validates `data/au_places.parquet` once."""

    def __init__(self, settings: QuerySettings) -> None:
        self._settings = settings
        self._places_df: pd.DataFrame | None = None

    @property
    def health_check(self) -> DatasetHealthCheck:
        df = self.places_df
        return DatasetHealthCheck(
            path=self._settings.places_parquet_path,
            row_count=len(df),
            required_columns=REQUIRED_PLACE_COLUMNS,
        )

    @property
    def places_df(self) -> pd.DataFrame:
        if self._places_df is None:
            self._places_df = self._load_places()
        return self._places_df

    @property
    def open_places_df(self) -> pd.DataFrame:
        df = self.places_df
        return df.loc[df["date_closed"].isna()].copy()

    def _load_places(self) -> pd.DataFrame:
        path = self._settings.places_parquet_path
        return load_places_frame(path)


def load_places_frame(path: Path | str) -> pd.DataFrame:
    """Load and validate a places parquet into the repository-ready shape."""

    path = Path(path)
    if not path.exists():
        raise DatasetValidationError(
            f"Places parquet not found at {path}. Expected data/au_places.parquet."
        )

    df = pd.read_parquet(path, columns=list(REQUIRED_PLACE_COLUMNS))
    missing_columns = [col for col in REQUIRED_PLACE_COLUMNS if col not in df.columns]
    if missing_columns:
        raise DatasetValidationError(
            f"Places parquet at {path} is missing required columns {missing_columns}. "
            f"Got columns: {sorted(df.columns)}."
        )

    duplicate_count = int(df["fsq_place_id"].duplicated().sum())
    if duplicate_count:
        raise DatasetValidationError(
            f"Places parquet at {path} contains {duplicate_count} duplicate fsq_place_id values."
        )

    if df["fsq_place_id"].isna().any():
        missing_id_count = int(df["fsq_place_id"].isna().sum())
        raise DatasetValidationError(
            f"Places parquet at {path} contains {missing_id_count} rows with missing fsq_place_id."
        )

    logger.info("Loaded %d raw places from %s", len(df), path)

    df = df.copy()
    df["fsq_place_id"] = df["fsq_place_id"].astype(str)
    df["fsq_category_ids"] = [
        _coerce_category_ids(value, place_id)
        for place_id, value in zip(df["fsq_place_id"], df["fsq_category_ids"])
    ]
    df["fsq_category_labels"] = [
        _coerce_category_labels(value)
        for value in df["fsq_category_labels"]
    ]
    df["postcode_norm"] = df["postcode"].fillna("").astype(str).str.strip()
    df["locality_norm"] = df["locality"].fillna("").astype(str).str.strip().str.casefold()
    df["region_norm"] = df["region"].fillna("").astype(str).str.strip().str.casefold()

    missing_coordinate_rows = int(
        df["latitude"].isna().sum() + df["longitude"].isna().sum()
    )
    if missing_coordinate_rows:
        logger.warning(
            "Loaded places include %d missing coordinate values. "
            "Those rows cannot survive radius filtering.",
            missing_coordinate_rows,
        )

    return df
