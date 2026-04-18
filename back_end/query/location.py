"""Typed location parsing and radius filtering for local place queries."""

from __future__ import annotations

import logging
import math
import re

import pandas as pd

from back_end.catalog.repository import PlacesRepository
from back_end.query.errors import (
    LocationAmbiguityError,
    LocationResolutionError,
)
from back_end.query.models import LocationType, ResolvedLocation

logger = logging.getLogger(__name__)

POSTCODE_PATTERN = re.compile(r"^\d{4}$")
REGION_ALIASES: dict[str, str] = {
    "nsw": "nsw",
    "new south wales": "nsw",
    "vic": "vic",
    "victoria": "vic",
    "qld": "qld",
    "queensland": "qld",
    "sa": "sa",
    "south australia": "sa",
    "wa": "wa",
    "western australia": "wa",
    "tas": "tas",
    "tasmania": "tas",
    "act": "act",
    "australian capital territory": "act",
    "nt": "nt",
    "northern territory": "nt",
}


def _normalize_region(text: str | None) -> str | None:
    if text is None:
        return None
    normalized = text.strip().casefold()
    if not normalized:
        return None
    return REGION_ALIASES.get(normalized, normalized)


def _parse_location_text(location_text: str) -> tuple[LocationType, str, str | None]:
    stripped = location_text.strip()
    if not stripped:
        raise LocationResolutionError("Location must not be empty.")

    if POSTCODE_PATTERN.fullmatch(stripped):
        return (LocationType.POSTCODE, stripped, None)

    parts = [part.strip() for part in stripped.split(",")]
    if len(parts) > 2:
        raise LocationResolutionError(
            f"Could not parse location {location_text!r}. Use a postcode or 'locality, region'."
        )

    locality = parts[0]
    region = parts[1] if len(parts) == 2 else None
    if not locality:
        raise LocationResolutionError(
            f"Could not parse location {location_text!r}. Locality is empty."
        )
    return (LocationType.LOCALITY, locality, region)


def _median_anchor(matches: pd.DataFrame, input_text: str) -> tuple[float, float]:
    anchored = matches.dropna(subset=["latitude", "longitude"])
    if anchored.empty:
        raise LocationResolutionError(
            f"Location {input_text!r} matched places, but none had coordinates for radius filtering."
        )
    latitude = float(anchored["latitude"].median())
    longitude = float(anchored["longitude"].median())
    return latitude, longitude


class TypedLocationResolver:
    """Resolve a postcode or locality against the local parquet dataset."""

    def __init__(self, repository: PlacesRepository) -> None:
        self._repository = repository

    def resolve(self, location_text: str) -> ResolvedLocation:
        location_type, first_part, region_text = _parse_location_text(location_text)
        open_places = self._repository.open_places_df

        if location_type is LocationType.POSTCODE:
            matches = open_places.loc[open_places["postcode_norm"] == first_part].copy()
            if matches.empty:
                raise LocationResolutionError(
                    f"Postcode {first_part!r} did not match any open places in the parquet dataset."
                )
            anchor_latitude, anchor_longitude = _median_anchor(matches, location_text)
            matched_regions = tuple(
                sorted(str(region) for region in matches["region"].dropna().unique())
            )
            logger.info(
                "Resolved postcode %s to %d anchor places across regions=%s",
                first_part,
                len(matches),
                matched_regions,
            )
            return ResolvedLocation(
                input_text=location_text.strip(),
                location_type=LocationType.POSTCODE,
                locality=None,
                region=None,
                postcode=first_part,
                anchor_latitude=anchor_latitude,
                anchor_longitude=anchor_longitude,
                matched_place_count=len(matches),
                matched_regions=matched_regions,
            )

        locality_norm = first_part.casefold().strip()
        matches = open_places.loc[open_places["locality_norm"] == locality_norm].copy()
        if matches.empty:
            raise LocationResolutionError(
                f"Locality {first_part!r} did not match any open places in the parquet dataset."
            )

        explicit_region = _normalize_region(region_text)
        matched_regions = tuple(
            sorted(str(region) for region in matches["region"].dropna().unique())
        )
        matched_region_norms = {
            _normalize_region(str(region)): str(region)
            for region in matches["region"].dropna().unique()
        }

        if explicit_region is None and len(matched_regions) > 1:
            raise LocationAmbiguityError(
                f"Locality {first_part!r} is ambiguous across regions {list(matched_regions)}. "
                "Use 'locality, region' to disambiguate."
            )

        if explicit_region is not None:
            if explicit_region not in matched_region_norms:
                raise LocationResolutionError(
                    f"Locality {first_part!r} exists, but not in region {region_text!r}. "
                    f"Known regions: {list(matched_regions)}."
                )
            matches = matches.loc[matches["region_norm"] == explicit_region].copy()
            matched_regions = tuple(
                sorted(str(region) for region in matches["region"].dropna().unique())
            )

        anchor_latitude, anchor_longitude = _median_anchor(matches, location_text)
        logger.info(
            "Resolved locality %s to %d anchor places in regions=%s",
            first_part,
            len(matches),
            matched_regions,
        )
        return ResolvedLocation(
            input_text=location_text.strip(),
            location_type=LocationType.LOCALITY,
            locality=first_part.strip(),
            region=matched_regions[0] if matched_regions else None,
            postcode=None,
            anchor_latitude=anchor_latitude,
            anchor_longitude=anchor_longitude,
            matched_place_count=len(matches),
            matched_regions=matched_regions,
        )


class LocationFilter:
    """Apply radius filtering from a resolved location anchor."""

    @staticmethod
    def apply_radius(
        places: pd.DataFrame,
        resolved_location: ResolvedLocation,
        radius_km: float,
    ) -> pd.DataFrame:
        if radius_km <= 0:
            raise ValueError("radius_km must be positive.")

        before = len(places)
        with_coords = places.dropna(subset=["latitude", "longitude"]).copy()
        missing_coords = before - len(with_coords)
        if missing_coords:
            logger.warning(
                "Dropping %d places without coordinates before radius filtering.",
                missing_coords,
            )

        if with_coords.empty:
            logger.warning("Radius filtering received no places with coordinates.")
            with_coords["distance_km"] = pd.Series(dtype=float)
            return with_coords

        with_coords["distance_km"] = [
            _haversine_km(
                resolved_location.anchor_latitude,
                resolved_location.anchor_longitude,
                float(latitude),
                float(longitude),
            )
            for latitude, longitude in zip(
                with_coords["latitude"], with_coords["longitude"]
            )
        ]
        filtered = with_coords.loc[with_coords["distance_km"] <= radius_km].copy()
        filtered.sort_values(by=["distance_km", "name", "fsq_place_id"], inplace=True)
        logger.info(
            "Radius filter kept %d / %d places within %.2fkm of %s",
            len(filtered),
            before,
            radius_km,
            resolved_location.input_text,
        )
        return filtered


def _haversine_km(
    origin_latitude: float,
    origin_longitude: float,
    destination_latitude: float,
    destination_longitude: float,
) -> float:
    earth_radius_km = 6371.0088
    origin_lat_rad = math.radians(origin_latitude)
    origin_lon_rad = math.radians(origin_longitude)
    destination_lat_rad = math.radians(destination_latitude)
    destination_lon_rad = math.radians(destination_longitude)
    latitude_delta = destination_lat_rad - origin_lat_rad
    longitude_delta = destination_lon_rad - origin_lon_rad

    haversine = (
        math.sin(latitude_delta / 2) ** 2
        + math.cos(origin_lat_rad)
        * math.cos(destination_lat_rad)
        * math.sin(longitude_delta / 2) ** 2
    )
    return 2 * earth_radius_km * math.asin(math.sqrt(haversine))
