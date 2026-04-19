"""Deterministic sharding helpers for large website enrichment jobs."""

from __future__ import annotations

import hashlib
from urllib.parse import urlparse

import pandas as pd

from back_end.services.website_profiles import _normalize_website_url

VALID_SHARD_KEYS = ("website", "domain", "fsq_place_id")


def shard_places_dataframe(
    places: pd.DataFrame,
    *,
    shard_count: int,
    shard_index: int,
    shard_key: str = "website",
) -> pd.DataFrame:
    """Return the subset of rows assigned to a deterministic shard."""

    if shard_count <= 0:
        raise ValueError(f"shard_count must be positive. Got {shard_count}.")
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError(
            f"shard_index must be in [0, {shard_count}). Got {shard_index}."
        )
    if shard_key not in VALID_SHARD_KEYS:
        raise ValueError(
            f"shard_key must be one of {VALID_SHARD_KEYS}. Got {shard_key!r}."
        )
    if "website" not in places.columns and shard_key != "fsq_place_id":
        raise ValueError("places DataFrame must contain a website column for sharding.")
    if shard_count == 1:
        return places.copy()

    shard_keys = build_shard_keys(places, shard_key=shard_key)
    shard_assignments = shard_keys.map(
        lambda key: stable_shard_index(key, shard_count=shard_count)
    )
    return places.loc[shard_assignments == shard_index].copy()


def build_shard_keys(
    places: pd.DataFrame,
    *,
    shard_key: str = "website",
) -> pd.Series:
    """Build stable shard keys from website or domain identity."""

    if "website" not in places.columns and shard_key != "fsq_place_id":
        raise ValueError("places DataFrame must contain a website column for sharding.")
    if shard_key not in VALID_SHARD_KEYS:
        raise ValueError(
            f"shard_key must be one of {VALID_SHARD_KEYS}. Got {shard_key!r}."
        )

    fallback_ids = places.get("fsq_place_id")
    if fallback_ids is None:
        fallback_ids = pd.Series(range(len(places)), index=places.index, dtype="object")

    if shard_key == "fsq_place_id":
        return fallback_ids.astype(str)

    keys = [
        _row_shard_key(
            website=website,
            fallback_id=fallback_id,
            shard_key=shard_key,
        )
        for website, fallback_id in zip(places["website"], fallback_ids, strict=False)
    ]
    return pd.Series(keys, index=places.index, dtype="object")


def stable_shard_index(key: str, *, shard_count: int) -> int:
    """Map a shard key onto a stable shard index."""

    if shard_count <= 0:
        raise ValueError(f"shard_count must be positive. Got {shard_count}.")
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=False) % shard_count


def shard_suffix(*, shard_count: int, shard_index: int) -> str:
    """Return a stable suffix for shard-specific output paths."""

    if shard_count <= 0:
        raise ValueError(f"shard_count must be positive. Got {shard_count}.")
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError(
            f"shard_index must be in [0, {shard_count}). Got {shard_index}."
        )
    width = max(2, len(str(shard_count)))
    return f"_shard-{shard_index:0{width}d}-of-{shard_count:0{width}d}"


def _row_shard_key(
    *,
    website: object,
    fallback_id: object,
    shard_key: str,
) -> str:
    normalized = _safe_normalize_website_url(website)
    if normalized is None:
        return f"place:{fallback_id}"
    if shard_key == "website":
        return normalized
    return _website_domain_key(normalized)


def _safe_normalize_website_url(value: object) -> str | None:
    try:
        return _normalize_website_url(value)
    except Exception:
        return None


def _website_domain_key(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc.casefold()
