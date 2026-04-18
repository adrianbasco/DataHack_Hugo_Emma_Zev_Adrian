"""Explicit failure types for the deterministic parquet query layer."""

from __future__ import annotations


class DateNightQueryError(Exception):
    """Base class for query-tool failures."""


class DatasetValidationError(DateNightQueryError):
    """Raised when a parquet dataset is missing or malformed."""


class ConstraintValidationError(DateNightQueryError):
    """Raised when the caller provides invalid query constraints."""


class LocationResolutionError(DateNightQueryError):
    """Raised when a typed location cannot be resolved cleanly."""


class LocationAmbiguityError(LocationResolutionError):
    """Raised when a locality matches multiple regions."""
