"""Load the Foursquare taxonomy and expand the hand-curated vibe seed lists.

The seed YAML (``config/date_categories.yaml``) lists vibes -> list of
category path strings (e.g. "Dining and Drinking > Restaurant"). Each seed
is matched against the ``category_label`` column of ``data/categories.parquet``
and expanded to include every descendant category.

Design notes (per AGENTS.md):

* Only ``.parquet`` data files are read.
* Unknown seed paths, duplicate labels, or a missing YAML raise ``ValueError``.
  We fail LOUDLY rather than silently dropping seeds — a missing seed means
  the vibe becomes narrower than the curator intended, which is a bug.
* No implicit fallbacks. If ``config/date_categories.yaml`` doesn't exist,
  the caller must handle the ``FileNotFoundError`` — we don't invent a default.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TAXONOMY_PATH = REPO_ROOT / "data" / "categories.parquet"
DEFAULT_SEED_PATH = REPO_ROOT / "config" / "date_categories.yaml"

VIBES: tuple[str, ...] = (
    "romantic",
    "foodie",
    "nightlife",
    "nerdy",
    "outdoorsy",
    "active",
    "casual",
)


@dataclass(frozen=True)
class Allowlist:
    """Expanded vibe -> set of Foursquare category IDs, plus the union."""

    by_vibe: dict[str, frozenset[str]]
    master: frozenset[str]
    label_by_id: dict[str, str]

    def ids_for(self, vibes: Iterable[str]) -> frozenset[str]:
        """Return the union of category IDs for the given vibes.

        Raises ``ValueError`` if any vibe is unknown. No silent drops.
        """
        vibes = list(vibes)
        unknown = [v for v in vibes if v not in self.by_vibe]
        if unknown:
            raise ValueError(
                f"Unknown vibe(s) {unknown!r}; known vibes are {sorted(self.by_vibe)}."
            )
        if not vibes:
            raise ValueError(
                "ids_for() requires at least one vibe; pass VIBES or a specific list."
            )
        result: set[str] = set()
        for v in vibes:
            result |= self.by_vibe[v]
        return frozenset(result)


def load_taxonomy(path: Path | str = DEFAULT_TAXONOMY_PATH) -> pd.DataFrame:
    """Read ``categories.parquet`` and return a DataFrame.

    Raises ``FileNotFoundError`` if the parquet is missing. We intentionally
    do NOT fall back to the CSV — AGENTS.md says parquet only.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"Taxonomy parquet not found at {path}. "
            "Expected data/categories.parquet; run your data download step."
        )
    df = pd.read_parquet(path)

    required = {"category_id", "category_label", "category_level"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Taxonomy at {path} is missing required columns {sorted(missing)}. "
            f"Got columns: {sorted(df.columns)}."
        )

    if df["category_id"].duplicated().any():
        dupes = df.loc[df["category_id"].duplicated(), "category_id"].unique().tolist()
        raise ValueError(
            f"Duplicate category_id in taxonomy: {dupes[:5]}{'...' if len(dupes) > 5 else ''}. "
            "This shouldn't happen in a clean Foursquare dump."
        )

    logger.info("Loaded taxonomy: %d categories from %s", len(df), path)
    return df


def _build_descendants_index(taxonomy: pd.DataFrame) -> dict[str, set[str]]:
    """For every category_id, return the set of all descendant IDs (inclusive).

    The taxonomy is already flattened — each row carries level1..level6
    category_id columns. So a category X's descendants are exactly the rows
    where X appears in *any* of the levelN_category_id columns.
    """
    level_id_cols = [c for c in taxonomy.columns if c.startswith("level") and c.endswith("_category_id")]
    if not level_id_cols:
        raise ValueError(
            f"Taxonomy has no levelN_category_id columns. Got: {sorted(taxonomy.columns)}."
        )

    descendants: dict[str, set[str]] = {}
    for _, row in taxonomy.iterrows():
        leaf_id = row["category_id"]
        for col in level_id_cols:
            ancestor_id = row[col]
            if ancestor_id is None or (isinstance(ancestor_id, float) and pd.isna(ancestor_id)) or ancestor_id == "":
                continue
            descendants.setdefault(ancestor_id, set()).add(leaf_id)
    return descendants


def _load_seed(path: Path | str = DEFAULT_SEED_PATH) -> dict[str, list[str]]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Seed YAML not found at {path}.")
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    if not isinstance(raw, dict) or "vibes" not in raw:
        raise ValueError(
            f"Seed YAML at {path} must be a mapping with a top-level 'vibes' key. "
            f"Got: {type(raw).__name__} with keys {list(raw) if isinstance(raw, dict) else None}."
        )
    vibes_cfg = raw["vibes"]
    if not isinstance(vibes_cfg, dict) or not vibes_cfg:
        raise ValueError(f"'vibes' in {path} must be a non-empty mapping.")

    declared = set(vibes_cfg)
    expected = set(VIBES)
    missing = expected - declared
    extra = declared - expected
    if missing or extra:
        raise ValueError(
            f"Vibe mismatch in {path}. Missing: {sorted(missing)}; extra: {sorted(extra)}. "
            f"Expected exactly {sorted(expected)}."
        )

    for vibe, seeds in vibes_cfg.items():
        if not isinstance(seeds, list) or not seeds:
            raise ValueError(
                f"Vibe {vibe!r} in {path} must be a non-empty list of category paths."
            )
        if any(not isinstance(s, str) or not s.strip() for s in seeds):
            raise ValueError(f"Vibe {vibe!r} in {path} has non-string or empty entries.")
    return vibes_cfg


def load_allowlist(
    seed_path: Path | str = DEFAULT_SEED_PATH,
    taxonomy_path: Path | str = DEFAULT_TAXONOMY_PATH,
) -> Allowlist:
    """Build the full ``Allowlist`` from seeds + taxonomy.

    Every seed path must resolve to exactly one row in the taxonomy. An
    unresolved or ambiguous seed raises ``ValueError``.
    """
    taxonomy = load_taxonomy(taxonomy_path)
    seeds = _load_seed(seed_path)

    label_to_ids: dict[str, list[str]] = (
        taxonomy.groupby("category_label")["category_id"].apply(list).to_dict()
    )
    label_by_id: dict[str, str] = dict(
        zip(taxonomy["category_id"], taxonomy["category_label"])
    )
    descendants = _build_descendants_index(taxonomy)

    by_vibe: dict[str, frozenset[str]] = {}
    for vibe, paths in seeds.items():
        vibe_ids: set[str] = set()
        for path in paths:
            if path not in label_to_ids:
                raise ValueError(
                    f"Vibe {vibe!r} references unknown category path {path!r}. "
                    "Check config/date_categories.yaml against data/categories.parquet."
                )
            matches = label_to_ids[path]
            if len(matches) != 1:
                raise ValueError(
                    f"Vibe {vibe!r} path {path!r} is ambiguous: resolves to "
                    f"{len(matches)} category IDs ({matches}). Taxonomy labels "
                    "should be unique."
                )
            seed_id = matches[0]
            expanded = descendants.get(seed_id)
            if not expanded:
                raise ValueError(
                    f"Vibe {vibe!r} seed {path!r} (id={seed_id}) has no descendants "
                    "in the taxonomy, not even itself. This indicates a bug in the "
                    "descendant index or a malformed taxonomy row."
                )
            vibe_ids |= expanded

        logger.info("Vibe %r: %d seed paths -> %d expanded category IDs", vibe, len(paths), len(vibe_ids))
        by_vibe[vibe] = frozenset(vibe_ids)

    master = frozenset().union(*by_vibe.values())
    logger.info("Master date-worthy allowlist: %d category IDs across %d vibes", len(master), len(by_vibe))

    return Allowlist(by_vibe=by_vibe, master=master, label_by_id=label_by_id)
