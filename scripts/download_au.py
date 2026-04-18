"""Download only Australia rows from the gated FSQ OS Places parquet shards.

This script:
1. Lists the remote parquet shards from Hugging Face.
2. Downloads one shard at a time.
3. Filters to `country = 'AU'` with DuckDB.
4. Saves the filtered rows as local parquet shards.
5. Merges those filtered shards into a single parquet file at the end.

It is restart-friendly: completed filtered shards are skipped on rerun.

Prerequisites:
- You must have accepted the gated dataset terms on Hugging Face.
- Authenticate with `huggingface-cli login` or set `HF_TOKEN`.
- Install dependencies in the repo venv:
  `./.venv/bin/pip install duckdb huggingface_hub pyarrow`
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb
from huggingface_hub import HfApi, hf_hub_download

REPO_ID = "foursquare/fsq-os-places"
DEFAULT_RELEASE_DATE = "2026-04-14"
COUNTRY_CODE = "AU"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--release-date",
        default=DEFAULT_RELEASE_DATE,
        help="Dataset release date under release/dt=YYYY-MM-DD.",
    )
    parser.add_argument(
        "--output",
        default="data/au_places.parquet",
        help="Final merged parquet output path.",
    )
    parser.add_argument(
        "--workdir",
        default="data/au_places_shards",
        help="Directory for intermediate filtered parquet shards.",
    )
    parser.add_argument(
        "--rebuild-final-only",
        action="store_true",
        help="Skip downloads and rebuild the final parquet from existing filtered shards.",
    )
    return parser.parse_args()


def resolve_token() -> str | None:
    token = os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_HUB_TOKEN")
    return token or None


def list_remote_shards(release_date: str, token: str | None) -> list[str]:
    parquet_dir = f"release/dt={release_date}/places/parquet"
    api = HfApi(token=token)
    files = sorted(
        path
        for path in api.list_repo_files(REPO_ID, repo_type="dataset")
        if path.startswith(parquet_dir) and path.endswith(".parquet")
    )
    if not files:
        raise RuntimeError(f"No parquet shards found under {parquet_dir}")
    return files


def filtered_shard_path(workdir: Path, remote_path: str) -> Path:
    return workdir / Path(remote_path).name


def remove_hf_cache_file(local_path: str) -> None:
    try:
        os.remove(local_path)
    except OSError:
        pass

    try:
        blob_path = Path(local_path).resolve()
        if blob_path.exists():
            blob_path.unlink()
    except OSError:
        pass


def export_filtered_shard(
    con: duckdb.DuckDBPyConnection,
    remote_path: str,
    workdir: Path,
    token: str | None,
) -> tuple[int, bool]:
    output_path = filtered_shard_path(workdir, remote_path)
    if output_path.exists():
        rows = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{output_path.as_posix()}')"
        ).fetchone()[0]
        return rows, True

    local_path = hf_hub_download(
        repo_id=REPO_ID,
        repo_type="dataset",
        filename=remote_path,
        token=token,
    )

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM read_parquet('{Path(local_path).as_posix()}')
            WHERE country = '{COUNTRY_CODE}'
        ) TO '{output_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{output_path.as_posix()}')"
    ).fetchone()[0]
    if rows == 0:
        output_path.unlink(missing_ok=True)

    remove_hf_cache_file(local_path)
    return rows, False


def merge_filtered_shards(
    con: duckdb.DuckDBPyConnection,
    workdir: Path,
    output_path: Path,
) -> int:
    shard_glob = (workdir / "*.parquet").as_posix()
    shard_count = len(list(workdir.glob("*.parquet")))
    if shard_count == 0:
        raise RuntimeError(f"No filtered shards found in {workdir}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    tmp_path.unlink(missing_ok=True)

    # Foursquare's shards are not schema-consistent: most encode `geom` as BLOB
    # (raw WKB), but some encode it as GEOMETRY('OGC:CRS84'). DuckDB cannot
    # cast between those types across files in a single read, so we drop
    # `geom` from the merged output (lat/lon columns are preserved).
    con.execute(
        f"""
        COPY (
            SELECT * EXCLUDE (geom)
            FROM read_parquet('{shard_glob}', union_by_name = true)
        ) TO '{tmp_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    total_rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{tmp_path.as_posix()}')"
    ).fetchone()[0]
    output_path.unlink(missing_ok=True)
    tmp_path.rename(output_path)
    return total_rows


def main() -> None:
    args = parse_args()
    token = resolve_token()
    output_path = Path(args.output)
    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    if args.rebuild_final_only:
        total_rows = merge_filtered_shards(con, workdir, output_path)
        print(
            f"Rebuilt {output_path.resolve()} from existing filtered shards "
            f"with {total_rows:,} AU rows."
        )
        return

    remote_shards = list_remote_shards(args.release_date, token)
    total_rows = 0

    print(
        f"Found {len(remote_shards)} parquet shards for release {args.release_date}. "
        f"Filtering country = '{COUNTRY_CODE}'."
    )

    for index, remote_path in enumerate(remote_shards, start=1):
        rows, skipped = export_filtered_shard(con, remote_path, workdir, token)
        total_rows += rows
        status = "skip" if skipped else "done"
        print(
            f"[{index:>3}/{len(remote_shards)}] {Path(remote_path).name} "
            f"{status}: {rows:,} AU rows"
        )

    merged_rows = merge_filtered_shards(con, workdir, output_path)
    print(
        f"\nMerged {len(list(workdir.glob('*.parquet')))} filtered shards into "
        f"{output_path.resolve()} with {merged_rows:,} AU rows."
    )


if __name__ == "__main__":
    main()
