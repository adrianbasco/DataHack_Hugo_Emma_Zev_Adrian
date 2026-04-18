"""Download the Foursquare categories lookup table to data/categories.parquet."""

from pathlib import Path

import pandas as pd
from huggingface_hub import HfApi, hf_hub_download

REPO_ID = "foursquare/fsq-os-places"
RELEASE_DATE = "2026-04-14"
OUT_DIR = Path("data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

prefix = f"release/dt={RELEASE_DATE}/categories/"
api = HfApi()
candidates = [
    f for f in api.list_repo_files(REPO_ID, repo_type="dataset")
    if f.startswith(prefix) and f.endswith(".parquet")
]
if not candidates:
    raise RuntimeError(f"No categories parquet under {prefix}")

print(f"Found {len(candidates)} categories file(s):")
for f in candidates:
    print(f"  {f}")

frames = []
for remote_path in candidates:
    local_path = hf_hub_download(
        repo_id=REPO_ID,
        repo_type="dataset",
        filename=remote_path,
    )
    frames.append(pd.read_parquet(local_path))

cats = pd.concat(frames, ignore_index=True)

parquet_out = OUT_DIR / "categories.parquet"
csv_out = OUT_DIR / "categories.csv"
cats.to_parquet(parquet_out, index=False)
cats.to_csv(csv_out, index=False)

print(f"\nSaved {len(cats):,} rows to:")
print(f"  {parquet_out.resolve()}")
print(f"  {csv_out.resolve()}")
print(f"\nColumns: {cats.columns.tolist()}")
print("\nHead:")
print(cats.head())
