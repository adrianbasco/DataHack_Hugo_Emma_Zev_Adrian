from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from back_end.services.profile_pipeline import build_chunk_plan
from back_end.services.profile_pipeline import chunk_output_path
from back_end.services.profile_pipeline import run_chunked_profile_enrichment
from back_end.services.profile_pipeline import run_layout
from back_end.services.profile_pipeline import stream_merge_shard_outputs


class _FakeProfileClient:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    async def enrich_dataframe(self, places: pd.DataFrame) -> pd.DataFrame:
        self.calls.append(places["fsq_place_id"].astype(str).tolist())
        enriched = places.copy()
        enriched["website_enrichment_status"] = "ok"
        return enriched


class ProfilePipelineTestCase(unittest.TestCase):
    def test_build_chunk_plan_keeps_duplicate_websites_in_same_chunk(self) -> None:
        places = pd.DataFrame(
            [
                {"fsq_place_id": "a", "website": "https://example.com"},
                {"fsq_place_id": "b", "website": "https://example.com/"},
                {"fsq_place_id": "c", "website": "https://other.com"},
            ]
        )

        plan = build_chunk_plan(
            places,
            chunk_size=1,
            shard_key="website",
        )

        by_id = plan.set_index("fsq_place_id")
        self.assertEqual(int(by_id.loc["a", "chunk_index"]), int(by_id.loc["b", "chunk_index"]))
        self.assertNotEqual(int(by_id.loc["a", "chunk_index"]), int(by_id.loc["c", "chunk_index"]))

    def test_run_chunked_profile_enrichment_writes_status_and_metrics(self) -> None:
        places = pd.DataFrame(
            [
                {"fsq_place_id": "a", "website": "https://example.com/a", "has_website": True},
                {"fsq_place_id": "b", "website": "https://example.com/b", "has_website": True},
                {"fsq_place_id": "c", "website": "https://other.com/a", "has_website": True},
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            layout = run_layout(
                run_dir=Path(temp_dir) / "run",
                shard_count=1,
                shard_index=0,
            )
            client = _FakeProfileClient()

            shard_output = asyncio.run(
                run_chunked_profile_enrichment(
                    places=places,
                    client=client,
                    layout=layout,
                    chunk_size=1,
                    shard_key="website",
                    overwrite=False,
                )
            )

            self.assertTrue(shard_output.exists())
            self.assertTrue(layout.shard_metrics_path.exists())
            self.assertTrue(layout.shard_status_path.exists())

            metrics = pd.read_parquet(layout.shard_metrics_path)
            status = json.loads(layout.shard_status_path.read_text(encoding="utf-8"))

            self.assertEqual(metrics["status"].tolist(), ["completed", "completed", "completed"])
            self.assertEqual(status["state"], "completed")
            self.assertEqual(status["processed_row_count"], 3)

    def test_run_chunked_profile_enrichment_accepts_progress_every(self) -> None:
        places = pd.DataFrame(
            [
                {"fsq_place_id": "a", "website": None},
                {"fsq_place_id": "b", "website": None},
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            layout = run_layout(
                run_dir=Path(temp_dir) / "run",
                shard_count=1,
                shard_index=0,
            )

            shard_output = asyncio.run(
                run_chunked_profile_enrichment(
                    places=places,
                    client=_FakeProfileClient(),
                    layout=layout,
                    chunk_size=1,
                    shard_key="fsq_place_id",
                    overwrite=False,
                    progress_every=1,
                )
            )

            self.assertTrue(shard_output.exists())

    def test_run_chunked_profile_enrichment_resumes_from_completed_chunks(self) -> None:
        places = pd.DataFrame(
            [
                {"fsq_place_id": "a", "website": "https://example.com/a", "has_website": True},
                {"fsq_place_id": "b", "website": "https://example.com/b", "has_website": True},
                {"fsq_place_id": "c", "website": "https://other.com/a", "has_website": True},
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            layout = run_layout(
                run_dir=Path(temp_dir) / "run",
                shard_count=1,
                shard_index=0,
            )

            first_client = _FakeProfileClient()
            shard_output = asyncio.run(
                run_chunked_profile_enrichment(
                    places=places,
                    client=first_client,
                    layout=layout,
                    chunk_size=1,
                    shard_key="website",
                    overwrite=False,
                )
            )
            self.assertTrue(shard_output.exists())
            self.assertEqual(len(first_client.calls), 3)

            layout.shard_output_path.unlink()

            second_client = _FakeProfileClient()
            resumed_output = asyncio.run(
                run_chunked_profile_enrichment(
                    places=places,
                    client=second_client,
                    layout=layout,
                    chunk_size=1,
                    shard_key="website",
                    overwrite=False,
                )
            )
            self.assertTrue(resumed_output.exists())
            self.assertEqual(second_client.calls, [])
            self.assertTrue(chunk_output_path(layout=layout, chunk_index=0).exists())
            metrics = pd.read_parquet(layout.shard_metrics_path)
            self.assertEqual(metrics["status"].tolist(), ["completed", "completed", "completed"])

    def test_stream_merge_tolerates_null_only_columns_in_early_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            first_path = temp_path / "first.parquet"
            second_path = temp_path / "second.parquet"
            output_path = temp_path / "merged.parquet"

            pd.DataFrame(
                [
                    {
                        "fsq_place_id": "a",
                        "no_website_maps_place_id": None,
                    }
                ]
            ).to_parquet(first_path, index=False)
            pd.DataFrame(
                [
                    {
                        "fsq_place_id": "b",
                        "no_website_maps_place_id": "maps_b",
                    }
                ]
            ).to_parquet(second_path, index=False)

            stream_merge_shard_outputs(
                shard_paths=[first_path, second_path],
                output_path=output_path,
                overwrite=False,
            )

            merged = pd.read_parquet(output_path)
            self.assertEqual(merged["fsq_place_id"].tolist(), ["a", "b"])
            self.assertTrue(pd.isna(merged.loc[0, "no_website_maps_place_id"]))
            self.assertEqual(merged.loc[1, "no_website_maps_place_id"], "maps_b")

    def test_stream_merge_widens_mixed_int_and_float_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            first_path = temp_path / "first.parquet"
            second_path = temp_path / "second.parquet"
            output_path = temp_path / "merged.parquet"

            pd.DataFrame(
                [
                    {
                        "fsq_place_id": "a",
                        "no_website_maps_user_rating_count": 12.0,
                    }
                ]
            ).to_parquet(first_path, index=False)
            pd.DataFrame(
                [
                    {
                        "fsq_place_id": "b",
                        "no_website_maps_user_rating_count": 34,
                    }
                ]
            ).to_parquet(second_path, index=False)

            stream_merge_shard_outputs(
                shard_paths=[first_path, second_path],
                output_path=output_path,
                overwrite=False,
            )

            merged = pd.read_parquet(output_path)
            self.assertEqual(merged["fsq_place_id"].tolist(), ["a", "b"])
            self.assertEqual(
                merged["no_website_maps_user_rating_count"].tolist(),
                [12.0, 34.0],
            )


if __name__ == "__main__":
    unittest.main()
