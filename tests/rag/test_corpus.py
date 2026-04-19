from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from back_end.rag.corpus import (
    RagCorpusError,
    build_and_write_rag_corpus,
    build_rag_corpus_documents,
)


class RagCorpusTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.candidates_path = self.root / "candidates.parquet"
        self.chunk_path = self.root / "runs" / "run-a" / "shard-00" / "chunks" / "chunk.parquet"
        self.chunk_path.parent.mkdir(parents=True)
        _candidate_df().to_parquet(self.candidates_path, index=False)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_corpus_includes_only_valid_ok_scraped_rows(self) -> None:
        _profile_df().to_parquet(self.chunk_path, index=False)

        documents, manifest = build_rag_corpus_documents(
            candidate_parquet_path=self.candidates_path,
            profile_chunk_paths=(self.chunk_path,),
            run_id="test-run",
        )

        self.assertEqual(documents["fsq_place_id"].tolist(), ["place-valid"])
        self.assertIn("romantic", documents.iloc[0]["document_text"].casefold())
        metrics = dict(zip(manifest["metric"], manifest["value"], strict=False))
        self.assertEqual(metrics["non_ok_scrape_status"], 1)
        self.assertEqual(metrics["empty_rich_profile_text"], 1)
        self.assertEqual(metrics["quality_score_below_threshold"], 1)
        self.assertEqual(metrics["not_in_candidate_parquet"], 1)
        self.assertEqual(metrics["document_rows"], 1)

    def test_corpus_refuses_non_empty_output_dir_without_overwrite(self) -> None:
        _profile_df().to_parquet(self.chunk_path, index=False)
        output_dir = self.root / "data" / "rag" / "runs" / "existing"
        output_dir.mkdir(parents=True)
        (output_dir / "placeholder.txt").write_text("existing", encoding="utf-8")

        with self.assertRaises(FileExistsError):
            build_and_write_rag_corpus(
                candidate_parquet_path=self.candidates_path,
                profile_chunk_paths=(self.chunk_path,),
                output_dir=output_dir,
                run_id="test-run",
            )

    def test_corpus_rejects_duplicate_candidate_ids(self) -> None:
        duplicate = pd.concat([_candidate_df(), _candidate_df().iloc[[0]]], ignore_index=True)
        duplicate.to_parquet(self.candidates_path, index=False)
        _profile_df().to_parquet(self.chunk_path, index=False)

        with self.assertRaises(RagCorpusError):
            build_rag_corpus_documents(
                candidate_parquet_path=self.candidates_path,
                profile_chunk_paths=(self.chunk_path,),
                run_id="test-run",
            )

    def test_corpus_hash_includes_place_id_for_duplicate_chain_profiles(self) -> None:
        candidates = pd.DataFrame(
            [
                {
                    "fsq_place_id": "chain-a",
                    "name": "Same Chain",
                    "latitude": -33.86,
                    "longitude": 151.2,
                    "locality": "Sydney",
                    "region": "NSW",
                    "postcode": "2000",
                    "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                    "date_closed": None,
                },
                {
                    "fsq_place_id": "chain-b",
                    "name": "Same Chain",
                    "latitude": -33.87,
                    "longitude": 151.21,
                    "locality": "Sydney",
                    "region": "NSW",
                    "postcode": "2000",
                    "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                    "date_closed": None,
                },
            ]
        )
        profiles = pd.DataFrame(
            [
                {
                    **_base_profile_fields(),
                    "fsq_place_id": "chain-a",
                    "crawl4ai_enrichment_status": "ok",
                    "crawl4ai_rich_profile_text": "Shared chain profile text.",
                    "crawl4ai_quality_score": 5,
                },
                {
                    **_base_profile_fields(),
                    "fsq_place_id": "chain-b",
                    "crawl4ai_enrichment_status": "ok",
                    "crawl4ai_rich_profile_text": "Shared chain profile text.",
                    "crawl4ai_quality_score": 5,
                },
            ]
        )
        candidates.to_parquet(self.candidates_path, index=False)
        profiles.to_parquet(self.chunk_path, index=False)

        documents, _ = build_rag_corpus_documents(
            candidate_parquet_path=self.candidates_path,
            profile_chunk_paths=(self.chunk_path,),
            run_id="test-run",
        )

        self.assertEqual(["chain-a", "chain-b"], documents["fsq_place_id"].tolist())
        self.assertFalse(documents["document_hash"].duplicated().any())


def _candidate_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "place-valid",
                "name": "Valid Date Room",
                "latitude": -33.86,
                "longitude": 151.2,
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                "date_closed": None,
            },
            {
                "fsq_place_id": "place-timeout",
                "name": "Timeout Room",
                "latitude": -33.87,
                "longitude": 151.21,
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                "date_closed": None,
            },
            {
                "fsq_place_id": "place-empty",
                "name": "Empty Room",
                "latitude": -33.88,
                "longitude": 151.22,
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                "date_closed": None,
            },
            {
                "fsq_place_id": "place-low-quality",
                "name": "Low Quality Room",
                "latitude": -33.89,
                "longitude": 151.23,
                "locality": "Sydney",
                "region": "NSW",
                "postcode": "2000",
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                "date_closed": None,
            },
        ]
    )


def _profile_df() -> pd.DataFrame:
    base = _base_profile_fields()
    return pd.DataFrame(
        [
            {
                **base,
                "fsq_place_id": "place-valid",
                "crawl4ai_enrichment_status": "ok",
                "crawl4ai_rich_profile_text": "A romantic restaurant for dinner dates.",
                "crawl4ai_quality_score": 5,
            },
            {
                **base,
                "fsq_place_id": "place-timeout",
                "crawl4ai_enrichment_status": "crawl_timeout",
                "crawl4ai_rich_profile_text": "Should be excluded.",
                "crawl4ai_quality_score": 5,
            },
            {
                **base,
                "fsq_place_id": "place-empty",
                "crawl4ai_enrichment_status": "ok",
                "crawl4ai_rich_profile_text": " ",
                "crawl4ai_quality_score": 5,
            },
            {
                **base,
                "fsq_place_id": "place-low-quality",
                "crawl4ai_enrichment_status": "ok",
                "crawl4ai_rich_profile_text": "Low quality profile.",
                "crawl4ai_quality_score": 0,
            },
            {
                **base,
                "fsq_place_id": "place-not-candidate",
                "crawl4ai_enrichment_status": "ok",
                "crawl4ai_rich_profile_text": "Not in current Sydney candidate set.",
                "crawl4ai_quality_score": 5,
            },
        ]
    )


def _base_profile_fields() -> dict[str, object]:
    return {
        "crawl4ai_template_stop_tags": ["restaurant"],
        "crawl4ai_ambience_tags": ["romantic"],
        "crawl4ai_setting_tags": ["intimate"],
        "crawl4ai_activity_tags": [],
        "crawl4ai_drink_tags": ["wine"],
        "crawl4ai_booking_signals": ["bookings"],
        "crawl4ai_evidence_snippets": ["Romantic dining room with wine."],
    }


if __name__ == "__main__":
    unittest.main()
