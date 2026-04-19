from __future__ import annotations

import asyncio
import unittest

import pandas as pd

from back_end.rag.models import StopRetrievalRequest
from back_end.rag.retriever import RagRetriever
from back_end.rag.vector_store import ExactVectorStore


class QueryEmbeddingClient:
    async def embed_texts(self, texts: tuple[str, ...]) -> tuple[tuple[float, ...], ...]:
        return tuple((1.0, 0.0) for _ in texts)


class RagRetrieverTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.store = ExactVectorStore(_documents_df(), _embeddings_df())
        self.retriever = RagRetriever(
            vector_store=self.store,
            embedding_client=QueryEmbeddingClient(),
        )

    def test_retriever_filters_by_stop_type_before_semantic_search(self) -> None:
        result = asyncio.run(
            self.retriever.retrieve_stop(
                StopRetrievalRequest(
                    stop_type="restaurant",
                    query_text="romantic restaurant with wine",
                    top_k=5,
                )
            )
        )

        self.assertIsNone(result.empty_reason)
        self.assertEqual([hit.fsq_place_id for hit in result.hits], ["restaurant-1"])

    def test_connective_stop_does_not_hit_vector_store(self) -> None:
        result = asyncio.run(
            self.retriever.retrieve_stop(
                StopRetrievalRequest(
                    stop_type="ferry_ride",
                    query_text="short ferry across the harbour",
                    top_k=5,
                )
            )
        )

        self.assertTrue(result.is_connective)
        self.assertEqual(result.hits, ())
        self.assertIn("Connective", result.empty_reason or "")


def _documents_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "restaurant-1",
                "name": "Romantic Restaurant",
                "document_hash": "hash-restaurant",
                "crawl4ai_evidence_snippets": ["Romantic restaurant with wine."],
                "crawl4ai_template_stop_tags": ["restaurant"],
                "fsq_category_labels": ["Dining and Drinking > Restaurant"],
                "crawl4ai_ambience_tags": ["romantic"],
                "crawl4ai_setting_tags": ["intimate"],
                "crawl4ai_activity_tags": [],
                "crawl4ai_drink_tags": ["wine"],
                "crawl4ai_quality_score": 8,
            },
            {
                "fsq_place_id": "bookstore-1",
                "name": "Quiet Bookstore",
                "document_hash": "hash-bookstore",
                "crawl4ai_evidence_snippets": ["Independent bookstore for browsing."],
                "crawl4ai_template_stop_tags": ["bookstore"],
                "fsq_category_labels": ["Retail > Bookstore"],
                "crawl4ai_ambience_tags": ["quiet"],
                "crawl4ai_setting_tags": [],
                "crawl4ai_activity_tags": ["books"],
                "crawl4ai_drink_tags": [],
                "crawl4ai_quality_score": 8,
            },
        ]
    )


def _embeddings_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "restaurant-1",
                "document_hash": "hash-restaurant",
                "embedding_dimension": 2,
                "embedding": [1.0, 0.0],
            },
            {
                "fsq_place_id": "bookstore-1",
                "document_hash": "hash-bookstore",
                "embedding_dimension": 2,
                "embedding": [1.0, 0.0],
            },
        ]
    )


if __name__ == "__main__":
    unittest.main()

