from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import httpx

from back_end.rag.embeddings import (
    EmbeddingError,
    LocalOpenAICompatibleEmbeddingClient,
    build_rag_embeddings,
)


class FakeEmbeddingClient:
    def __init__(self, dimensions: tuple[int, ...] = (2,)) -> None:
        self.dimensions = dimensions
        self.calls: list[tuple[str, ...]] = []

    async def embed_texts(self, texts: tuple[str, ...]) -> tuple[tuple[float, ...], ...]:
        self.calls.append(texts)
        result = []
        for index, text in enumerate(texts):
            dimension = self.dimensions[min(index, len(self.dimensions) - 1)]
            result.append(tuple(float(len(text) + offset) for offset in range(dimension)))
        return tuple(result)


class RagEmbeddingsTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.documents_path = self.root / "place_documents.parquet"
        self.output_path = self.root / "place_embeddings.parquet"
        _documents_df().to_parquet(self.documents_path, index=False)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_embedding_build_reuses_existing_document_hashes(self) -> None:
        existing_path = self.root / "existing_embeddings.parquet"
        pd.DataFrame(
            [
                {
                    "fsq_place_id": "place-1",
                    "document_hash": "hash-1",
                    "embedding_model": "local-test",
                    "embedding_dimension": 2,
                    "embedding": [1.0, 0.0],
                    "embedded_at": "2026-04-18T00:00:00+00:00",
                }
            ]
        ).to_parquet(existing_path, index=False)
        client = FakeEmbeddingClient()

        result = asyncio.run(
            build_rag_embeddings(
                documents_path=self.documents_path,
                output_path=self.output_path,
                client=client,
                embedding_model="local-test",
                batch_size=10,
                existing_embeddings_path=existing_path,
            )
        )

        self.assertEqual(result.embedding_count, 2)
        self.assertEqual(result.reused_embedding_count, 1)
        self.assertEqual(result.new_embedding_count, 1)
        self.assertEqual(client.calls, [("second document",)])
        written = pd.read_parquet(self.output_path)
        self.assertEqual(set(written["document_hash"]), {"hash-1", "hash-2"})

    def test_embedding_build_rejects_mixed_dimensions(self) -> None:
        client = FakeEmbeddingClient(dimensions=(2, 3))

        with self.assertRaises(EmbeddingError):
            asyncio.run(
                build_rag_embeddings(
                    documents_path=self.documents_path,
                    output_path=self.output_path,
                    client=client,
                    embedding_model="local-test",
                    batch_size=2,
                )
            )

    def test_local_embedding_client_wraps_connection_failures(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("not running", request=request)

        client = LocalOpenAICompatibleEmbeddingClient(
            base_url="http://localhost:1234/v1",
            model="local-test",
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self.addCleanup(lambda: asyncio.run(client.http_client.aclose()))

        with self.assertRaisesRegex(EmbeddingError, "local embedding server"):
            asyncio.run(client.embed_texts(("hello",)))


def _documents_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fsq_place_id": "place-1",
                "document_hash": "hash-1",
                "document_text": "first document",
            },
            {
                "fsq_place_id": "place-2",
                "document_hash": "hash-2",
                "document_text": "second document",
            },
        ]
    )


if __name__ == "__main__":
    unittest.main()
