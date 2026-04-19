from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from back_end.precache.settings import (
    PrecacheConfigurationError,
    PrecacheSettings,
    resolve_bucket_transport_mode,
)
from back_end.domain.models import TravelMode


class PrecacheSettingsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.documents_path = self.root / "place_documents.parquet"
        self.embeddings_path = self.root / "place_embeddings.parquet"
        pd.DataFrame(
            [
                {
                    "fsq_place_id": "fsq-1",
                    "name": "Cafe",
                    "document_hash": "hash-1",
                }
            ]
        ).to_parquet(self.documents_path, index=False)
        pd.DataFrame([{"embedding_model": "local-hashing-v1:8"}]).to_parquet(
            self.embeddings_path,
            index=False,
        )

    def test_from_env_requires_both_explicit_rag_paths(self) -> None:
        with patch.dict(
            os.environ,
            {"PRECACHE_RAG_DOCUMENTS_PATH": str(self.documents_path)},
            clear=False,
        ):
            with self.assertRaises(PrecacheConfigurationError):
                PrecacheSettings.from_env()

    def test_from_env_accepts_explicit_rag_paths(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PRECACHE_RAG_DOCUMENTS_PATH": str(self.documents_path),
                "PRECACHE_RAG_EMBEDDINGS_PATH": str(self.embeddings_path),
            },
            clear=False,
        ):
            settings = PrecacheSettings.from_env()

        self.assertEqual(self.documents_path, settings.rag_documents_path)
        self.assertEqual(self.embeddings_path, settings.rag_embeddings_path)

    def test_transport_mode_resolution_is_strict(self) -> None:
        self.assertEqual(TravelMode.WALK, resolve_bucket_transport_mode("walking"))
        with self.assertRaises(PrecacheConfigurationError):
            resolve_bucket_transport_mode("teleport")
