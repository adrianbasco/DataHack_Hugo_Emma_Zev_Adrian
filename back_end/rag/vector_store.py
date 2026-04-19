"""Exact local vector search over RAG place embeddings."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from back_end.rag.models import RagSearchHit

logger = logging.getLogger(__name__)

REQUIRED_DOCUMENT_COLUMNS: tuple[str, ...] = (
    "fsq_place_id",
    "name",
    "document_hash",
    "crawl4ai_evidence_snippets",
)
REQUIRED_EMBEDDING_COLUMNS: tuple[str, ...] = (
    "fsq_place_id",
    "document_hash",
    "embedding_dimension",
    "embedding",
)


class VectorStoreError(RuntimeError):
    """Raised when a vector store cannot be loaded or queried safely."""


class ExactVectorStore:
    """In-memory exact cosine search over normalized embeddings."""

    def __init__(self, documents: pd.DataFrame, embeddings: pd.DataFrame) -> None:
        _validate_columns(documents, REQUIRED_DOCUMENT_COLUMNS, label="documents")
        _validate_columns(embeddings, REQUIRED_EMBEDDING_COLUMNS, label="embeddings")
        if documents.empty:
            raise VectorStoreError("Cannot load vector store from an empty document frame.")
        if embeddings.empty:
            raise VectorStoreError("Cannot load vector store from an empty embedding frame.")

        merged = documents.merge(
            embeddings.loc[:, list(REQUIRED_EMBEDDING_COLUMNS)],
            on=["fsq_place_id", "document_hash"],
            how="inner",
            validate="one_to_one",
        )
        if merged.empty:
            raise VectorStoreError("No documents matched embeddings by fsq_place_id/document_hash.")

        dimensions = set(int(value) for value in merged["embedding_dimension"])
        if len(dimensions) != 1:
            raise VectorStoreError(f"Embeddings contain mixed dimensions {dimensions}.")
        self.embedding_dimension = next(iter(dimensions))
        matrix = np.asarray([list(vector) for vector in merged["embedding"]], dtype=np.float32)
        if matrix.ndim != 2 or matrix.shape[1] != self.embedding_dimension:
            raise VectorStoreError(
                "Embedding matrix shape does not match the recorded embedding dimension."
            )
        self._documents = merged.reset_index(drop=True)
        self._matrix = _normalize(matrix)
        logger.info("Loaded exact vector store with %d documents.", len(self._documents))

    @classmethod
    def from_parquet(
        cls,
        *,
        documents_path: Path | str,
        embeddings_path: Path | str,
    ) -> "ExactVectorStore":
        """Load documents and embeddings from parquet files."""

        return cls(
            documents=pd.read_parquet(documents_path),
            embeddings=pd.read_parquet(embeddings_path),
        )

    @property
    def documents(self) -> pd.DataFrame:
        """Return a copy of loaded document metadata."""

        return self._documents.copy()

    def search(
        self,
        query_embedding: Iterable[float],
        *,
        top_k: int = 10,
        candidate_place_ids: Iterable[str] | None = None,
    ) -> tuple[RagSearchHit, ...]:
        """Return top-k exact cosine hits."""

        if top_k <= 0:
            raise ValueError("top_k must be positive.")
        query = np.asarray(list(query_embedding), dtype=np.float32)
        if query.shape != (self.embedding_dimension,):
            raise VectorStoreError(
                f"Query embedding dimension {query.shape} does not match store dimension "
                f"{self.embedding_dimension}."
            )
        candidate_mask = np.ones(len(self._documents), dtype=bool)
        if candidate_place_ids is not None:
            allowed = {str(place_id) for place_id in candidate_place_ids}
            if not allowed:
                return ()
            candidate_mask = self._documents["fsq_place_id"].astype(str).isin(allowed).to_numpy()
            if not bool(candidate_mask.any()):
                return ()

        normalized_query = _normalize(query.reshape(1, -1))[0]
        scores = self._matrix @ normalized_query
        scores = np.where(candidate_mask, scores, -np.inf)
        candidate_count = int(candidate_mask.sum())
        limit = min(top_k, candidate_count)
        if limit <= 0:
            return ()

        top_indices = np.argpartition(scores, -limit)[-limit:]
        top_indices = top_indices[np.argsort(scores[top_indices])[::-1]]
        hits: list[RagSearchHit] = []
        for index in top_indices:
            if not np.isfinite(scores[index]):
                continue
            row = self._documents.iloc[int(index)]
            score = float(scores[index])
            hits.append(
                RagSearchHit(
                    fsq_place_id=str(row["fsq_place_id"]),
                    name=str(row["name"]),
                    semantic_score=score,
                    final_score=score,
                    score_breakdown={"semantic": score},
                    document_hash=str(row["document_hash"]),
                    evidence_snippets=_string_tuple(row["crawl4ai_evidence_snippets"]),
                    metadata=row.to_dict(),
                )
            )
        return tuple(hits)


def _normalize(matrix: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    if np.any(norms == 0):
        raise VectorStoreError("Encountered a zero-length embedding vector.")
    return matrix / norms


def _validate_columns(df: pd.DataFrame, required: tuple[str, ...], *, label: str) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise VectorStoreError(
            f"{label} frame is missing required columns {missing}. "
            f"Got columns: {sorted(df.columns)}."
        )


def _string_tuple(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        values = [value]
    else:
        try:
            values = list(value)  # type: ignore[arg-type]
        except TypeError:
            values = [value]
    return tuple(str(item).strip() for item in values if str(item).strip())

