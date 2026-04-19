"""Async embedding clients and parquet embedding builds for RAG documents."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import httpx
import pandas as pd

from back_end.rag.models import EmbeddingBuildResult
from back_end.rag.settings import RagSettings, load_rag_settings

logger = logging.getLogger(__name__)
TOKEN_RE = re.compile(r"[a-z0-9]+")
LOCAL_HASHING_EMBEDDING_MODEL_PREFIX = "local-hashing-v1"

REQUIRED_DOCUMENT_COLUMNS: tuple[str, ...] = (
    "fsq_place_id",
    "document_hash",
    "document_text",
)
REQUIRED_EMBEDDING_COLUMNS: tuple[str, ...] = (
    "fsq_place_id",
    "document_hash",
    "embedding_model",
    "embedding_dimension",
    "embedding",
    "embedded_at",
)


class EmbeddingError(RuntimeError):
    """Raised when embeddings cannot be generated or validated."""


class EmbeddingClient(Protocol):
    """Async embedding client interface."""

    async def embed_texts(self, texts: tuple[str, ...]) -> tuple[tuple[float, ...], ...]:
        """Return one embedding per input text."""


@dataclass(frozen=True)
class LocalHashingEmbeddingClient:
    """Deterministic in-process embedding client for local smoke runs.

    This is an explicit lexical backend. It avoids the OpenAI-compatible local
    server requirement, and should be replaced by a real semantic embedding
    model for quality-sensitive generation.
    """

    dimension: int = 512

    @property
    def model_name(self) -> str:
        return f"{LOCAL_HASHING_EMBEDDING_MODEL_PREFIX}:{self.dimension}"

    async def embed_texts(self, texts: tuple[str, ...]) -> tuple[tuple[float, ...], ...]:
        if self.dimension <= 0:
            raise EmbeddingError("Hashing embedding dimension must be positive.")
        if not texts:
            return ()
        if any(not text.strip() for text in texts):
            raise EmbeddingError("Refusing to embed empty text.")
        return tuple(_hash_text_embedding(text, dimension=self.dimension) for text in texts)


@dataclass
class LocalOpenAICompatibleEmbeddingClient:
    """Embedding client for local OpenAI-compatible servers such as LM Studio."""

    base_url: str
    model: str
    timeout_seconds: float = 120.0
    http_client: httpx.AsyncClient | None = None

    async def embed_texts(self, texts: tuple[str, ...]) -> tuple[tuple[float, ...], ...]:
        if not texts:
            return ()
        if any(not text.strip() for text in texts):
            raise EmbeddingError("Refusing to embed empty text.")

        owns_client = self.http_client is None
        client = self.http_client or httpx.AsyncClient(timeout=self.timeout_seconds)
        try:
            embeddings_url = f"{self.base_url.rstrip('/')}/embeddings"
            try:
                response = await client.post(
                    embeddings_url,
                    json={"model": self.model, "input": list(texts)},
                )
            except httpx.HTTPError as exc:
                logger.error(
                    "Embedding server request failed for url=%s model=%s: %s",
                    embeddings_url,
                    self.model,
                    exc,
                )
                raise EmbeddingError(
                    "Could not connect to the local embedding server at "
                    f"{embeddings_url}. Start an OpenAI-compatible embedding "
                    f"server there, or pass --base-url/--model explicitly. "
                    f"Configured model: {self.model!r}."
                ) from exc
            if response.status_code >= 400:
                raise EmbeddingError(
                    f"Embedding server returned HTTP {response.status_code}: "
                    f"{response.text[:500]}"
                )
            try:
                payload = response.json()
            except ValueError as exc:
                raise EmbeddingError(
                    "Embedding server returned a non-JSON response body: "
                    f"{response.text[:500]}"
                ) from exc
        finally:
            if owns_client:
                await client.aclose()

        return _parse_openai_embedding_payload(payload, expected_count=len(texts))


async def build_rag_embeddings(
    *,
    documents_path: Path | str,
    output_path: Path | str,
    client: EmbeddingClient,
    embedding_model: str,
    batch_size: int = 16,
    overwrite: bool = False,
    existing_embeddings_path: Path | str | None = None,
) -> EmbeddingBuildResult:
    """Embed RAG documents and write a derived embeddings parquet."""

    if batch_size <= 0:
        raise ValueError("batch_size must be positive.")

    documents_path = _require_parquet(documents_path, must_exist=True)
    output_path = _require_parquet(output_path, must_exist=False)
    if output_path.exists() and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing embeddings parquet {output_path}."
        )

    documents = pd.read_parquet(documents_path)
    _validate_columns(documents, REQUIRED_DOCUMENT_COLUMNS, source=documents_path)
    if documents.empty:
        raise EmbeddingError(f"Document parquet {documents_path} is empty.")
    duplicate_hashes = int(documents["document_hash"].duplicated().sum())
    if duplicate_hashes:
        raise EmbeddingError(
            f"Document parquet {documents_path} contains {duplicate_hashes} duplicate "
            "document_hash values."
        )

    reusable = _load_reusable_embeddings(
        existing_embeddings_path=existing_embeddings_path,
        current_document_hashes=set(documents["document_hash"].astype(str)),
        embedding_model=embedding_model,
    )
    reusable_hashes = set(reusable["document_hash"].astype(str)) if not reusable.empty else set()
    missing = documents.loc[
        ~documents["document_hash"].astype(str).isin(reusable_hashes)
    ].copy()

    new_records: list[dict[str, object]] = []
    for start in range(0, len(missing), batch_size):
        batch = missing.iloc[start : start + batch_size]
        texts = tuple(str(text) for text in batch["document_text"])
        embeddings = await client.embed_texts(texts)
        _validate_embedding_batch(embeddings, expected_count=len(batch))
        for (_, row), embedding in zip(batch.iterrows(), embeddings, strict=True):
            new_records.append(
                {
                    "fsq_place_id": str(row["fsq_place_id"]),
                    "document_hash": str(row["document_hash"]),
                    "embedding_model": embedding_model,
                    "embedding_dimension": len(embedding),
                    "embedding": list(embedding),
                    "embedded_at": datetime.now(UTC).isoformat(),
                }
            )
        logger.info("Embedded %d / %d missing RAG documents.", len(new_records), len(missing))
        await asyncio.sleep(0)

    new_frame = pd.DataFrame(new_records, columns=list(REQUIRED_EMBEDDING_COLUMNS))
    embeddings_frame = pd.concat([reusable, new_frame], ignore_index=True)
    _validate_embedding_frame(embeddings_frame)
    _atomic_write_parquet(embeddings_frame, output_path, overwrite=overwrite)

    dimension = int(embeddings_frame["embedding_dimension"].iloc[0])
    return EmbeddingBuildResult(
        output_path=output_path,
        embedding_count=len(embeddings_frame),
        new_embedding_count=len(new_frame),
        reused_embedding_count=len(reusable),
        embedding_dimension=dimension,
    )


def default_local_embedding_client(
    settings: RagSettings | None = None,
) -> LocalOpenAICompatibleEmbeddingClient:
    """Return the default local OpenAI-compatible embedding client."""

    settings = settings or load_rag_settings()
    return LocalOpenAICompatibleEmbeddingClient(
        base_url=settings.local_embedding_base_url,
        model=settings.embedding_model,
        timeout_seconds=settings.embedding_timeout_seconds,
    )


def _hash_text_embedding(text: str, *, dimension: int) -> tuple[float, ...]:
    vector = [0.0] * dimension
    tokens = TOKEN_RE.findall(text.casefold())
    features = tokens + [f"{left}_{right}" for left, right in zip(tokens, tokens[1:])]
    if not features:
        features = [text.strip().casefold()]

    for feature in features:
        digest = hashlib.blake2b(
            feature.encode("utf-8"),
            digest_size=8,
            person=b"dn-rag-v1",
        ).digest()
        bucket = int.from_bytes(digest[:4], "big") % dimension
        sign = 1.0 if digest[4] & 1 else -1.0
        vector[bucket] += sign

    return tuple(vector)


def _parse_openai_embedding_payload(
    payload: object,
    *,
    expected_count: int,
) -> tuple[tuple[float, ...], ...]:
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise EmbeddingError("Embedding server returned an unexpected JSON payload shape.")

    data = sorted(payload["data"], key=lambda item: int(item.get("index", 0)))
    if len(data) != expected_count:
        raise EmbeddingError(
            f"Embedding server returned {len(data)} embeddings for {expected_count} texts."
        )

    embeddings: list[tuple[float, ...]] = []
    for item in data:
        vector = item.get("embedding") if isinstance(item, dict) else None
        if not isinstance(vector, list) or not vector:
            raise EmbeddingError("Embedding server returned an empty or malformed vector.")
        try:
            embeddings.append(tuple(float(value) for value in vector))
        except (TypeError, ValueError) as exc:
            raise EmbeddingError("Embedding vector contained a non-numeric value.") from exc

    _validate_embedding_batch(tuple(embeddings), expected_count=expected_count)
    return tuple(embeddings)


def _load_reusable_embeddings(
    *,
    existing_embeddings_path: Path | str | None,
    current_document_hashes: set[str],
    embedding_model: str,
) -> pd.DataFrame:
    if existing_embeddings_path is None:
        return pd.DataFrame(columns=list(REQUIRED_EMBEDDING_COLUMNS))

    path = _require_parquet(existing_embeddings_path, must_exist=True)
    existing = pd.read_parquet(path)
    _validate_columns(existing, REQUIRED_EMBEDDING_COLUMNS, source=path)
    _validate_embedding_frame(existing)

    wrong_model = existing["embedding_model"].astype(str).ne(embedding_model)
    if bool(wrong_model.any()):
        raise EmbeddingError(
            f"Existing embeddings at {path} contain a model different from "
            f"{embedding_model!r}."
        )
    reusable = existing.loc[
        existing["document_hash"].astype(str).isin(current_document_hashes)
    ].copy()
    duplicate_hashes = int(reusable["document_hash"].duplicated().sum())
    if duplicate_hashes:
        raise EmbeddingError(
            f"Existing embeddings at {path} contain {duplicate_hashes} duplicate "
            "document_hash values."
        )
    return reusable.loc[:, list(REQUIRED_EMBEDDING_COLUMNS)].copy()


def _validate_embedding_batch(
    embeddings: tuple[tuple[float, ...], ...],
    *,
    expected_count: int,
) -> None:
    if len(embeddings) != expected_count:
        raise EmbeddingError(
            f"Embedding client returned {len(embeddings)} vectors for {expected_count} texts."
        )
    dimensions = {len(vector) for vector in embeddings}
    if len(dimensions) != 1:
        raise EmbeddingError(f"Embedding client returned mixed dimensions {dimensions}.")
    if not dimensions or next(iter(dimensions)) <= 0:
        raise EmbeddingError("Embedding client returned empty vectors.")


def _validate_embedding_frame(frame: pd.DataFrame) -> None:
    _validate_columns(frame, REQUIRED_EMBEDDING_COLUMNS, source=Path("<embedding-frame>"))
    if frame.empty:
        raise EmbeddingError("Embedding parquet would be empty.")
    dimensions = set(int(value) for value in frame["embedding_dimension"])
    if len(dimensions) != 1:
        raise EmbeddingError(f"Embedding parquet contains mixed dimensions {dimensions}.")
    expected = next(iter(dimensions))
    bad_lengths = [
        index
        for index, embedding in enumerate(frame["embedding"])
        if len(list(embedding)) != expected
    ]
    if bad_lengths:
        raise EmbeddingError(
            f"Embedding parquet contains vectors whose length does not match "
            f"embedding_dimension at rows {bad_lengths[:5]}."
        )


def _atomic_write_parquet(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing parquet {path}.")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f"{path.stem}.",
            suffix=".tmp.parquet",
            dir=path.parent,
            delete=False,
        ) as tmp_file:
            tmp_path = Path(tmp_file.name)
        df.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, path)
    except Exception:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()
        raise


def _validate_columns(df: pd.DataFrame, required: tuple[str, ...], *, source: Path) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise EmbeddingError(
            f"Parquet {source} is missing required embedding columns {missing}. "
            f"Got columns: {sorted(df.columns)}."
        )


def _require_parquet(path: Path | str, *, must_exist: bool) -> Path:
    resolved = Path(path)
    if resolved.suffix != ".parquet":
        raise ValueError(f"Expected a .parquet path. Got {resolved}.")
    if must_exist and not resolved.exists():
        raise FileNotFoundError(f"Parquet file not found at {resolved}.")
    return resolved
