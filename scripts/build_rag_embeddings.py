"""Build derived embeddings for a RAG corpus using a local embedding server.

Run from the repo root after starting the local embedding server:

    source .venv/bin/activate
    python scripts/build_rag_embeddings.py \
      --documents data/rag/runs/<run-id>/place_documents.parquet
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from back_end.rag.embeddings import (  # noqa: E402
    EmbeddingError,
    LocalHashingEmbeddingClient,
    LocalOpenAICompatibleEmbeddingClient,
    build_rag_embeddings,
)
from back_end.rag.settings import load_rag_settings  # noqa: E402


def parse_args() -> argparse.Namespace:
    settings = load_rag_settings()
    parser = argparse.ArgumentParser(
        description="Embed RAG place documents through a local OpenAI-compatible server."
    )
    parser.add_argument(
        "--backend",
        choices=("server", "hashing"),
        default="server",
        help=(
            "Embedding backend. 'server' calls an OpenAI-compatible local server; "
            "'hashing' uses an in-process deterministic lexical backend."
        ),
    )
    parser.add_argument(
        "--documents",
        type=Path,
        required=True,
        help="Derived place_documents.parquet from scripts/build_rag_corpus.py.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Destination place_embeddings.parquet. Defaults beside --documents.",
    )
    parser.add_argument(
        "--existing-embeddings",
        type=Path,
        default=None,
        help="Optional existing embeddings parquet to reuse by document_hash.",
    )
    parser.add_argument(
        "--base-url",
        default=settings.local_embedding_base_url,
        help="Local OpenAI-compatible base URL.",
    )
    parser.add_argument(
        "--model",
        default=settings.embedding_model,
        help="Embedding model name exposed by the local server.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.embedding_batch_size,
        help="Number of documents per embedding request.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=settings.embedding_timeout_seconds,
        help="HTTP timeout for embedding requests.",
    )
    parser.add_argument(
        "--hashing-dimension",
        type=int,
        default=512,
        help="Vector dimension for --backend hashing.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the derived embeddings output parquet.",
    )
    return parser.parse_args()


async def async_main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("build_rag_embeddings")
    output = args.output or args.documents.with_name("place_embeddings.parquet")
    if args.backend == "hashing":
        client = LocalHashingEmbeddingClient(dimension=args.hashing_dimension)
        embedding_model = client.model_name
    else:
        client = LocalOpenAICompatibleEmbeddingClient(
            base_url=args.base_url,
            model=args.model,
            timeout_seconds=args.timeout_seconds,
        )
        embedding_model = args.model

    try:
        if args.backend == "server":
            await _preflight_embedding_server(
                base_url=args.base_url,
                model=args.model,
                timeout_seconds=min(args.timeout_seconds, 5.0),
            )
        result = await build_rag_embeddings(
            documents_path=args.documents,
            output_path=output,
            client=client,
            embedding_model=embedding_model,
            batch_size=args.batch_size,
            overwrite=args.overwrite,
            existing_embeddings_path=args.existing_embeddings,
        )
    except EmbeddingError as exc:
        log.error("Failed to build RAG embeddings: %s", exc)
        return 1
    except Exception:
        log.exception("Failed to build RAG embeddings.")
        return 1

    log.info(
        "Embedding build completed: %s rows, %s new, %s reused, dim=%s at %s",
        result.embedding_count,
        result.new_embedding_count,
        result.reused_embedding_count,
        result.embedding_dimension,
        result.output_path,
    )
    return 0


async def _preflight_embedding_server(
    *,
    base_url: str,
    model: str,
    timeout_seconds: float,
) -> None:
    models_url = f"{base_url.rstrip('/')}/models"
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.get(models_url)
    except httpx.HTTPError as exc:
        raise EmbeddingError(
            "No local embedding server is reachable. Start an OpenAI-compatible "
            f"embedding server before running this script. Tried: {models_url}. "
            f"Configured embedding model: {model!r}. If your server uses a different "
            "URL or model name, pass --base-url and --model."
        ) from exc
    if response.status_code >= 400:
        raise EmbeddingError(
            f"Embedding server preflight failed at {models_url} with HTTP "
            f"{response.status_code}: {response.text[:500]}"
        )


if __name__ == "__main__":
    raise SystemExit(asyncio.run(async_main()))
