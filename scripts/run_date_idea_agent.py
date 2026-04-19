"""Run the RAG-backed date-idea agent from local parquet artifacts."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from back_end.agents.date_idea_agent import (  # noqa: E402
    DEFAULT_DATE_IDEA_AGENT_MODEL,
    DEFAULT_REASONING_EFFORT,
    DateIdeaAgent,
    DateIdeaRequest,
    RagPlaceSearchTool,
)
from back_end.clients.openrouter import OpenRouterClient  # noqa: E402
from back_end.clients.settings import (  # noqa: E402
    OpenRouterConfigurationError,
    OpenRouterSettings,
)
from back_end.rag.embeddings import (  # noqa: E402
    LOCAL_HASHING_EMBEDDING_MODEL_PREFIX,
    LocalHashingEmbeddingClient,
    default_local_embedding_client,
)
from back_end.rag.settings import load_rag_settings  # noqa: E402
from back_end.rag.vector_store import ExactVectorStore  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate grounded date ideas with the RAG-backed LLM agent."
    )
    parser.add_argument(
        "--rag-run",
        default=None,
        help=(
            "RAG run id under data/rag/runs. When supplied, documents and "
            "embeddings default to that run's parquet files."
        ),
    )
    parser.add_argument("--documents", type=Path, default=None)
    parser.add_argument("--embeddings", type=Path, default=None)
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--location", default="Sydney")
    parser.add_argument("--time-window", default=None)
    parser.add_argument("--vibe", default=None)
    parser.add_argument("--budget", default=None)
    parser.add_argument("--transport-mode", default=None)
    parser.add_argument("--party-size", type=int, default=2)
    parser.add_argument("--constraints", default=None)
    parser.add_argument("--desired-idea-count", type=int, default=3)
    parser.add_argument("--model", default=DEFAULT_DATE_IDEA_AGENT_MODEL)
    parser.add_argument("--reasoning-effort", default=DEFAULT_REASONING_EFFORT)
    return parser.parse_args()


async def async_main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("run_date_idea_agent")

    try:
        _load_repo_env()
        documents_path, embeddings_path = _resolve_rag_paths(args)
        vector_store = ExactVectorStore.from_parquet(
            documents_path=documents_path,
            embeddings_path=embeddings_path,
        )
        embedding_client = _embedding_client_for_existing_embeddings(embeddings_path)
        rag_tool = RagPlaceSearchTool(
            vector_store=vector_store,
            embedding_client=embedding_client,
        )
        async with OpenRouterClient(OpenRouterSettings.from_env()) as llm_client:
            agent = DateIdeaAgent(
                llm_client=llm_client,
                rag_search_tool=rag_tool,
                model=args.model,
                reasoning_effort=args.reasoning_effort,
            )
            result = await agent.generate(
                DateIdeaRequest(
                    prompt=args.prompt,
                    location=args.location,
                    time_window=args.time_window,
                    vibe=args.vibe,
                    budget=args.budget,
                    transport_mode=args.transport_mode,
                    party_size=args.party_size,
                    constraints=args.constraints,
                    desired_idea_count=args.desired_idea_count,
                )
            )
    except OpenRouterConfigurationError as exc:
        log.error("Date-idea agent is missing OpenRouter configuration: %s", exc)
        return 1
    except Exception:
        log.exception("Date-idea agent run failed.")
        return 1

    print(json.dumps(result.raw_output, indent=2, sort_keys=True))
    log.info(
        "Generated %d ideas using %d RAG tool calls.",
        len(result.ideas),
        len(result.tool_executions),
    )
    return 0


def _resolve_rag_paths(args: argparse.Namespace) -> tuple[Path, Path]:
    if args.rag_run:
        if "<" in args.rag_run or ">" in args.rag_run:
            raise ValueError(
                "--rag-run must be an actual run id, not a placeholder like <run-id>."
            )
        run_dir = load_rag_settings().rag_runs_root / args.rag_run
        documents = args.documents or run_dir / "place_documents.parquet"
        embeddings = args.embeddings or run_dir / "place_embeddings.parquet"
    else:
        if args.documents is None or args.embeddings is None:
            raise ValueError(
                "Pass --rag-run or pass both --documents and --embeddings."
            )
        documents = args.documents
        embeddings = args.embeddings

    missing = [str(path) for path in (documents, embeddings) if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing required RAG parquet file(s): "
            + ", ".join(missing)
            + ". Build the corpus and embeddings before running the agent."
        )
    return documents, embeddings


def _embedding_client_for_existing_embeddings(embeddings_path: Path):
    metadata = pd.read_parquet(embeddings_path, columns=["embedding_model"])
    if metadata.empty:
        raise ValueError(f"Embedding parquet {embeddings_path} is empty.")
    models = set(metadata["embedding_model"].astype(str))
    if len(models) != 1:
        raise ValueError(
            f"Embedding parquet {embeddings_path} contains mixed models: {sorted(models)}."
        )
    model = next(iter(models))
    if model.startswith(f"{LOCAL_HASHING_EMBEDDING_MODEL_PREFIX}:"):
        raw_dimension = model.rsplit(":", maxsplit=1)[-1]
        try:
            dimension = int(raw_dimension)
        except ValueError as exc:
            raise ValueError(
                f"Could not parse hashing embedding dimension from model {model!r}."
            ) from exc
        return LocalHashingEmbeddingClient(dimension=dimension)
    return default_local_embedding_client()


def _load_repo_env() -> None:
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key or key in os.environ:
            continue
        os.environ[key] = value


if __name__ == "__main__":
    raise SystemExit(asyncio.run(async_main()))
