"""Run the local async API that serves cached plans and static images."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import uvicorn  # noqa: E402

from scripts.run_precache import _load_repo_env  # noqa: E402


def main() -> None:
    _load_repo_env()
    uvicorn.run(
        "back_end.api.app:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
    )


if __name__ == "__main__":
    main()
