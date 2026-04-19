from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from scripts.enrich_no_website_profiles import _load_env_file


class EnrichNoWebsiteProfilesScriptTests(unittest.TestCase):
    def test_load_env_file_sets_missing_keys_without_overwriting_existing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "BRAVE_API_KEY=from-file\nEXISTING_KEY=from-file\n",
                encoding="utf-8",
            )
            old_brave = os.environ.pop("BRAVE_API_KEY", None)
            old_existing = os.environ.get("EXISTING_KEY")
            os.environ["EXISTING_KEY"] = "already-set"
            self.addCleanup(self._restore_env, "BRAVE_API_KEY", old_brave)
            self.addCleanup(self._restore_env, "EXISTING_KEY", old_existing)

            _load_env_file(env_path)

            self.assertEqual(os.environ["BRAVE_API_KEY"], "from-file")
            self.assertEqual(os.environ["EXISTING_KEY"], "already-set")

    @staticmethod
    def _restore_env(name: str, value: str | None) -> None:
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value
