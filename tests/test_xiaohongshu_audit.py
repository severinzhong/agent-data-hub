from __future__ import annotations

from pathlib import Path
import os
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

import json
import tempfile
import unittest
from unittest.mock import patch

from cli import main as cli_main
from store.db import Store
from tests.fixtures import default_storage_specs


class XiaohongshuAuditTests(unittest.TestCase):
    def test_xiaohongshu_content_interact_writes_action_audit(self) -> None:
        class FakeClient:
            def __init__(self, *, cookies, **kwargs) -> None:
                _ = cookies, kwargs

            def __enter__(self):
                return self

            def __exit__(self, *args) -> None:
                return None

            def favorite_note(self, note_id: str) -> dict[str, object]:
                self.note_id = note_id
                return {"ok": True}

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "xiaohongshu-audit.db")
            with (
                patch.object(cli_main, "DEFAULT_DB_PATH", db_path),
                patch("sources.xiaohongshu.source.XiaohongshuClient", FakeClient),
            ):
                self.assertEqual(
                    cli_main.main(
                        [
                            "config",
                            "source",
                            "set",
                            "xiaohongshu",
                            "cookie",
                            "a1=token; web_session=session",
                        ]
                    ),
                    0,
                )
                self.assertEqual(
                    cli_main.main(
                        [
                            "content",
                            "interact",
                            "--source",
                            "xiaohongshu",
                            "--verb",
                            "favorite",
                            "--ref",
                            "xiaohongshu:content/note%3Anote-1",
                        ]
                    ),
                    0,
                )

            store = Store(db_path)
            store.init_schema(storage_specs=default_storage_specs())
            row = store._connection.execute(
                "SELECT action, target_kind, status, dry_run, targets_json FROM action_audits ORDER BY audit_id DESC"
            ).fetchone()
            self.assertEqual(row["action"], "content.interact")
            self.assertEqual(row["target_kind"], "content_ref")
            self.assertEqual(row["status"], "ok")
            self.assertEqual(row["dry_run"], 0)
            self.assertEqual(json.loads(row["targets_json"]), ["xiaohongshu:content/note%3Anote-1"])


if __name__ == "__main__":
    unittest.main()
