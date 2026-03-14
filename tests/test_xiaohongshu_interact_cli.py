from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
import os
os.chdir(REPO_ROOT)

import tempfile
import unittest
from unittest.mock import patch

from cli import main as cli_main


class XiaohongshuInteractCliTests(unittest.TestCase):
    def test_xiaohongshu_content_interact_like_calls_note_endpoint(self) -> None:
        class FakeClient:
            like_calls: list[str] = []

            def __init__(self, *, cookies, **kwargs) -> None:
                _ = cookies, kwargs

            def __enter__(self):
                return self

            def __exit__(self, *args) -> None:
                return None

            def like_note(self, note_id: str) -> dict[str, object]:
                self.__class__.like_calls.append(note_id)
                return {"ok": True}

        with (
            tempfile.TemporaryDirectory() as temp_dir,
            patch.object(cli_main, "DEFAULT_DB_PATH", os.path.join(temp_dir, "xiaohongshu-interact.db")),
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
                        "like",
                        "--ref",
                        "xiaohongshu:content/note%3Anote-1",
                    ]
                ),
                0,
            )
        self.assertEqual(FakeClient.like_calls, ["note-1"])

    def test_xiaohongshu_content_interact_rejects_non_note_ref(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            cli_main, "DEFAULT_DB_PATH", os.path.join(temp_dir, "xiaohongshu-ref.db")
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
            with self.assertRaisesRegex(RuntimeError, "note:<note_id>"):
                cli_main.main(
                    [
                        "content",
                        "interact",
                        "--source",
                        "xiaohongshu",
                        "--verb",
                        "like",
                        "--ref",
                        "xiaohongshu:content/comment%3A1",
                    ]
                )

    def test_xiaohongshu_content_interact_comment_requires_text(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            cli_main, "DEFAULT_DB_PATH", os.path.join(temp_dir, "xiaohongshu-comment.db")
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
            with self.assertRaises(SystemExit):
                cli_main.main(
                    [
                        "content",
                        "interact",
                        "--source",
                        "xiaohongshu",
                        "--verb",
                        "comment",
                        "--ref",
                        "xiaohongshu:content/note%3Anote-1",
                    ]
                )


if __name__ == "__main__":
    unittest.main()
