from __future__ import annotations

from pathlib import Path
import os
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

import io
import json
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from cli import main as cli_main
from cli.main import main
from core.config import ResolvedSourceConfig, SourceConfigEntry
from sources.rsshub.source import RsshubSource
from utils.time import utc_now_iso


ROUTES_JSON = {
    "demo": {
        "name": "Demo",
        "url": "demo.local",
        "description": "",
        "categories": ["social-media"],
        "heat": 1,
        "routes": {
            "/demo/item/:id": {
                "path": "/item/:id",
                "name": "Demo Item",
                "url": "demo.local/item/:id",
                "maintainers": ["test"],
                "example": "/demo/item/42",
                "description": "demo route",
                "categories": ["social-media"],
                "features": {"requireConfig": False, "requirePuppeteer": False, "antiCrawler": False, "supportRadar": True, "supportBT": False, "supportPodcast": False, "supportScihub": False},
                "radar": [],
                "topFeeds": [
                    {"id": "1", "type": "feed", "url": "rsshub://demo/item/42", "title": "Demo Item 42", "description": "demo", "image": None}
                ],
            }
        },
    }
}

RSS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Demo feed</title>
    <item>
      <guid>a1</guid>
      <title>new item</title>
      <link>https://demo.local/new</link>
      <description>new desc</description>
      <pubDate>Wed, 11 Mar 2026 10:00:00 GMT</pubDate>
    </item>
    <item>
      <guid>a0</guid>
      <title>old item</title>
      <link>https://demo.local/old</link>
      <description>old desc</description>
      <pubDate>Tue, 10 Mar 2026 10:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""


def _build_config(base_url: str, routes_json_url: str) -> ResolvedSourceConfig:
    now = utc_now_iso()
    entries = {
        "base_url": SourceConfigEntry(source="rsshub", key="base_url", value=base_url, value_type="string", is_secret=False, updated_at=now),
        "routes_json_url": SourceConfigEntry(source="rsshub", key="routes_json_url", value=routes_json_url, value_type="string", is_secret=False, updated_at=now),
    }
    return ResolvedSourceConfig(source="rsshub", entries=entries)


class _FakeHttp:
    def __init__(self, routes_json: dict, feed_xml: str) -> None:
        self._routes_json = routes_json
        self._feed_xml = feed_xml

    def get_json(self, _url: str):
        return self._routes_json

    def get_text(self, _url: str, **_kwargs):
        return self._feed_xml


class RsshubSourceTests(unittest.TestCase):
    def test_search_channels_returns_channel_records(self) -> None:
        source = RsshubSource(store=None, config=ResolvedSourceConfig.empty("rsshub"))
        source.http = _FakeHttp(ROUTES_JSON, RSS_XML)
        results = source.search_channels("42", limit=10)
        self.assertGreaterEqual(len(results), 1)
        self.assertEqual(results[0].channel_key, "/demo/item/42")

    def test_fetch_content_respects_since(self) -> None:
        source = RsshubSource(store=None, config=_build_config("http://127.0.0.1:1200", "http://127.0.0.1/routes.json"))
        source.http = _FakeHttp(ROUTES_JSON, RSS_XML)
        records = source.fetch_content(
            channel_key="/demo/item/42",
            since=datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc),
            limit=10,
            fetch_all=True,
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].title, "new item")


class _CliTestHandler(BaseHTTPRequestHandler):
    routes_json = ROUTES_JSON
    rss_xml = RSS_XML

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/routes.json":
            payload = json.dumps(self.routes_json).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path == "/feed/demo/item/42":
            payload = self.rss_xml.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/rss+xml; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, _format: str, *_args) -> None:
        return


class CliIntegrationTests(unittest.TestCase):
    def test_cli_channel_search_sub_update_query(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), _CliTestHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_port}"
        with tempfile.TemporaryDirectory() as tempdir:
            db_path = os.path.join(tempdir, "rsshub.db")
            with patch.object(cli_main, "DEFAULT_DB_PATH", db_path):
                self.assertEqual(
                    main(["config", "source", "set", "rsshub", "base_url", f"{base}/feed"]),
                    0,
                )
                self.assertEqual(
                    main(
                        [
                            "config",
                            "source",
                            "set",
                            "rsshub",
                            "routes_json_url",
                            f"{base}/routes.json",
                        ]
                    ),
                    0,
                )
                out_search = io.StringIO()
                with redirect_stdout(out_search):
                    self.assertEqual(main(["channel", "search", "--source", "rsshub", "--query", "42", "--jsonl"]), 0)
                rows = [json.loads(line) for line in out_search.getvalue().splitlines() if line.strip()]
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0]["channel_key"], "/demo/item/42")
                self.assertEqual(main(["sub", "add", "--source", "rsshub", "--channel", "/demo/item/42"]), 0)
                self.assertEqual(main(["content", "update", "--source", "rsshub", "--channel", "/demo/item/42", "--limit", "10"]), 0)
                out_query = io.StringIO()
                with redirect_stdout(out_query):
                    self.assertEqual(main(["content", "query", "--source", "rsshub", "--channel", "/demo/item/42", "--jsonl"]), 0)
                rows = [json.loads(line) for line in out_query.getvalue().splitlines() if line.strip()]
                self.assertEqual(len(rows), 2)
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    unittest.main()
