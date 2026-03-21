from __future__ import annotations

import re
import time
from datetime import datetime
from html import unescape
from urllib.parse import quote_plus

from core.base import BaseSource
from core.manifest import (
    ActionOptionSpec,
    DocsSpec,
    SourceActionSpec,
    SourceIdentity,
    SourceManifest,
    StorageSpec,
)
from core.models import HealthRecord, SearchColumnSpec, SearchResult, SearchViewSpec
from core.source_defaults import proxy_url_config
from utils.text import clean_text
from utils.time import utc_now_iso


BLOCKQUOTE_LINE_RE = re.compile(r'<div class="blockquote-like".*?<p>(?P<text>.*?)</p>', re.S)
DT_DD_RE = re.compile(r"<dt>(?P<key>.*?)</dt>\s*<dd>(?P<value>.*?)</dd>", re.S)
LINK_TEXT_RE = re.compile(r"<a\b[^>]*>(?P<text>.*?)</a>", re.S)
CODE_LIKE_RE = re.compile(r"^[A-Z]{2,10}\d{2,6}[A-Z]?$")


class AvwikiSource(BaseSource):
    name = "avwiki"
    display_name = "AV Wiki"
    description = "AV-Wiki Japanese AV metadata search"

    _API_ROOT = "https://av-wiki.net/wp-json/"
    _POSTS_API = "https://av-wiki.net/wp-json/wp/v2/posts"
    _JSON_HEADERS = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    def health(self) -> HealthRecord:
        started_at = time.perf_counter()
        payload = self.http.get_json(self._API_ROOT, headers=self._JSON_HEADERS)
        namespaces = payload.get("namespaces")
        routes = payload.get("routes")
        if not isinstance(namespaces, list) or "wp/v2" not in namespaces:
            raise RuntimeError("avwiki wp-json response missing wp/v2 namespace")
        if not isinstance(routes, dict):
            raise RuntimeError("avwiki wp-json response missing routes")
        latency_ms = int((time.perf_counter() - started_at) * 1000)
        return HealthRecord(
            source=self.name,
            status="ok",
            checked_at=utc_now_iso(),
            latency_ms=latency_ms,
            error=None,
            details="av-wiki rest reachable",
        )

    def search_content(
        self,
        channel_key: str | None = None,
        query: str | None = None,
        since: datetime | None = None,
        limit: int = 20,
    ) -> list[SearchResult]:
        if channel_key is not None:
            raise RuntimeError("avwiki content search does not support --channel")
        if since is not None:
            raise RuntimeError("avwiki content search does not support --since")
        if query is None:
            return []
        normalized_query = query.strip()
        if not normalized_query:
            raise RuntimeError("avwiki search query cannot be empty")
        if limit <= 0:
            return []

        page_size = min(limit, 100)
        results: list[SearchResult] = []
        page = 1
        while len(results) < limit:
            page_results = self._fetch_search_page(normalized_query, page=page, page_size=page_size)
            if not page_results:
                break
            results.extend(page_results)
            if len(page_results) < page_size:
                break
            page += 1
        results = self._filter_exact_code_matches(normalized_query, results)
        return results[:limit]

    def get_content_search_view(self, channel_key: str | None) -> SearchViewSpec | None:
        _ = channel_key
        return SearchViewSpec(
            columns=[
                SearchColumnSpec("title", lambda item: item.title, max_width=40),
                SearchColumnSpec("actresses", lambda item: self._meta(item, "actresses"), max_width=24),
                SearchColumnSpec("label", lambda item: self._meta(item, "label"), max_width=18),
                SearchColumnSpec("code", lambda item: self._meta(item, "code"), no_wrap=True),
                SearchColumnSpec("delivered_at", lambda item: self._meta(item, "delivered_at"), no_wrap=True),
                SearchColumnSpec("url", lambda item: item.url, no_wrap=True, max_width=56),
            ]
        )

    def _fetch_search_page(self, query: str, *, page: int, page_size: int) -> list[SearchResult]:
        url = f"{self._POSTS_API}?search={quote_plus(query)}&per_page={page_size}&page={page}"
        payload = self.http.get_json(url, headers=self._JSON_HEADERS)
        if not isinstance(payload, list):
            raise RuntimeError("avwiki posts response must be a list")
        results: list[SearchResult] = []
        for post in payload:
            if not isinstance(post, dict):
                continue
            item = self._parse_post(post)
            if item is not None:
                results.append(item)
        return results

    def _parse_post(self, post: dict[str, object]) -> SearchResult | None:
        url = self._coerce_text(post.get("link"))
        content = post.get("content")
        if not isinstance(content, dict):
            return None
        rendered = self._coerce_text(content.get("rendered"))
        if not url or not rendered:
            return None

        field_values = self._parse_field_values(rendered)
        actresses = self._extract_actresses(field_values.get("AV女優名", ""))
        code = clean_text(field_values.get("メーカー品番", ""))
        delivered_at = clean_text(field_values.get("配信開始日", ""))
        label = clean_text(field_values.get("レーベル", ""))
        title = self._extract_title(rendered, code=code, actresses=actresses)
        if not title or not code:
            return None

        metadata = {
            "actresses": ", ".join(actresses),
            "label": label,
            "code": code,
            "delivered_at": delivered_at,
        }
        snippet = (
            f"code={code}; actresses={metadata['actresses']}; "
            f"label={label}; delivered_at={delivered_at}"
        )
        return SearchResult(
            title=title,
            url=url,
            snippet=snippet,
            source=self.name,
            metadata=metadata,
        )

    def _parse_field_values(self, rendered: str) -> dict[str, str]:
        field_values: dict[str, str] = {}
        for match in DT_DD_RE.finditer(rendered):
            key = clean_text(match.group("key"))
            if not key:
                continue
            field_values[key] = match.group("value")
        return field_values

    def _extract_actresses(self, value_html: str) -> list[str]:
        actresses = [clean_text(unescape(match.group("text"))) for match in LINK_TEXT_RE.finditer(value_html)]
        actresses = [item for item in actresses if item]
        if actresses:
            return actresses
        value = clean_text(value_html)
        if not value:
            return []
        return [value]

    def _extract_title(self, rendered: str, *, code: str, actresses: list[str]) -> str:
        match = BLOCKQUOTE_LINE_RE.search(rendered)
        if match is None:
            return ""
        line = clean_text(match.group("text"))
        if not line:
            return ""
        prefix = f"【{code}】"
        if line.startswith(prefix):
            line = line[len(prefix) :].strip()
        actresses_text = " ".join(actresses).strip()
        if actresses_text and line.endswith(actresses_text):
            line = line[: -len(actresses_text)].rstrip()
        return line.strip()

    def _filter_exact_code_matches(self, query: str, results: list[SearchResult]) -> list[SearchResult]:
        if not self._looks_like_code(query):
            return results
        normalized_query = self._normalize_code(query)
        exact = [item for item in results if self._normalize_code(self._meta(item, "code")) == normalized_query]
        if exact:
            return exact
        return results

    @staticmethod
    def _looks_like_code(query: str) -> bool:
        if " " in query.strip():
            return False
        return CODE_LIKE_RE.fullmatch(AvwikiSource._normalize_code(query)) is not None

    @staticmethod
    def _normalize_code(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9]", "", value).upper()

    @staticmethod
    def _coerce_text(value: object) -> str:
        if isinstance(value, str):
            return value
        return ""

    @staticmethod
    def _meta(item: SearchResult, key: str) -> str:
        if item.metadata is None:
            return ""
        return item.metadata.get(key, "")


MANIFEST = SourceManifest(
    identity=SourceIdentity(
        name="avwiki",
        display_name="AV Wiki",
        summary="AV-Wiki Japanese AV metadata search",
    ),
    mode=None,
    config_fields=(proxy_url_config(),),
    source_actions={
        "source.health": SourceActionSpec(name="source.health", summary="Check AV-Wiki REST reachability"),
        "content.search": SourceActionSpec(
            name="content.search",
            summary="Search Japanese AV metadata by code, actress, or label",
            options={
                "query": ActionOptionSpec(name="query"),
                "limit": ActionOptionSpec(name="limit"),
            },
        ),
    },
    query=None,
    interaction_verbs={},
    storage=StorageSpec(
        table_name="avwiki_records",
        required_record_fields=(
            "source",
            "content_key",
            "content_type",
            "title",
            "url",
            "fetched_at",
            "raw_payload",
        ),
    ),
    docs=DocsSpec(
        notes=("这是 discovery-only source，不支持 channel、订阅、更新和本地 query。",),
        examples=(
            "adc content search --source avwiki --query SSIS-001 --limit 5",
            "adc content search --source avwiki --query 葵つかさ --limit 10",
        ),
    ),
)

SOURCE_CLASS = AvwikiSource
AvwikiSource.manifest = MANIFEST
