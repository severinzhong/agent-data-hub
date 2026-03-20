from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
import html
import time
from urllib.parse import quote_plus, urlparse

from core.base import BaseSource
from core.manifest import (
    ActionOptionSpec,
    DocsSpec,
    QuerySpec,
    SourceActionSpec,
    SourceIdentity,
    SourceManifest,
    StorageSpec,
)
from core.models import (
    ChannelRecord,
    ContentChannelLink,
    ContentNode,
    ContentSyncBatch,
    HealthRecord,
    QueryColumnSpec,
    QueryViewSpec,
    SearchResult,
)
from core.source_defaults import proxy_url_config
from utils.text import clean_text
from utils.time import unix_to_iso, utc_now_iso


AP_CHANNELS = {
    "world-news": {
        "display_name": "World News",
        "url": "https://apnews.com/hub/world-news",
    },
    "business": {
        "display_name": "Business",
        "url": "https://apnews.com/hub/business",
    },
    "technology": {
        "display_name": "Technology",
        "url": "https://apnews.com/hub/technology",
    },
}

_VOID_TAGS = {
    "area",
    "base",
    "br",
    "col",
    "embed",
    "hr",
    "img",
    "input",
    "link",
    "meta",
    "param",
    "source",
    "track",
    "wbr",
}


@dataclass(slots=True)
class _ParsedCard:
    url: str
    title: str
    snippet: str
    posted_timestamp_ms: int | None
    updated_timestamp_ms: int | None
    raw_html: str


@dataclass(slots=True)
class _MutableCard:
    url: str | None = None
    posted_timestamp_ms: int | None = None
    updated_timestamp_ms: int | None = None
    title_parts: list[str] | None = None
    snippet_parts: list[str] | None = None
    raw_parts: list[str] | None = None


class _ApCardParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.cards: list[_ParsedCard] = []
        self._depth = 0
        self._card: _MutableCard | None = None
        self._card_root_depth: int | None = None
        self._card_root_tag: str | None = None
        self._title_depth: int | None = None
        self._snippet_depth: int | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._depth += 1
        attrs_map = {key: value or "" for key, value in attrs}
        classes = set(attrs_map.get("class", "").split())

        if self._card is None and "PagePromo" in classes:
            self._card = _MutableCard(title_parts=[], snippet_parts=[], raw_parts=[self.get_starttag_text()])
            self._card_root_depth = self._depth
            self._card_root_tag = tag
            posted_timestamp = attrs_map.get("data-posted-date-timestamp", "")
            if posted_timestamp.isdigit():
                self._card.posted_timestamp_ms = int(posted_timestamp)
            updated_timestamp = attrs_map.get("data-updated-date-timestamp", "")
            if updated_timestamp.isdigit():
                self._card.updated_timestamp_ms = int(updated_timestamp)
            if tag in _VOID_TAGS:
                self._depth -= 1
            return

        if self._card is None:
            if tag in _VOID_TAGS:
                self._depth -= 1
            return

        self._card.raw_parts.append(self.get_starttag_text())

        if "PagePromo-title" in classes:
            self._title_depth = self._depth
        if "PagePromo-description" in classes:
            self._snippet_depth = self._depth

        href = attrs_map.get("href", "")
        if href.startswith("https://apnews.com/") and self._card.url is None:
            if self._title_depth is not None or self._snippet_depth is not None:
                self._card.url = href

        if tag in _VOID_TAGS:
            self._depth -= 1

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._card is not None:
            rendered_attrs = "".join(
                f' {key}="{html.escape(value or "", quote=True)}"'
                for key, value in attrs
            )
            self._card.raw_parts.append(f"<{tag}{rendered_attrs}/>")

    def handle_endtag(self, tag: str) -> None:
        if self._card is not None:
            self._card.raw_parts.append(f"</{tag}>")

            if self._depth == self._title_depth:
                self._title_depth = None
            if self._depth == self._snippet_depth:
                self._snippet_depth = None

            if self._depth == self._card_root_depth and tag == self._card_root_tag:
                self._finalize_card()

        self._depth -= 1

    def handle_data(self, data: str) -> None:
        if self._card is None:
            return
        self._card.raw_parts.append(data)
        if self._title_depth is not None:
            self._card.title_parts.append(data)
        if self._snippet_depth is not None:
            self._card.snippet_parts.append(data)

    def handle_entityref(self, name: str) -> None:
        self._append_text(f"&{name};")

    def handle_charref(self, name: str) -> None:
        self._append_text(f"&#{name};")

    def _append_text(self, value: str) -> None:
        if self._card is None:
            return
        self._card.raw_parts.append(value)
        if self._title_depth is not None:
            self._card.title_parts.append(value)
        if self._snippet_depth is not None:
            self._card.snippet_parts.append(value)

    def _finalize_card(self) -> None:
        if self._card is None:
            return

        title = clean_text("".join(self._card.title_parts or []))
        snippet = clean_text("".join(self._card.snippet_parts or [])) or title
        raw_html = "".join(self._card.raw_parts or [])

        if self._card.url and title:
            self.cards.append(
                _ParsedCard(
                    url=self._card.url,
                    title=title,
                    snippet=snippet,
                    posted_timestamp_ms=self._card.posted_timestamp_ms,
                    updated_timestamp_ms=self._card.updated_timestamp_ms,
                    raw_html=raw_html,
                )
            )

        self._card = None
        self._card_root_depth = None
        self._card_root_tag = None
        self._title_depth = None
        self._snippet_depth = None


class ApSource(BaseSource):
    name = "ap"
    display_name = "AP News"
    description = "AP News fixed channels and site search"

    def list_channels(self) -> list[ChannelRecord]:
        return [
            ChannelRecord(
                source=self.name,
                channel_id=channel_key,
                channel_key=channel_key,
                display_name=payload["display_name"],
                url=payload["url"],
                metadata={},
            )
            for channel_key, payload in AP_CHANNELS.items()
        ]

    def health(self) -> HealthRecord:
        started_at = time.perf_counter()
        self.http.get_text(AP_CHANNELS["world-news"]["url"])
        latency_ms = int((time.perf_counter() - started_at) * 1000)
        return HealthRecord(
            source=self.name,
            status="ok",
            checked_at=utc_now_iso(),
            latency_ms=latency_ms,
            error=None,
            details="ap world-news page reachable",
        )

    def search_content(
        self,
        channel_key: str | None = None,
        query: str | None = None,
        since: datetime | None = None,
        limit: int = 20,
    ) -> list[SearchResult]:
        _ = channel_key, since
        if not query:
            return []
        html_body = self.http.get_text(f"https://apnews.com/search?q={quote_plus(query)}")
        return [
            SearchResult(
                title=card.title,
                url=card.url,
                snippet=card.snippet,
                source=self.name,
            )
            for card in self._parse_cards(html_body)[:limit]
        ]

    def get_query_view(self, channel_key: str | None = None) -> QueryViewSpec | None:
        _ = channel_key
        return QueryViewSpec(
            columns=[
                QueryColumnSpec("published_at", lambda item: item.published_at or "", no_wrap=True, max_width=20),
                QueryColumnSpec("title", lambda item: item.title, max_width=20),
                QueryColumnSpec("snippet", lambda item: item.snippet, max_width=24),
                QueryColumnSpec("url", lambda item: item.url, max_width=28),
            ]
        )

    def fetch_content(
        self,
        channel_key: str,
        since: datetime | None = None,
        limit: int | None = 20,
        fetch_all: bool = False,
    ) -> ContentSyncBatch:
        channel = self.get_channel(channel_key)
        html_body = self.http.get_text(channel.url)
        cards = self._parse_cards(html_body)
        nodes: list[ContentNode] = []

        for card in cards:
            fetched_at = utc_now_iso()
            published_at = (
                self._timestamp_ms_to_iso(card.posted_timestamp_ms)
                or self._timestamp_ms_to_iso(card.updated_timestamp_ms)
                or fetched_at
            )
            if since is not None and published_at < since.astimezone(UTC).isoformat():
                continue
            content_type, external_id = self._content_identity_from_url(card.url)
            nodes.append(
                ContentNode(
                    source=self.name,
                    content_key=f"{content_type}:{external_id}",
                    content_type=content_type,
                    external_id=external_id,
                    title=card.title,
                    url=card.url,
                    snippet=card.snippet,
                    author=None,
                    published_at=published_at,
                    fetched_at=fetched_at,
                    raw_payload=card.raw_html,
                )
            )

        if not fetch_all:
            nodes = nodes[: (limit or 20)]

        return ContentSyncBatch(
            nodes=nodes,
            channel_links=[
                ContentChannelLink(
                    source=self.name,
                    channel_key=channel.channel_key,
                    content_key=node.content_key,
                    membership_kind="direct",
                )
                for node in nodes
            ],
            relations=[],
        )

    def _parse_cards(self, html_body: str) -> list[_ParsedCard]:
        parser = _ApCardParser()
        parser.feed(html_body)
        return parser.cards

    def _timestamp_ms_to_iso(self, timestamp_ms: int | None) -> str | None:
        if timestamp_ms is None:
            return None
        return unix_to_iso(timestamp_ms // 1000)

    def _content_identity_from_url(self, url: str) -> tuple[str, str]:
        parsed = urlparse(url)
        path_parts = [part for part in parsed.path.split("/") if part]
        if len(path_parts) != 2:
            raise RuntimeError(f"{self.name} returned unsupported detail url shape: {url}")
        raw_kind, external_id = path_parts
        content_type = raw_kind.strip().replace("-", "_")
        if not content_type:
            raise RuntimeError(f"{self.name} returned unsupported detail url type: {url}")
        if not external_id:
            raise RuntimeError(f"{self.name} returned empty detail slug: {url}")
        return content_type, external_id


MANIFEST = SourceManifest(
    identity=SourceIdentity(name="ap", display_name="AP News", summary="AP News fixed channels and site search"),
    mode=None,
    config_fields=(proxy_url_config(),),
    source_actions={
        "source.health": SourceActionSpec(name="source.health", summary="Check AP News reachability"),
        "channel.list": SourceActionSpec(name="channel.list", summary="List built-in AP News channels"),
        "content.search": SourceActionSpec(
            name="content.search",
            summary="Search AP News site content",
            options={
                "query": ActionOptionSpec(name="query"),
                "limit": ActionOptionSpec(name="limit"),
            },
        ),
        "content.update": SourceActionSpec(
            name="content.update",
            summary="Fetch subscribed AP channel cards into local store",
            options={
                "channel": ActionOptionSpec(name="channel"),
                "since": ActionOptionSpec(name="since"),
                "limit": ActionOptionSpec(name="limit"),
                "all": ActionOptionSpec(name="all"),
            },
        ),
    },
    query=QuerySpec(time_field="published_at", supports_keywords=True),
    interaction_verbs={},
    storage=StorageSpec(
        table_name="ap_records",
        required_record_fields=(
            "source",
            "content_key",
            "content_type",
            "external_id",
            "title",
            "url",
            "snippet",
            "published_at",
            "fetched_at",
            "raw_payload",
        ),
    ),
    docs=DocsSpec(
        examples=(
            "adc content search --source ap --query openai --limit 5",
            "adc content update --source ap --channel world-news --limit 10",
            "adc content query --source ap --limit 20",
        ),
    ),
)

SOURCE_CLASS = ApSource
ApSource.manifest = MANIFEST
