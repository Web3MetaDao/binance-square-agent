"""
blog_feed.py — BlogFeedHarvester

Polls RSS/Atom feeds from institutional crypto / quant finance blogs:
  - Jump Crypto (https://jumpcrypto.com/feed/)
  - Jane Street (https://blog.janestreet.com/feed.xml)
  - Glassnode (https://glassnode.com/blog/feed)
"""

from datetime import datetime, timezone
from typing import Optional

import httpx
import feedparser

# ── Constants ──────────────────────────────────────────────────────────────

DEFAULT_FEEDS = [
    {"name": "Jump Crypto", "url": "https://jumpcrypto.com/feed/"},
    {"name": "Jane Street", "url": "https://blog.janestreet.com/feed.xml"},
    {"name": "Glassnode", "url": "https://glassnode.com/blog/feed"},
]

USER_AGENT = (
    "Mozilla/5.0 (compatible; BinanceSquareAgent/1.0; "
    "+https://github.com/nous-research/binance-square-agent)"
)

MAX_RETRIES = 2


# ── Helpers ─────────────────────────────────────────────────────────────────


def _feed_content_summary(entry) -> str:
    """Extract plain-text content from a feed entry (summary->content)."""
    content = ""
    if hasattr(entry, "content") and entry.content:
        content = " ".join(c.get("value", "") for c in entry.content)
    if not content and hasattr(entry, "summary") and entry.summary:
        content = entry.summary
    if not content and hasattr(entry, "description") and entry.description:
        content = entry.description
    return content.strip() if content else ""


def _feed_published(entry) -> str:
    """Extract published/updated date from entry."""
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            dt = datetime(*entry.published_parsed[:6])
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            pass
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        try:
            dt = datetime(*entry.updated_parsed[:6])
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            pass
    if hasattr(entry, "published") and entry.published:
        return entry.published
    return datetime.now(timezone.utc).isoformat()


# ── Harvester ──────────────────────────────────────────────────────────────


class BlogFeedHarvester:
    """
    Polls RSS/Atom feeds from institutional crypto/quant blogs.

    Args:
        max_items: Max entries to fetch total across all feeds (default 10).
        timeout: HTTP request timeout in seconds (default 30).
        feeds: List of feed configs: [{name: str, url: str}, ...]
    """

    def __init__(
        self,
        max_items: int = 10,
        timeout: int = 30,
        feeds: Optional[list[dict]] = None,
    ):
        self.max_items = max_items
        self.timeout = timeout
        self.feeds = feeds or DEFAULT_FEEDS

    def harvest(self) -> list[dict]:
        """
        Poll all configured feeds and return latest entries.

        Returns list of dicts:
            {source, title, content, url, published, source_type, harvested_at}
        """
        results: list[dict] = []

        for feed_cfg in self.feeds:
            if len(results) >= self.max_items:
                break

            feed_name = feed_cfg["name"]
            feed_url = feed_cfg["url"]

            # Fetch raw XML via httpx
            raw_xml = None
            for attempt in range(MAX_RETRIES):
                try:
                    with httpx.Client(timeout=self.timeout) as client:
                        resp = client.get(
                            feed_url,
                            headers={"User-Agent": USER_AGENT},
                            follow_redirects=True,
                        )
                        resp.raise_for_status()
                        raw_xml = resp.text
                        break
                except (httpx.HTTPError, httpx.TimeoutException) as e:
                    if attempt == MAX_RETRIES - 1:
                        break
                    continue

            if not raw_xml:
                continue

            # Parse with feedparser
            feed = feedparser.parse(raw_xml)

            if feed.bozo and not feed.entries:
                continue

            per_feed_max = self.max_items - len(results)
            for entry in feed.entries[:per_feed_max]:
                title = entry.get("title", "").strip() if hasattr(entry, "title") else ""
                link = entry.get("link", "").strip() if hasattr(entry, "link") else ""
                content = _feed_content_summary(entry)
                published = _feed_published(entry)

                results.append(
                    {
                        "source": feed_name,
                        "title": title,
                        "content": content,
                        "url": link,
                        "published": published,
                        "source_type": "rss",
                        "harvested_at": datetime.now(timezone.utc).isoformat(),
                    }
                )

        return results[: self.max_items]
