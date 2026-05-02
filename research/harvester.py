"""
harvester.py — 数据采集调度器

Aggregates all source harvesters (GitHub, arXiv, blog) and returns a unified
list of raw strategy / paper / blog entries for downstream processing.
"""

import logging
import sys
from datetime import datetime, timezone
from typing import Optional

try:
    from .sources import GitHubQuantHarvester, ArxivPaperHarvester, BlogFeedHarvester
except ImportError:
    from sources import GitHubQuantHarvester, ArxivPaperHarvester, BlogFeedHarvester

logger = logging.getLogger(__name__)


# ── Unified Harvester ──────────────────────────────────────────────────────


class UnifiedHarvester:
    """
    Orchestrates data collection from all configured sources.

    Args:
        max_items_per_source: Max items to fetch from each source type (default 10).
        timeout: HTTP request timeout in seconds (default 30).
        github_repos: Optional list of GitHub repo configs (overrides defaults).
        arxiv_queries: Optional list of arXiv search queries (overrides defaults).
        blog_feeds: Optional list of blog feed configs (overrides defaults).
    """

    def __init__(
        self,
        max_items_per_source: int = 10,
        timeout: int = 30,
        github_repos: Optional[list[dict]] = None,
        arxiv_queries: Optional[list[str]] = None,
        blog_feeds: Optional[list[dict]] = None,
    ):
        self.max_items_per_source = max_items_per_source
        self.timeout = timeout

        self.github_harvester = GitHubQuantHarvester(
            max_items=max_items_per_source,
            timeout=timeout,
        )
        self.arxiv_harvester = ArxivPaperHarvester(
            max_items=max_items_per_source,
            timeout=timeout,
            queries=arxiv_queries,
        )
        self.blog_harvester = BlogFeedHarvester(
            max_items=max_items_per_source,
            timeout=timeout,
            feeds=blog_feeds,
        )

    def harvest_all(self) -> dict[str, list[dict]]:
        """
        Run all harvesters and return categorized results.

        Returns:
            {
                "github": [...],
                "arxiv": [...],
                "blog": [...],
                "total_count": int,
                "harvested_at": "ISO-8601"
            }
        """
        total_count = 0
        results: dict[str, list[dict]] = {}

        for source_name, harvester in [
            ("github", self.github_harvester),
            ("arxiv", self.arxiv_harvester),
            ("blog", self.blog_harvester),
        ]:
            try:
                items = harvester.harvest()
                results[source_name] = items
                total_count += len(items)
                logger.info(
                    "Harvested %d items from %s", len(items), source_name
                )
            except Exception as e:
                logger.error("Harvester %s failed: %s", source_name, e)
                results[source_name] = []

        results["total_count"] = total_count
        results["harvested_at"] = datetime.now(timezone.utc).isoformat()
        return results

    def harvest_flat(self) -> list[dict]:
        """
        Run all harvesters and return a flat unified list.

        Each item gets a 'category' field added: 'github' | 'arxiv' | 'blog'.
        """
        categorized = self.harvest_all()
        flat: list[dict] = []

        for category in ("github", "arxiv", "blog"):
            for item in categorized.get(category, []):
                item["category"] = category
                flat.append(item)

        return flat


# ── Standalone Usage ───────────────────────────────────────────────────────


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Quick check: skip GitHub if we detect rate-limit exhaustion
    import httpx
    gh_repos = None
    try:
        rl_resp = httpx.get(
            "https://api.github.com/rate_limit",
            headers={"Accept": "application/vnd.github.v3+json"},
            timeout=5,
        )
        rl_data = rl_resp.json()
        gh_remaining = rl_data["resources"]["core"]["remaining"]
        if gh_remaining < 5:
            logger.warning("GitHub API rate limit low (%d remaining), skipping GitHub", gh_remaining)
            gh_repos = []
        else:
            gh_repos = None
    except Exception:
        pass

    harvester = UnifiedHarvester(
        max_items_per_source=5,
        timeout=20,
        github_repos=gh_repos,
    )
    results = harvester.harvest_all()

    print(f"\n=== Harvest Complete ===")
    print(f"Harvested at: {results['harvested_at']}")
    print(f"Total items:  {results['total_count']}")
    for source in ("github", "arxiv", "blog"):
        items = results.get(source, [])
        print(f"\n--- {source.upper()} ({len(items)} items) ---")
        for item in items[:3]:
            title_or_path = (
                item.get("title")
                or item.get("file_path")
                or item.get("source_name", "?")
            )
            print(f"  • {title_or_path}")
