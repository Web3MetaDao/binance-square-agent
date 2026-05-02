"""
arxiv_feed.py — ArxivPaperHarvester

Searches arXiv for recent papers on crypto trading, quantitative finance,
market microstructure, reinforcement learning trading, and volatility modeling.
"""

import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Optional

import httpx

# ── Constants ──────────────────────────────────────────────────────────────

ARXIV_API_URL = "http://export.arxiv.org/api/query"

DEFAULT_QUERIES = [
    "crypto trading",
    "quantitative finance machine learning",
    "market microstructure deep learning",
    "reinforcement learning trading",
    "LSTM volatility cryptocurrency",
]

ARXIV_NAMESPACES = {
    "atom": "http://www.w3.org/2005/Atom",
    "arxiv": "http://arxiv.org/schemas/atom",
}


def _query_arxiv(
    client: httpx.Client,
    search_query: str,
    max_results: int,
    timeout: int,
) -> list[dict]:
    """Run a single arXiv query and parse results."""
    params = {
        "search_query": f"all:\"{search_query}\"",
        "start": 0,
        "max_results": max_results,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }

    try:
        resp = client.get(ARXIV_API_URL, params=params, timeout=timeout, follow_redirects=True)
        resp.raise_for_status()
    except (httpx.HTTPError, httpx.TimeoutException) as e:
        return []

    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError:
        return []

    entries = root.findall("atom:entry", ARXIV_NAMESPACES)
    papers = []

    for entry in entries:
        title_el = entry.find("atom:title", ARXIV_NAMESPACES)
        summary_el = entry.find("atom:summary", ARXIV_NAMESPACES)
        published_el = entry.find("atom:published", ARXIV_NAMESPACES)
        pdf_link = None
        authors = []

        for link in entry.findall("atom:link", ARXIV_NAMESPACES):
            title_attr = link.get("title", "")
            if title_attr == "pdf":
                pdf_link = link.get("href")
                break

        for author in entry.findall("atom:author", ARXIV_NAMESPACES):
            name_el = author.find("atom:name", ARXIV_NAMESPACES)
            if name_el is not None and name_el.text:
                authors.append(name_el.text.strip())

        title = (title_el.text or "").strip().replace("\n", " ").replace("  ", " ")
        summary = (summary_el.text or "").strip().replace("\n", " ")

        papers.append(
            {
                "title": title,
                "authors": authors,
                "summary": summary,
                "pdf_url": pdf_link or "",
                "published": (published_el.text or "").strip() if published_el is not None else "",
                "source_type": "arxiv",
                "harvested_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    return papers


# ── Harvester ──────────────────────────────────────────────────────────────


class ArxivPaperHarvester:
    """
    Harvests academic papers from arXiv on quantitative finance / crypto topics.

    Args:
        max_items: Max papers to fetch total across all queries (default 10).
        timeout: HTTP request timeout in seconds (default 30).
        queries: List of search queries to use.
        delay: Delay in seconds between API calls to be polite (default 3).
    """

    def __init__(
        self,
        max_items: int = 10,
        timeout: int = 30,
        queries: Optional[list[str]] = None,
        delay: float = 3.0,
    ):
        self.max_items = max_items
        self.timeout = timeout
        self.queries = queries or DEFAULT_QUERIES
        self.delay = delay

    def harvest(self) -> list[dict]:
        """
        Run all configured arXiv queries and return deduplicated results.

        Returns list of dicts: {title, authors, summary, pdf_url, published,
                                source_type, harvested_at}
        """
        seen_titles: set[str] = set()
        results: list[dict] = []
        per_query = max(1, self.max_items // len(self.queries))

        with httpx.Client(timeout=self.timeout) as client:
            for i, query in enumerate(self.queries):
                if len(results) >= self.max_items:
                    break

                if i > 0:
                    time.sleep(self.delay)

                papers = _query_arxiv(client, query, per_query, self.timeout)

                for paper in papers:
                    title_key = paper["title"].lower().strip()
                    if title_key in seen_titles:
                        continue
                    seen_titles.add(title_key)
                    results.append(paper)

                    if len(results) >= self.max_items:
                        break

        return results[: self.max_items]
