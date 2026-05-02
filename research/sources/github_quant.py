"""
github_quant.py — GitHubQuantHarvester

Fetches strategy files from popular quantitative finance GitHub repos.
Uses raw.githubusercontent.com URLs — no API key needed, no rate limits.
"""

import json
import logging
import os
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────

RAW_BASE = "https://raw.githubusercontent.com"

# Predefined well-known quant strategy files (no API needed — direct raw URLs)
KNOWN_STRATEGY_FILES = [
    # freqtrade strategies (popular open-source)
    "https://raw.githubusercontent.com/freqtrade/freqtrade-strategies/main/user_data/strategies/CombinedBinHAndCluc.py",
    "https://raw.githubusercontent.com/freqtrade/freqtrade-strategies/main/user_data/strategies/NostalgiaForInfinity.py",
    "https://raw.githubusercontent.com/freqtrade/freqtrade-strategies/main/user_data/strategies/SMAOffsetProtectorOpt.py",
    "https://raw.githubusercontent.com/freqtrade/freqtrade-strategies/main/user_data/strategies/BinHp105.py",
    "https://raw.githubusercontent.com/freqtrade/freqtrade-strategies/main/user_data/strategies/ElliotV5.py",
    "https://raw.githubusercontent.com/freqtrade/freqtrade-strategies/main/user_data/strategies/Strategy003.py",
    # awesome-quant curated list (markdown)
    "https://raw.githubusercontent.com/wilsonfreitas/awesome-quant/main/README.md",
    # vectorbt strategies
    "https://raw.githubusercontent.com/polakowo/vectorbt-strategies/main/tutorials/Portfolio_Optimization.ipynb",
    # Additional popular strategies from various repos
    "https://raw.githubusercontent.com/mentat786/awesome-quant/main/README.md",
    "https://raw.githubusercontent.com/jesse-ai/jesse/master/jesse/indicators/acceleration.py",
    "https://raw.githubusercontent.com/jesse-ai/jesse/master/jesse/indicators/channel.py",
    "https://raw.githubusercontent.com/jesse-ai/jesse/master/jesse/indicators/ichimoku.py",
    "https://raw.githubusercontent.com/jesse-ai/jesse/master/jesse/indicators/supertrend.py",
]


class GitHubQuantHarvester:
    """
    Harvests strategy files from known GitHub repositories via raw URLs.

    Uses raw.githubusercontent.com URLs — no API rate limits.

    Args:
        max_items: Max files to download per harvest cycle (default 10).
        timeout: HTTP request timeout in seconds (default 15).
    """

    def __init__(
        self,
        max_items: int = 10,
        timeout: int = 15,
    ):
        self.max_items = max_items
        self.timeout = timeout

    def harvest(self) -> list[dict]:
        """
        Fetch strategy files from known GitHub URLs via raw content.

        Returns list of dicts:
            {title, content, description, source_name, source_type, url}
        """
        results: list[dict] = []

        for url in KNOWN_STRATEGY_FILES[:self.max_items * 3]:  # fetch extra to account for failures
            try:
                resp = httpx.get(url, timeout=self.timeout, follow_redirects=True)
                if resp.status_code != 200:
                    continue
                content = resp.text
                fname = url.rsplit("/", 1)[-1]
                repo_part = url.replace("https://raw.githubusercontent.com/", "").split("/", 1)[0]
                results.append({
                    "source_name": f"github/{repo_part}",
                    "file_path": fname,
                    "raw_content": content,
                    "content": content,
                    "description": f"GitHub strategy: {repo_part} - {fname}",
                    "title": fname,
                    "source_type": "github",
                    "url": url,
                    "harvested_at": datetime.now(timezone.utc).isoformat(),
                })
                if len(results) >= self.max_items:
                    break
            except Exception as e:
                logger.warning(f"  Failed to fetch {url}: {e}")

        return results
