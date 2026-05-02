# sources/__init__.py — 采集器注册

from .github_quant import GitHubQuantHarvester
from .arxiv_feed import ArxivPaperHarvester
from .blog_feed import BlogFeedHarvester

__all__ = [
    "GitHubQuantHarvester",
    "ArxivPaperHarvester",
    "BlogFeedHarvester",
]
