"""配置与日志模块。

从环境变量读取关键配置，提供全局日志器。
自动加载同级 .env 文件（如果存在）。
"""
import logging
import os
from pathlib import Path

# ── 自动加载 .env ──────────────────────────────────────
_env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                _k = _k.strip()
                _v = _v.strip().strip("'\"")
                if _k not in os.environ:
                    os.environ[_k] = _v

# === 基础路径 ===
DATA_DIR = os.getenv("DATA_DIR", "/root/binance-square-agent/data")

# === Telegram ===
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

# === API ===
SQUARE_API_KEY = os.getenv("SQUARE_API_KEY", "")

# === 数据库 ===
DB_PATH = os.path.join(DATA_DIR, "radar.db")

# === 标的参数 ===
POOL_MIN_VOL = 100_000       # 最小日均成交额阈值
WATCHLIST_MAX = 100          # 最多跟踪标的数量

# ── 日志 ──────────────────────────────────────────────
os.makedirs(DATA_DIR, exist_ok=True)

_log_file = os.path.join(DATA_DIR, "radar.log")
_fh = logging.FileHandler(_log_file, encoding="utf-8")
_sh = logging.StreamHandler()

_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
_fh.setFormatter(_fmt)
_sh.setFormatter(_fmt)

_logger = logging.getLogger("accumulation_radar")
_logger.setLevel(logging.INFO)
_logger.addHandler(_fh)
_logger.addHandler(_sh)

logger = _logger
COINALYZE_API_KEY = os.getenv("COINALYZE_API_KEY", "")
