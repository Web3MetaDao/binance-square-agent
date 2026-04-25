"""
币安广场运营系统智能体 - 核心配置
====================================
所有可调参数集中在此文件，部署时只需修改此处。
"""
import os
import pathlib

# ══════════════════════════════════════════════
# 自动加载 .env 文件（支持 python-dotenv，同时兼容无 dotenv 环境）
# ══════════════════════════════════════════════
def _load_env_file():
    """优先使用 python-dotenv，如果未安装则手动解析 .env 文件。"""
    env_path = pathlib.Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(env_path, override=False)  # 不覆盖已有环境变量
    except ImportError:
        # 手动解析 .env（当 python-dotenv 未安装时的兑底方案）
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:  # 不覆盖已有环境变量
                    os.environ[key] = val

_load_env_file()

# ══════════════════════════════════════════════
# 密鑰配置（通过环境变量注入，勿硬编码）
# ══════════════════════════════════════════════
SQUARE_API_KEY  = os.environ.get("SQUARE_API_KEY", "")
OPENAI_API_KEY  = os.environ.get("OPENAI_API_KEY", "")

# ══════════════════════════════════════════════
# OpenAI 第三方中转站配置
# 将 OPENAI_BASE_URL 设置为中转站地址，例如：
#   export OPENAI_BASE_URL="https://your-proxy.example.com/v1"
# 若不设置则使用官方默认地址
# ══════════════════════════════════════════════
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")

# ══════════════════════════════════════════════
# 币安广场内容挖矿（Write to Earn）配置
# 规则：发帖含 cashtag（如 $BTC）→ 读者点击后交易 → 获得手续费返佣
# 返佣比例：普通创作者 20%，周榜 Top30 最高 50%
# 返佣以 USDC 发放，每周四结算，最低 0.1 USDC 起付
# 帖子有效期：发布后 7 天内带来的交易才计入返佣
# ══════════════════════════════════════════════
WRITE_TO_EARN_URL = "https://www.binance.com/zh-CN/square/write-to-earn"
WRITE_TO_EARN_GUIDE = "https://www.binance.com/zh-CN/academy/articles/write-to-earn-on-binance-square-all-you-need-to-know"

# ══════════════════════════════════════════════
# 执行层参数
# ══════════════════════════════════════════════
DAILY_LIMIT       = 72     # 每日最大发帖数（W2E 48篇 + 热点 24篇）
MIN_INTERVAL_MIN  = 14     # 两贴之间最短间隔（分钟）
COIN_COOLDOWN_H   = 4      # 同一币种最短间隔（小时）
SCAN_INTERVAL_M   = 30     # 感知层扫描间隔（分钟）
MAX_JITTER_MIN    = 3      # 发帖间隔随机抖动上限（分钟）

# ══════════════════════════════════════════════
# LLM 配置
# ══════════════════════════════════════════════
LLM_MODEL         = os.environ.get("LLM_MODEL", "gpt-5.5")
LLM_TEMPERATURE   = 0.85
LLM_MAX_TOKENS    = 500
POST_MIN_CHARS    = 80     # 短贴最小字数
POST_MAX_CHARS    = 200    # 短贴最大字数

# ══════════════════════════════════════════════
# Twitter KOL 列表（可动态扩展）
# ══════════════════════════════════════════════
KOL_LIST = [
    {"username": "CryptoKaleo",    "rest_id": "906234475604037637",    "weight": 3},
    {"username": "RaoulGMI",       "rest_id": "2453385626",            "weight": 3},
    {"username": "cz_binance",     "rest_id": "902926941413453824",    "weight": 5},
    {"username": "VitalikButerin", "rest_id": "295218901",             "weight": 4},
    {"username": "inversebrah",    "rest_id": "1051852534518824960",   "weight": 2},
    {"username": "PeterLBrandt",   "rest_id": "247857712",             "weight": 3},
    {"username": "CryptoHayes",    "rest_id": "983993370048630785",    "weight": 3},
]

# ══════════════════════════════════════════════
# 代币 → 期货合约映射表
# ══════════════════════════════════════════════
FUTURES_MAP = {
    "BTC": "BTCUSDT",   "Bitcoin": "BTCUSDT",
    "ETH": "ETHUSDT",   "Ethereum": "ETHUSDT",
    "BNB": "BNBUSDT",
    "SOL": "SOLUSDT",   "Solana": "SOLUSDT",
    "XRP": "XRPUSDT",
    "DOGE": "DOGEUSDT",
    "PEPE": "PEPEUSDT",
    "ARB": "ARBUSDT",
    "OP": "OPUSDT",
    "SUI": "SUIUSDT",
    "AVAX": "AVAXUSDT",
    "LINK": "LINKUSDT",
    "DOT": "DOTUSDT",
    "ADA": "ADAUSDT",
    "MATIC": "MATICUSDT",
    "LTC": "LTCUSDT",
    "ATOM": "ATOMUSDT",
    "NEAR": "NEARUSDT",
    "APT": "APTUSDT",
    "INJ": "INJUSDT",
    "TIA": "TIAUSDT",
    "WIF": "WIFUSDT",
    "BONK": "BONKUSDT",
    "TRUMP": "TRUMPUSDT",
    "MEME": "MEMEUSDT",
    "TON": "TONUSDT",
    "NOT": "NOTUSDT",
    "JUP": "JUPUSDT",
    "W": "WUSDT",
}

# ══════════════════════════════════════════════
# 路径配置
# ══════════════════════════════════════════════
import pathlib
BASE_DIR     = pathlib.Path(__file__).parent.parent
DATA_DIR     = BASE_DIR / "data"
LOG_DIR      = BASE_DIR / "logs"
PERSONA_FILE = DATA_DIR / "persona.md"
STATE_FILE   = DATA_DIR / "agent_state.json"
POST_LOG     = LOG_DIR  / "post_log.jsonl"
MARKET_FILE  = DATA_DIR / "market_context.json"
