"""
币安广场运营系统智能体 - 核心配置
====================================
所有可调参数集中在此文件，部署时只需修改此处。
"""
import os

# ══════════════════════════════════════════════
# 密钥配置（通过环境变量注入，勿硬编码）
# ══════════════════════════════════════════════
SQUARE_API_KEY  = os.environ.get("SQUARE_API_KEY", "")
OPENAI_API_KEY  = os.environ.get("OPENAI_API_KEY", "")
REFERRAL_CODE   = os.environ.get("REFERRAL_CODE", "YOUR_REF_CODE")
REFERRAL_LINK   = os.environ.get("REFERRAL_LINK",
    f"https://www.binance.com/zh-CN/join?ref={os.environ.get('REFERRAL_CODE','YOUR_REF_CODE')}")

# ══════════════════════════════════════════════
# 执行层参数
# ══════════════════════════════════════════════
DAILY_LIMIT       = 100    # 每日最大发帖数
MIN_INTERVAL_MIN  = 14     # 两贴之间最短间隔（分钟）
COIN_COOLDOWN_H   = 4      # 同一币种最短间隔（小时）
SCAN_INTERVAL_M   = 30     # 感知层扫描间隔（分钟）
MAX_JITTER_MIN    = 3      # 发帖间隔随机抖动上限（分钟）

# ══════════════════════════════════════════════
# LLM 配置
# ══════════════════════════════════════════════
LLM_MODEL         = "gpt-4.1-mini"
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
