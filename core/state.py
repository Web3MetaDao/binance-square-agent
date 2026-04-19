"""
核心状态管理模块
================
负责智能体运行状态的读取、写入与跨天重置。
"""
import json
import time
from datetime import datetime
from config.settings import STATE_FILE, DATA_DIR, LOG_DIR, POST_LOG

# 确保目录存在
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


def load_state() -> dict:
    """加载智能体状态，跨天自动重置每日计数器。"""
    default = {
        "status": "idle",
        "today": "",
        "daily_count": 0,
        "last_post_time": 0,
        "coin_last_post": {},
        "guest_token": None,
        "guest_token_time": 0,
        "total_posts": 0,
        "started_at": "",
    }
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            s = json.load(f)
        today = datetime.now().strftime("%Y-%m-%d")
        if s.get("today") != today:
            s["today"] = today
            s["daily_count"] = 0
        return {**default, **s}
    return default


def save_state(state: dict):
    """持久化保存智能体状态。"""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def log_post(entry: dict):
    """追加写入发帖日志（JSONL格式）。"""
    entry["logged_at"] = datetime.now().isoformat()
    with open(POST_LOG, "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def get_status_summary(state: dict) -> str:
    """返回人类可读的状态摘要。"""
    today = state.get("daily_count", 0)
    total = state.get("total_posts", 0)
    last = state.get("last_post_time", 0)
    last_str = datetime.fromtimestamp(last).strftime("%H:%M:%S") if last else "无"
    status = state.get("status", "idle")
    return (
        f"状态: {status} | 今日: {today}/100 | 累计: {total} | 上次发帖: {last_str}"
    )
