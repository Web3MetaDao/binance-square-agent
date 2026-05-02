"""
核心状态管理模块
================
负责智能体运行状态的读取、写入与跨天重置。
"""
import json
import os
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import fcntl

from config.settings import STATE_FILE, DATA_DIR, LOG_DIR, POST_LOG, DAILY_LIMIT

# 确保目录存在
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

LIVE_SCRIPT_FILE = DATA_DIR / "live_script.json"
LIVE_MARKET_REPORT_FILE = DATA_DIR / "live_market_report.json"
RUNTIME_ARTIFACT_FRESHNESS_SECONDS = 15 * 60

_STATE_LOCKS: dict[str, threading.RLock] = {}
_STATE_LOCKS_GUARD = threading.Lock()


def _default_state() -> dict:
    return {
        "status": "idle",
        "today": "",
        "daily_count": 0,
        "last_post_time": 0,
        "coin_last_post": {},
        "coin_last_post_date": {},
        "guest_token": None,
        "guest_token_time": 0,
        "total_posts": 0,
        "started_at": "",
        "posting_intent": None,
        "posting_intent_cleared_at": 0,
        "recent_post_keys": {},
    }


def _path(value) -> Path:
    return value if isinstance(value, Path) else Path(value)


@contextmanager
def _thread_lock(path: Path):
    path_key = str(path)
    with _STATE_LOCKS_GUARD:
        lock = _STATE_LOCKS.setdefault(path_key, threading.RLock())
    with lock:
        yield


@contextmanager
def _file_lock(path: Path):
    lock_path = path.with_name(path.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def _locked_path(path):
    target = _path(path)
    with _thread_lock(target):
        with _file_lock(target):
            yield target


def _merge_recent_post_keys(latest_recent: dict, incoming_recent: dict) -> dict:
    merged_recent = dict(latest_recent or {})
    for key, incoming_entry in dict(incoming_recent or {}).items():
        latest_entry = merged_recent.get(key)
        if not isinstance(incoming_entry, dict):
            if latest_entry is None:
                merged_recent[key] = incoming_entry
            continue
        if not isinstance(latest_entry, dict):
            merged_recent[key] = incoming_entry
            continue

        latest_created_at = float(latest_entry.get("created_at") or 0)
        incoming_created_at = float(incoming_entry.get("created_at") or 0)
        if incoming_created_at >= latest_created_at:
            merged_recent[key] = {**latest_entry, **incoming_entry}
        else:
            merged_recent[key] = {**incoming_entry, **latest_entry}
    return merged_recent


def _merge_state_for_save(latest: dict, incoming: dict) -> dict:
    merged = {**latest, **incoming}

    latest_status = str(latest.get("status", "")).upper()
    incoming_status = str(incoming.get("status", "")).upper()
    if latest_status == "BANNED" and incoming_status != "BANNED":
        merged["status"] = latest.get("status")

    latest_today = latest.get("today")
    incoming_today = incoming.get("today")
    is_initial_write = not latest_today
    same_day = incoming_today == latest_today

    if is_initial_write or same_day:
        merged["today"] = incoming_today or latest_today
        merged["daily_count"] = max(latest.get("daily_count", 0), incoming.get("daily_count", 0))
        merged["coin_last_post_date"] = {
            **dict(latest.get("coin_last_post_date", {})),
            **dict(incoming.get("coin_last_post_date", {})),
        }
    else:
        merged["today"] = latest_today
        merged["daily_count"] = latest.get("daily_count", 0)
        merged["coin_last_post_date"] = dict(latest.get("coin_last_post_date", {}))

    merged["total_posts"] = max(latest.get("total_posts", 0), incoming.get("total_posts", 0))
    merged["last_post_time"] = max(latest.get("last_post_time", 0), incoming.get("last_post_time", 0))

    latest_coin_last_post = dict(latest.get("coin_last_post", {}))
    for coin, ts in dict(incoming.get("coin_last_post", {})).items():
        latest_coin_last_post[coin] = max(latest_coin_last_post.get(coin, 0), ts)
    merged["coin_last_post"] = latest_coin_last_post

    latest_intent = latest.get("posting_intent")
    incoming_intent = incoming.get("posting_intent")
    latest_intent_cleared_at = float(latest.get("posting_intent_cleared_at") or 0)
    incoming_intent_cleared_at = float(incoming.get("posting_intent_cleared_at") or 0)
    merged["posting_intent_cleared_at"] = max(latest_intent_cleared_at, incoming_intent_cleared_at)
    if latest_intent:
        latest_intent_id = latest_intent.get("id")
        incoming_intent_id = incoming_intent.get("id") if incoming_intent else None
        if incoming_intent is None:
            merged["posting_intent"] = latest_intent
        elif latest_intent_id and incoming_intent_id != latest_intent_id:
            merged["posting_intent"] = latest_intent
    elif incoming_intent and merged["posting_intent_cleared_at"]:
        incoming_created_at = float(incoming_intent.get("created_at") or 0)
        if incoming_created_at <= merged["posting_intent_cleared_at"]:
            merged["posting_intent"] = None

    merged["recent_post_keys"] = _merge_recent_post_keys(
        latest.get("recent_post_keys", {}),
        incoming.get("recent_post_keys", {}),
    )

    return merged


def _load_state_unlocked(state_path: Path) -> tuple[dict, bool]:
    default = _default_state()
    if not state_path.exists():
        return default, False

    with open(state_path, encoding="utf-8") as f:
        state = json.load(f)

    merged = {**default, **state}
    today = datetime.now().strftime("%Y-%m-%d")
    needs_persist = False
    if merged.get("today") != today:
        merged["today"] = today
        merged["daily_count"] = 0
        merged["coin_last_post_date"] = {}
        needs_persist = True

    return merged, needs_persist


def _save_state_unlocked(state_path: Path, state: dict):
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_file = state_path.with_name(
        f"{state_path.name}.{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex}.tmp"
    )
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_file, state_path)
    dir_fd = os.open(str(state_path.parent), os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def load_state() -> dict:
    """加载智能体状态，跨天自动重置每日计数器。"""
    with _locked_path(STATE_FILE) as state_path:
        state, needs_persist = _load_state_unlocked(state_path)
        if needs_persist:
            _save_state_unlocked(state_path, state)
        return state


def save_state(state: dict):
    """持久化保存智能体状态。对安全关键字段执行单调合并，避免陈旧快照覆盖磁盘最新状态。"""
    with _locked_path(STATE_FILE) as state_path:
        latest, _ = _load_state_unlocked(state_path)
        merged = _merge_state_for_save(latest, state)
        _save_state_unlocked(state_path, merged)
        return merged


def update_state(mutator):
    """在路径级锁内基于磁盘最新状态执行读改写，避免陈旧内存快照覆盖新字段。"""
    with _locked_path(STATE_FILE) as state_path:
        current, _ = _load_state_unlocked(state_path)
        candidate = mutator(current)
        state = current if candidate is None else candidate
        _save_state_unlocked(state_path, state)
        return state


def log_post(entry: dict):
    """追加写入发帖日志（JSONL格式）。"""
    with _locked_path(POST_LOG) as post_log:
        post_log.parent.mkdir(parents=True, exist_ok=True)
        log_entry = dict(entry)
        log_entry["logged_at"] = datetime.now().isoformat()
        with open(post_log, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())


def _read_json_file(path: Path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_iso_to_timestamp(value) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except Exception:
        return 0.0


def _artifact_timestamp(payload, fallback_mtime: float = 0.0) -> float:
    if not isinstance(payload, dict):
        return float(fallback_mtime or 0.0)

    candidates = [
        payload.get("generated_at"),
        (payload.get("overview") or {}).get("generated_at") if isinstance(payload.get("overview"), dict) else None,
        (payload.get("stats") or {}).get("generated_at") if isinstance(payload.get("stats"), dict) else None,
    ]
    for candidate in candidates:
        ts = _safe_iso_to_timestamp(candidate)
        if ts:
            return ts
    return float(fallback_mtime or 0.0)


def _artifact_health(path: Path) -> dict:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return {
            "path": str(path),
            "exists": False,
            "fresh": False,
            "timestamp": 0.0,
            "timestamp_text": "无",
            "age_seconds": None,
            "age_text": "无",
        }

    mtime = stat.st_mtime
    payload = _read_json_file(path)
    ts = _artifact_timestamp(payload, fallback_mtime=mtime)
    age_seconds = max(0.0, datetime.now().timestamp() - ts) if ts else None
    fresh = bool(age_seconds is not None and age_seconds <= RUNTIME_ARTIFACT_FRESHNESS_SECONDS)
    return {
        "path": str(path),
        "exists": True,
        "fresh": fresh,
        "timestamp": ts,
        "timestamp_text": datetime.fromtimestamp(ts).isoformat(timespec="seconds") if ts else "无",
        "age_seconds": age_seconds,
        "age_text": _format_elapsed(age_seconds),
    }


def _format_elapsed(seconds: float | None) -> str:
    if seconds is None:
        return "无"
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def get_status_payload(state: dict) -> dict:
    now = datetime.now().timestamp()
    status = str(state.get("status", "idle") or "idle")
    daily_count = _safe_int(state.get("daily_count", 0), 0)
    total_posts = _safe_int(state.get("total_posts", 0), 0)
    last_post_time = _safe_float(state.get("last_post_time", 0), 0.0)
    guest_token_time = _safe_float(state.get("guest_token_time", 0), 0.0)
    posting_intent = state.get("posting_intent") if isinstance(state.get("posting_intent"), dict) else None
    posting_intent_created_at = _safe_float((posting_intent or {}).get("created_at"), 0.0)
    remaining_today = max(0, DAILY_LIMIT - daily_count)
    last_post_age_seconds = max(0.0, now - last_post_time) if last_post_time else None
    guest_token_age_seconds = max(0.0, now - guest_token_time) if guest_token_time else None
    posting_intent_age_seconds = max(0.0, now - posting_intent_created_at) if posting_intent_created_at else None

    live_script = _artifact_health(LIVE_SCRIPT_FILE)
    live_market_report = _artifact_health(LIVE_MARKET_REPORT_FILE)

    return {
        "status": status,
        "today": state.get("today", ""),
        "daily_count": daily_count,
        "daily_limit": DAILY_LIMIT,
        "remaining_today": remaining_today,
        "total_posts": total_posts,
        "last_post_time": last_post_time,
        "last_post_time_text": datetime.fromtimestamp(last_post_time).strftime("%Y-%m-%d %H:%M:%S") if last_post_time else "无",
        "last_post_age_seconds": last_post_age_seconds,
        "last_post_age_text": _format_elapsed(last_post_age_seconds),
        "is_banned": status.upper() == "BANNED",
        "has_guest_token": bool(state.get("guest_token")),
        "guest_token_age_seconds": guest_token_age_seconds,
        "guest_token_age_text": _format_elapsed(guest_token_age_seconds),
        "has_posting_intent": bool(posting_intent),
        "posting_intent": posting_intent,
        "posting_intent_age_seconds": posting_intent_age_seconds,
        "posting_intent_age_text": _format_elapsed(posting_intent_age_seconds),
        "recent_post_key_count": len(state.get("recent_post_keys", {}) or {}),
        "coin_cooldown_count": len(state.get("coin_last_post", {}) or {}),
        "live_script": live_script,
        "live_market_report": live_market_report,
        "runtime_artifact_freshness_seconds": RUNTIME_ARTIFACT_FRESHNESS_SECONDS,
    }


def get_status_summary(state: dict) -> str:
    """返回人类可读的状态摘要。"""
    payload = get_status_payload(state)
    return (
        f"状态: {payload['status']} | 今日: {payload['daily_count']}/{payload['daily_limit']} "
        f"| 剩余: {payload['remaining_today']} | 累计: {payload['total_posts']} "
        f"| 上次发帖: {payload['last_post_time_text']}"
    )
