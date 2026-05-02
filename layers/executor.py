"""
执行层（Executor Layer）
=========================
负责发帖决策、配额管理、冷却控制与实际发帖执行。

四重防封控机制：
  1. 每日 100 贴硬上限
  2. 全局发帖间隔 ≥ 14 分钟（含随机抖动）
  3. 同币种 4 小时冷却
  4. 异常熔断（连续失败自动暂停）

错误码处理：
  000000 → 成功
  20013  → 内容长度超限
  20020  → 内容为空/不支持空内容
  20022  → 内容含敏感词
  220003 → API Key 无效
  220009 → OpenAPI 每日发帖上限
  220010 → 不支持的内容类型
  220011 → 内容为空
  2000001→ 账号被封禁（触发熔断）
"""

import time
import random
import threading
import hashlib
import uuid
from contextlib import contextmanager
import requests
from datetime import datetime
import fcntl

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    SQUARE_API_KEY, DAILY_LIMIT, MIN_INTERVAL_MIN,
    COIN_COOLDOWN_H, MAX_JITTER_MIN, DATA_DIR,
)
from core.state import save_state, log_post, update_state

POST_URL = (
    "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"
)

_POST_LOCKS: dict[str, threading.Lock] = {}
_POST_LOCKS_GUARD = threading.Lock()
_POST_FLOW_LOCK = threading.Lock()
_POST_FLOW_LOCK_FILE = DATA_DIR / "post_flow.lock"


def _normalize_coin_key(coin: str) -> str:
    coin_key = (coin or "").upper().strip()
    if coin_key.endswith("USDT"):
        coin_key = coin_key[:-4]
    return coin_key or "__EMPTY__"


@contextmanager
def _global_post_flow_file_lock():
    _POST_FLOW_LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(_POST_FLOW_LOCK_FILE, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def _locked_coin(coin: str):
    coin_key = _normalize_coin_key(coin)
    with _POST_LOCKS_GUARD:
        lock = _POST_LOCKS.setdefault(coin_key, threading.Lock())
    with lock:
        yield


@contextmanager
def _posting_transaction(coin: str):
    with _POST_FLOW_LOCK:
        with _global_post_flow_file_lock():
            with _locked_coin(coin):
                yield


def _content_hash(content: str) -> str:
    return f"sha256:{hashlib.sha256((content or '').encode('utf-8')).hexdigest()}"


def _prune_recent_post_keys(recent: dict, keep: int = 100) -> dict:
    normalized = {}
    for key, value in dict(recent or {}).items():
        if isinstance(value, dict):
            normalized[key] = value
        else:
            normalized[key] = {"created_at": 0}
    items = sorted(
        normalized.items(),
        key=lambda item: item[1].get("created_at", 0),
        reverse=True,
    )
    return dict(items[:keep])


def _build_posting_intent(*, coin: str, content: str, source: str, tier: str = "", mock: bool = False) -> dict:
    return {
        "id": uuid.uuid4().hex,
        "coin": (coin or "").upper().strip(),
        "content_hash": _content_hash(content),
        "content_preview": (content or "")[:100],
        "source": source,
        "tier": tier,
        "created_at": time.time(),
        "status": "IN_FLIGHT",
        "post_id": "",
        "url": "",
        "result_code": "",
        "mock": bool(mock),
    }


def _refresh_state_binding(state: dict, quota=None) -> dict:
    latest = update_state(lambda current: current)
    if latest is not state:
        state.clear()
        state.update(latest)
    if quota is not None and getattr(quota, "state", None) is not state:
        quota.state = state
    return state


def _reserve_post_intent(*, state: dict, content: str, coin: str, source: str, tier: str = "", mock: bool = False):
    candidate = _build_posting_intent(coin=coin, content=content, source=source, tier=tier, mock=mock)

    def _mutate(current: dict):
        recent = dict(current.get("recent_post_keys", {}))
        recent_entry = recent.get(candidate["content_hash"])
        if not isinstance(recent_entry, dict):
            recent_entry = None
        if recent_entry and recent_entry.get("coin") == candidate["coin"]:
            current["__phase4_blocked_reason"] = f"recent_duplicate:{candidate['coin']}"
            return current

        live = current.get("posting_intent")
        if live:
            live_coin = (live.get("coin") or "").upper().strip()
            if live.get("content_hash") == candidate["content_hash"] and live_coin == candidate["coin"]:
                current["__phase4_blocked_reason"] = f"pending_duplicate:{candidate['coin']}"
                return current
            current["__phase4_blocked_reason"] = f"pending_intent_in_flight:{live_coin or '__UNKNOWN__'}"
            return current

        current["posting_intent"] = candidate
        current.pop("__phase4_blocked_reason", None)
        return current

    latest = update_state(_mutate)
    blocked_reason = latest.pop("__phase4_blocked_reason", None)
    if blocked_reason is not None:
        update_state(lambda current: {k: v for k, v in current.items() if k != "__phase4_blocked_reason"})
        _refresh_state_binding(state)
        return None, blocked_reason

    _refresh_state_binding(state)
    return dict(state.get("posting_intent") or candidate), None


def _clear_posting_intent(intent_id: str):
    cleared_at = time.time()

    def _mutate(current: dict):
        live = current.get("posting_intent")
        if live and live.get("id") == intent_id:
            current["posting_intent"] = None
            current["posting_intent_cleared_at"] = max(
                float(current.get("posting_intent_cleared_at") or 0),
                cleared_at,
            )
        return current

    return update_state(_mutate)


def _finalize_post_success(*, intent: dict, result: dict, content: str = ""):
    finalized_at = time.time()

    def _mutate(current: dict):
        recent = dict(current.get("recent_post_keys", {}))
        recent[intent["content_hash"]] = {
            "coin": intent.get("coin", ""),
            "created_at": finalized_at,
            "post_id": result.get("post_id", ""),
            "url": result.get("url", ""),
            "source": intent.get("source", ""),
            "tier": intent.get("tier", ""),
            "mock": result.get("mock", False),
        }
        current["recent_post_keys"] = _prune_recent_post_keys(recent)

        # ── 持久化帖子正文到 post_history（用于后续去重引用） ──
        if content:
            history = list(current.get("post_history", []))
            history.append({
                "content_hash": intent["content_hash"],
                "coin": intent.get("coin", ""),
                "content": content,
                "title": (content or "").split("\n")[0][:80],
                "created_at": finalized_at,
                "source": intent.get("source", ""),
                "tier": intent.get("tier", ""),
            })
            # 保留最近 200 条（含去重哈希过滤）
            seen = set()
            deduped = []
            for entry in reversed(history):
                h = entry.get("content_hash", "")
                if h and h in seen:
                    continue
                if h:
                    seen.add(h)
                deduped.append(entry)
            deduped.reverse()
            current["post_history"] = deduped[-200:]

        live = current.get("posting_intent")
        if live and live.get("id") == intent.get("id"):
            current["posting_intent"] = None
            current["posting_intent_cleared_at"] = max(
                float(current.get("posting_intent_cleared_at") or 0),
                finalized_at,
            )
        return current

    return update_state(_mutate)


def _is_ambiguous_post_failure(result: dict) -> bool:
    return result.get("code") == "NETWORK_ERROR"


# 错误码说明
ERROR_CODES = {
    "20013":  "内容长度超限",
    "20020":  "内容为空或内容类型不支持",
    "220003":  "API Key 无效或未找到",
    "220004":  "API Key 已过期",
    "220009":  "OpenAPI 今日发帖已达每日发帖上限",
    "220010":  "OpenAPI 不支持的内容类型",
    "220011":  "内容为空",
    "20022":   "内容含敏感词，已被风控拦截",
    "2000001": "账号已被封禁（触发熔断）",
    "429":     "请求频率过高，触发限流",
}


# ──────────────────────────────────────────────
# 配额控制器
# ──────────────────────────────────────────────
class QuotaController:
    """
    检查当前是否满足发帖条件，并在发帖后更新状态。
    """

    def __init__(self, state: dict):
        self.state = state

    def _normalize_coin(self, coin: str) -> str:
        coin = (coin or "").upper().strip()
        if coin.endswith("USDT"):
            coin = coin[:-4]
        return coin

    def can_post(self, coin: str) -> tuple:
        """
        返回 (bool, str)：是否可以发帖，以及拒绝原因。
        """
        now = time.time()
        coin = self._normalize_coin(coin)

        # 0. 账号状态熔断
        if str(self.state.get("status", "")).upper() == "BANNED":
            return False, "账号状态为 BANNED，已熔断禁止发帖"

        # 1. 每日配额
        daily_count = self.state.get("daily_count", 0)
        if daily_count >= DAILY_LIMIT:
            return False, f"今日已发 {daily_count} 贴，达到每日上限 {DAILY_LIMIT}"

        # 2. 全局发帖间隔
        last = self.state.get("last_post_time", 0)
        elapsed_min = (now - last) / 60
        if elapsed_min < MIN_INTERVAL_MIN:
            wait = MIN_INTERVAL_MIN - elapsed_min
            return False, f"距上次发帖仅 {elapsed_min:.1f} 分钟，需再等 {wait:.1f} 分钟"

        # 3. 同币种当天唯一
        today = datetime.now().strftime("%Y-%m-%d")
        coin_last_date = self.state.get("coin_last_post_date", {}).get(coin)
        if coin_last_date == today:
            return False, f"{coin} 今日已发过，按规则同币种一天只能发一次"

        # 4. 同币种冷却
        coin_last = self.state.get("coin_last_post", {}).get(coin, 0)
        coin_elapsed_h = (now - coin_last) / 3600
        if coin_last and coin_elapsed_h < COIN_COOLDOWN_H:
            wait_h = COIN_COOLDOWN_H - coin_elapsed_h
            return False, f"{coin} 距上次发帖仅 {coin_elapsed_h:.1f}h，需再等 {wait_h:.1f}h"

        return True, ""

    def record_post(self, coin: str):
        """记录一次成功发帖，更新所有计数器。"""
        now = time.time()
        coin = self._normalize_coin(coin)
        today = datetime.now().strftime("%Y-%m-%d")

        def _mutate(latest_state: dict):
            latest_state["daily_count"] = latest_state.get("daily_count", 0) + 1
            latest_state["total_posts"] = latest_state.get("total_posts", 0) + 1
            latest_state["last_post_time"] = now
            latest_state.setdefault("coin_last_post", {})
            latest_state.setdefault("coin_last_post_date", {})
            latest_state["coin_last_post"][coin] = now
            latest_state["coin_last_post_date"][coin] = today
            latest_state["today"] = today
            return latest_state

        latest = update_state(_mutate)
        if latest is not self.state:
            self.state.clear()
            self.state.update(latest)

    def next_wait_seconds(self) -> float:
        """计算下次发帖需要等待的秒数（含随机抖动）。"""
        base = MIN_INTERVAL_MIN * 60
        jitter = random.uniform(0, MAX_JITTER_MIN * 60)
        return base + jitter


# ──────────────────────────────────────────────
# 广场发帖执行器
# ──────────────────────────────────────────────
class SquarePoster:
    """
    调用币安广场 OpenAPI 发帖。
    未配置 SQUARE_API_KEY 时自动进入模拟模式。
    """

    def __init__(self):
        self.mock_mode = not bool(SQUARE_API_KEY)
        if self.mock_mode:
            print("  [执行层] ⚠️  SQUARE_API_KEY 未配置，运行在模拟模式")

    def post(self, content: str) -> dict:
        """
        发布一条帖子。
        返回标准化结果字典：
          {"success": bool, "code": str, "post_id": str, "mock": bool}
        """
        if self.mock_mode:
            mock_id = f"MOCK_{int(time.time())}"
            return {
                "success": True,
                "code": "000000",
                "post_id": mock_id,
                "url": f"https://www.binance.com/square/post/{mock_id}",
                "mock": True,
            }

        try:
            r = requests.post(
                POST_URL,
                headers={
                    "X-Square-OpenAPI-Key": SQUARE_API_KEY,
                    "Content-Type": "application/json",
                    "clienttype": "binanceSkill",
                },
                json={"bodyTextOnly": content},
                timeout=15,
            )
            try:
                data = r.json()
            except Exception:
                snippet = (getattr(r, "text", "") or "").strip().replace("\n", " ")[:200]
                return {
                    "success": False,
                    "code": f"HTTP_{getattr(r, 'status_code', 'UNKNOWN')}_NON_JSON",
                    "message": snippet or "接口返回非 JSON 响应",
                    "mock": False,
                }

            code = str(data.get("code", ""))
            if code == "000000":
                post_id = str((data.get("data") or {}).get("id") or "")
                if post_id:
                    return {
                        "success": True,
                        "code": code,
                        "post_id": post_id,
                        "url": f"https://www.binance.com/square/post/{post_id}",
                        "message": data.get("message", "success"),
                        "mock": False,
                    }
                return {
                    "success": True,
                    "code": code,
                    "post_id": "",
                    "url": None,
                    "message": "接口返回成功但无返回ID，请到币安广场后台确认是否已成功发帖",
                    "mock": False,
                }

            msg = data.get("message") or ERROR_CODES.get(code, "未知错误")
            return {
                "success": False,
                "code": code or f"HTTP_{getattr(r, 'status_code', 'UNKNOWN')}",
                "message": msg,
                "mock": False,
            }
        except Exception as e:
            return {
                "success": False,
                "code": "NETWORK_ERROR",
                "message": str(e),
                "mock": False,
            }


# ──────────────────────────────────────────────
# 执行层主函数
# ──────────────────────────────────────────────
def execute_post(
    coin_info: dict,
    content: str,
    state: dict,
    quota: QuotaController,
    poster: SquarePoster,
) -> dict:
    """
    执行一次发帖操作，包含配额检查、发帖、状态更新与日志记录。
    返回执行结果字典。
    """
    coin = coin_info["coin"]
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with _posting_transaction(coin):
        _refresh_state_binding(state, quota)

        # 配额检查
        ok, reason = quota.can_post(coin)
        if not ok:
            print(f"  [执行层] ⏸  跳过 {coin}: {reason}")
            return {"success": False, "skipped": True, "reason": reason}

        intent, blocked_reason = _reserve_post_intent(
            state=state,
            content=content,
            coin=coin,
            source="executor",
            tier=coin_info.get("tier", ""),
            mock=getattr(poster, "mock_mode", False),
        )
        if blocked_reason:
            print(f"  [执行层] ⏸  跳过 {coin}: {blocked_reason}")
            return {"success": False, "skipped": True, "reason": blocked_reason}

        # 发帖
        result = poster.post(content)

        if result["success"]:
            quota.record_post(coin)
            _refresh_state_binding(state, quota)
            _finalize_post_success(intent=intent, result=result, content=content)
            _refresh_state_binding(state, quota)
            print(f"  [执行层] ✅ 发帖成功 | {coin} | {result.get('url', '')}")
            print(f"  [执行层] 📊 今日进度: {state['daily_count']}/{DAILY_LIMIT}")
            log_post({
                "time":    now_str,
                "coin":    coin,
                "tier":    coin_info.get("tier", ""),
                "futures": coin_info.get("futures", ""),
                "post_id": result.get("post_id", ""),
                "url":     result.get("url", ""),
                "mock":    result.get("mock", False),
                "preview": content[:100],
                "status":  "SUCCESS",
            })
        else:
            code = result.get("code", "")
            msg  = result.get("message", "")
            print(f"  [执行层] ❌ 发帖失败 | code={code} | {msg}")

            # 熔断：账号封禁
            if code == "2000001":
                latest = update_state(lambda current: {**current, "status": "BANNED"})
                if latest is not state:
                    state.clear()
                    state.update(latest)
                if getattr(quota, "state", None) is state:
                    quota.state = state
                print("  [执行层] 🚨 账号封禁，系统熔断！请检查账号状态。")

            if not _is_ambiguous_post_failure(result):
                _clear_posting_intent(intent["id"])
                _refresh_state_binding(state, quota)

            log_post({
                "time":       now_str,
                "coin":       coin,
                "status":     "FAILED",
                "error_code": code,
                "error_msg":  msg,
            })

        return result
