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
  220003 → API Key 无效
  220011 → 内容为空
  20022  → 内容含敏感词
  2000001→ 账号被封禁（触发熔断）
"""

import time
import random
import requests
from datetime import datetime

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    SQUARE_API_KEY, DAILY_LIMIT, MIN_INTERVAL_MIN,
    COIN_COOLDOWN_H, MAX_JITTER_MIN,
)
from core.state import save_state, log_post

POST_URL = (
    "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"
)

# 错误码说明
ERROR_CODES = {
    "220003":  "API Key 无效或未找到",
    "220004":  "API Key 已过期",
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

    def can_post(self, coin: str) -> tuple:
        """
        返回 (bool, str)：是否可以发帖，以及拒绝原因。
        """
        now = time.time()

        # 1. 每日配额
        if self.state["daily_count"] >= DAILY_LIMIT:
            return False, f"今日已发 {self.state['daily_count']} 贴，达到每日上限 {DAILY_LIMIT}"

        # 2. 全局发帖间隔
        last = self.state.get("last_post_time", 0)
        elapsed_min = (now - last) / 60
        if elapsed_min < MIN_INTERVAL_MIN:
            wait = MIN_INTERVAL_MIN - elapsed_min
            return False, f"距上次发帖仅 {elapsed_min:.1f} 分钟，需再等 {wait:.1f} 分钟"

        # 3. 同币种冷却
        coin_last = self.state.get("coin_last_post", {}).get(coin, 0)
        coin_elapsed_h = (now - coin_last) / 3600
        if coin_elapsed_h < COIN_COOLDOWN_H:
            wait_h = COIN_COOLDOWN_H - coin_elapsed_h
            return False, f"{coin} 距上次发帖仅 {coin_elapsed_h:.1f}h，需再等 {wait_h:.1f}h"

        return True, ""

    def record_post(self, coin: str):
        """记录一次成功发帖，更新所有计数器。"""
        now = time.time()
        self.state["daily_count"] = self.state.get("daily_count", 0) + 1
        self.state["total_posts"] = self.state.get("total_posts", 0) + 1
        self.state["last_post_time"] = now
        if "coin_last_post" not in self.state:
            self.state["coin_last_post"] = {}
        self.state["coin_last_post"][coin] = now
        self.state["today"] = datetime.now().strftime("%Y-%m-%d")
        save_state(self.state)

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
            data = r.json()
            code = str(data.get("code", ""))

            if code == "000000":
                post_id = data.get("data", {}).get("id", "")
                return {
                    "success": True,
                    "code": code,
                    "post_id": post_id,
                    "url": f"https://www.binance.com/square/post/{post_id}",
                    "mock": False,
                }
            else:
                msg = data.get("message", ERROR_CODES.get(code, "未知错误"))
                return {
                    "success": False,
                    "code": code,
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

    # 配额检查
    ok, reason = quota.can_post(coin)
    if not ok:
        print(f"  [执行层] ⏸  跳过 {coin}: {reason}")
        return {"success": False, "skipped": True, "reason": reason}

    # 发帖
    result = poster.post(content)

    if result["success"]:
        quota.record_post(coin)
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
            state["status"] = "BANNED"
            save_state(state)
            print("  [执行层] 🚨 账号封禁，系统熔断！请检查账号状态。")

        log_post({
            "time":       now_str,
            "coin":       coin,
            "status":     "FAILED",
            "error_code": code,
            "error_msg":  msg,
        })

    return result
