import time
from dataclasses import dataclass, field
from datetime import datetime

from config.settings import COIN_COOLDOWN_H, DAILY_LIMIT, MIN_INTERVAL_MIN


@dataclass(frozen=True)
class PostIntent:
    capability_name: str
    coin: str
    content: str


@dataclass(frozen=True)
class SafetyDecision:
    allowed: bool
    reason: str
    normalized_coin: str
    risk_level: str
    checks_run: list[str] = field(default_factory=list)


class SafetyGate:
    def __init__(self, restricted_phrases=None):
        phrases = ["稳赚", "guaranteed profit", "保本", "稳赚不赔"] if restricted_phrases is None else restricted_phrases
        self.restricted_phrases = [phrase for phrase in phrases if phrase]

    def evaluate(self, state: dict, intent: PostIntent) -> SafetyDecision:
        checks_run: list[str] = []
        normalized_coin = self._normalize_coin(intent.coin)
        risk_level = self._risk_level_for(intent.capability_name)

        checks_run.append("account")
        if str(state.get("status", "")).upper() == "BANNED":
            return SafetyDecision(False, "账号状态为 BANNED，禁止执行写操作", normalized_coin, risk_level, checks_run)

        checks_run.append("content")
        if not normalized_coin:
            return SafetyDecision(False, "缺少有效币种标识", normalized_coin, risk_level, checks_run)
        if not str(intent.content or "").strip():
            return SafetyDecision(False, "内容为空，禁止执行", normalized_coin, risk_level, checks_run)
        lowered = str(intent.content or "").lower()
        for phrase in self.restricted_phrases:
            phrase_text = str(phrase)
            if phrase_text and phrase_text.lower() in lowered:
                return SafetyDecision(False, f"内容包含安全敏感表达: {phrase_text}", normalized_coin, risk_level, checks_run)

        if risk_level != "write":
            return SafetyDecision(True, "只读能力通过安全检查", normalized_coin, risk_level, checks_run)

        checks_run.append("quota")
        daily_count = int(state.get("daily_count", 0) or 0)
        if daily_count >= DAILY_LIMIT:
            return SafetyDecision(False, f"今日已达上限 {DAILY_LIMIT}", normalized_coin, risk_level, checks_run)

        last_post_time = float(state.get("last_post_time", 0) or 0)
        elapsed_min = (time.time() - last_post_time) / 60 if last_post_time else None
        if elapsed_min is not None and elapsed_min < MIN_INTERVAL_MIN:
            return SafetyDecision(False, f"全局冷却未结束，需再等待 {MIN_INTERVAL_MIN - elapsed_min:.1f} 分钟", normalized_coin, risk_level, checks_run)

        today = datetime.now().strftime("%Y-%m-%d")
        coin_last_post_date = (state.get("coin_last_post_date") or {}).get(normalized_coin)
        if coin_last_post_date == today:
            return SafetyDecision(False, f"{normalized_coin} 今日已发过，禁止重复发帖", normalized_coin, risk_level, checks_run)

        coin_last_post = float((state.get("coin_last_post") or {}).get(normalized_coin, 0) or 0)
        if coin_last_post:
            elapsed_hours = (time.time() - coin_last_post) / 3600
            if elapsed_hours < COIN_COOLDOWN_H:
                return SafetyDecision(False, f"{normalized_coin} 冷却未结束，需再等待 {COIN_COOLDOWN_H - elapsed_hours:.1f} 小时", normalized_coin, risk_level, checks_run)

        return SafetyDecision(True, "通过安全检查", normalized_coin, risk_level, checks_run)

    def _normalize_coin(self, coin: str) -> str:
        normalized = (coin or "").strip().upper()
        if normalized.endswith("USDT"):
            normalized = normalized[:-4]
        return normalized

    def _risk_level_for(self, capability_name: str) -> str:
        if capability_name == "binance.square.create_post":
            return "write"
        return "read"
