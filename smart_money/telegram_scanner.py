"""
smart_money/telegram_scanner.py
================================
通过 Telegram 公开网页（t.me/s/频道名）抓取最新消息，
解析为标准化信号格式。无需登录，适用于公开频道。

频道说明：
  - HyperInsight：Hyperliquid 链上巨鲸持仓异动（中文）
  - BWE_OI_Price_monitor：币安合约 OI + 价格异动（中英双语）

信号格式（与 smart_money_monitor.py 兼容）：
  {
    "coin": "BTC",
    "type": "long" | "short" | "oi_surge" | "oi_drop",
    "source": "tg_hyper_insight" | "tg_bwe_oi",
    "action": "开仓" | "加仓" | "减仓" | "平仓" | "OI异动",
    "size_usd": 1234567.0,
    "price": 77523.96,
    "pnl_pct": 0.52,
    "oi_change_pct": 67.5,
    "price_change_pct": 16.9,
    "h24_change_pct": 37.2,
    "note": "...",
    "priority": 1-5,
    "raw_text": "...",
    "ts": 1714000000,
  }
"""

import re
import time
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

# ── 配置 ──────────────────────────────────────────────────────────────────────

TG_CHANNELS = {
    "HyperInsight":         "tg_hyper_insight",
    "BWE_OI_Price_monitor": "tg_bwe_oi",
}

# 只处理最近 N 小时内的消息
MAX_AGE_HOURS = 2

# 每次抓取最近 N 条消息
FETCH_LIMIT = 20

# 缓存文件
_BASE_DIR   = Path(__file__).parent.parent
_CACHE_FILE = _BASE_DIR / "data" / "tg_signals_cache.json"
_CACHE_TTL  = 10 * 60  # 10 分钟

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ── HyperInsight 解析 ─────────────────────────────────────────────────────────

_HI_COIN_RE    = re.compile(r"】(\w+)\s*(多单|空单|多头|空头)")
_HI_ACTION_RE  = re.compile(r"(开仓|加仓|减仓|平仓|止盈|止损|滚仓|摊平|清仓)")
_HI_SIZE_RE    = re.compile(r"约合\s*([\d,]+\.?\d*)\s*美元")
_HI_PRICE_RE   = re.compile(r"当前币价[：:]\s*([\d,]+\.?\d*)\s*美元")
_HI_PNL_RE     = re.compile(r"当前盈亏[^(]*\(([-+]?\d+\.?\d*)%\)")
_HI_NOTE_RE    = re.compile(r"注[：:]\s*(.+?)(?:\n|$)")


def _parse_hyper_insight(text: str, msg_ts: int = 0) -> dict | None:
    """解析 HyperInsight 频道的巨鲸持仓异动消息。"""
    if "持仓异动" not in text and "华尔街速报" not in text:
        return None

    coin_m = _HI_COIN_RE.search(text)
    if not coin_m:
        return None

    coin      = coin_m.group(1).upper()
    direction = coin_m.group(2)
    sig_type  = "long" if "多" in direction else "short"

    action_m = _HI_ACTION_RE.search(text)
    action   = action_m.group(1) if action_m else "持仓变动"

    if action in ("减仓", "止盈", "止损", "平仓", "清仓"):
        priority = 2
    elif action in ("开仓", "加仓", "滚仓", "摊平"):
        priority = 4
    else:
        priority = 3

    size_m   = _HI_SIZE_RE.search(text)
    size_usd = float(size_m.group(1).replace(",", "")) if size_m else 0.0
    if size_usd > 5_000_000:
        priority = min(5, priority + 1)
    elif size_usd < 100_000:
        priority = max(1, priority - 1)

    price_m = _HI_PRICE_RE.search(text)
    price   = float(price_m.group(1).replace(",", "")) if price_m else 0.0

    pnl_m   = _HI_PNL_RE.search(text)
    pnl_pct = float(pnl_m.group(1)) if pnl_m else 0.0

    note_m  = _HI_NOTE_RE.search(text)
    note    = note_m.group(1).strip() if note_m else ""

    return {
        "coin":             coin,
        "type":             sig_type,
        "source":           "tg_hyper_insight",
        "action":           action,
        "size_usd":         size_usd,
        "price":            price,
        "pnl_pct":          pnl_pct,
        "oi_change_pct":    0.0,
        "price_change_pct": 0.0,
        "h24_change_pct":   0.0,
        "note":             note,
        "priority":         priority,
        "raw_text":         text[:300],
        "ts":               msg_ts or int(time.time()),
    }


# ── BWE OI 解析 ───────────────────────────────────────────────────────────────

_BWE_COIN_RE      = re.compile(r"([A-Z]{2,10})USDT")
_BWE_OI_CHANGE_RE = re.compile(r"[Oo]pen\s*[Ii]nterest\s*([-+]?\d+\.?\d*)%", re.IGNORECASE)
_BWE_PRICE_CHG_RE = re.compile(r"[Pp]rice\s*([-+]?\d+\.?\d*)%")
_BWE_OI_VAL_RE    = re.compile(r"OI:\s*\$([\d.]+)([MKmk]?)")
_BWE_24H_RE       = re.compile(r"24H\s*Price\s*Change:\s*([-+]?\d+\.?\d*)%", re.IGNORECASE)
_BWE_DIRECTION_RE = re.compile(r"(🟢|🔻|📈|📉)")


def _parse_bwe_oi(text: str, msg_ts: int = 0) -> dict | None:
    """解析 BWE OI & Price 异动频道消息。"""
    upper = text.upper()
    if "USDT" not in upper:
        return None
    if "INTEREST" not in upper and "OI" not in upper:
        return None

    coin_m = _BWE_COIN_RE.search(text)
    if not coin_m:
        return None
    coin = coin_m.group(1).upper()

    dir_m    = _BWE_DIRECTION_RE.search(text)
    is_up    = dir_m and dir_m.group(1) in ("🟢", "📈")
    sig_type = "oi_surge" if is_up else "oi_drop"

    oi_chg_m  = _BWE_OI_CHANGE_RE.search(text)
    oi_chg    = float(oi_chg_m.group(1)) if oi_chg_m else 0.0

    price_chg_m = _BWE_PRICE_CHG_RE.search(text)
    price_chg   = float(price_chg_m.group(1)) if price_chg_m else 0.0

    oi_val_m = _BWE_OI_VAL_RE.search(text)
    oi_usd   = 0.0
    if oi_val_m:
        val  = float(oi_val_m.group(1))
        unit = oi_val_m.group(2).upper()
        oi_usd = val * (1_000_000 if unit == "M" else 1_000 if unit == "K" else 1)

    h24_m   = _BWE_24H_RE.search(text)
    h24_chg = float(h24_m.group(1)) if h24_m else 0.0

    abs_oi  = abs(oi_chg)
    abs_prc = abs(price_chg)
    if abs_oi >= 50 or abs_prc >= 20:
        priority = 5
    elif abs_oi >= 20 or abs_prc >= 10:
        priority = 4
    elif abs_oi >= 10 or abs_prc >= 5:
        priority = 3
    else:
        priority = 2

    return {
        "coin":             coin,
        "type":             sig_type,
        "source":           "tg_bwe_oi",
        "action":           "OI异动",
        "size_usd":         oi_usd,
        "price":            0.0,
        "pnl_pct":          0.0,
        "oi_change_pct":    oi_chg,
        "price_change_pct": price_chg,
        "h24_change_pct":   h24_chg,
        "note":             f"OI {oi_chg:+.1f}% | 价格 {price_chg:+.1f}% | 24H {h24_chg:+.1f}%",
        "priority":         priority,
        "raw_text":         text[:300],
        "ts":               msg_ts or int(time.time()),
    }


# ── Web 抓取 ──────────────────────────────────────────────────────────────────

def _fetch_channel_web(channel_username: str, limit: int = FETCH_LIMIT,
                       max_age_hours: int = MAX_AGE_HOURS) -> list[tuple[str, int]]:
    """
    通过 t.me/s/{channel} 抓取公开频道消息。
    返回 [(text, unix_ts), ...] 列表（最新在前）。
    """
    import requests
    from bs4 import BeautifulSoup

    url = f"https://t.me/s/{channel_username}"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"[TG-Web] 抓取 @{channel_username} 失败: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    cutoff = int((datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).timestamp())

    results = []
    # 消息容器
    msg_wraps = soup.find_all("div", class_="tgme_widget_message_wrap")
    for wrap in reversed(msg_wraps):  # 从最新开始
        if len(results) >= limit:
            break

        # 提取时间戳
        time_tag = wrap.find("time")
        ts = 0
        if time_tag and time_tag.get("datetime"):
            try:
                dt = datetime.fromisoformat(time_tag["datetime"].replace("Z", "+00:00"))
                ts = int(dt.timestamp())
            except Exception:
                pass

        if ts and ts < cutoff:
            continue

        # 提取文本
        text_div = wrap.find("div", class_="tgme_widget_message_text")
        if not text_div:
            continue
        text = text_div.get_text(separator="\n").strip()
        if text:
            results.append((text, ts))

    logger.info(f"[TG-Web] @{channel_username}: 抓取 {len(results)} 条消息")
    return results


# ── 主扫描函数 ────────────────────────────────────────────────────────────────

def scan_telegram_signals(force: bool = False) -> list[dict]:
    """
    扫描两个 TG 频道，返回标准化信号列表。
    使用 Web 抓取（无需登录），适用于公开频道。
    结果缓存 10 分钟，避免频繁请求。
    """
    _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)

    # 读缓存
    if not force and _CACHE_FILE.exists():
        try:
            cached = json.loads(_CACHE_FILE.read_text())
            age = time.time() - cached.get("ts", 0)
            if age < _CACHE_TTL:
                logger.info(f"[TG扫描] 使用缓存（{age:.0f}s 前更新）")
                return cached.get("signals", [])
        except Exception:
            pass

    parsers = {
        "HyperInsight":         _parse_hyper_insight,
        "BWE_OI_Price_monitor": _parse_bwe_oi,
    }

    signals = []
    for ch, source_key in TG_CHANNELS.items():
        parser = parsers.get(ch)
        if not parser:
            continue

        msgs = _fetch_channel_web(ch)
        count = 0
        for text, ts in msgs:
            sig = parser(text, ts)
            if sig:
                signals.append(sig)
                count += 1
        logger.info(f"[TG扫描] @{ch}: {len(msgs)} 条消息 → {count} 个信号")

    # 去重（同币种同来源只保留最新一条）
    seen: dict = {}
    deduped = []
    for sig in reversed(signals):
        key = (sig["coin"], sig["source"])
        if key not in seen:
            seen[key] = True
            deduped.append(sig)
    signals = list(reversed(deduped))

    # 按优先级排序
    signals.sort(key=lambda x: x["priority"], reverse=True)

    # 写缓存
    try:
        _CACHE_FILE.write_text(json.dumps({
            "ts":      time.time(),
            "signals": signals,
        }, ensure_ascii=False, indent=2))
    except Exception as e:
        logger.warning(f"[TG扫描] 缓存写入失败: {e}")

    logger.info(f"[TG扫描] 共获取 {len(signals)} 个信号")
    return signals


def get_cached_tg_signals() -> list[dict]:
    """读取 TG 信号缓存（不重新扫描）。"""
    if _CACHE_FILE.exists():
        try:
            cached = json.loads(_CACHE_FILE.read_text())
            return cached.get("signals", [])
        except Exception:
            pass
    return []


# ── 调试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print("🔍 正在扫描 TG 频道（Web 抓取模式）...")
    sigs = scan_telegram_signals(force=True)
    print(f"\n共获取 {len(sigs)} 个信号：")
    for s in sigs:
        print(f"  [{s['source']}] {s['coin']} {s['type']} "
              f"优先级={s['priority']} | {s['note'][:60]}")
        if s.get("raw_text"):
            print(f"    原文: {s['raw_text'][:80]}...")
