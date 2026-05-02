#!/usr/bin/env python3
"""
聪明钱信号 → 内容层适配器
将链上聪明钱信号转化为高转化率的短贴素材

核心逻辑：
1. 读取 Hyperliquid 聪明钱信号（smart_money_signal.json）
2. 读取 TG 频道信号（HyperInsight + BWE_OI_Price_monitor）
3. 融合两路信号，同一币种多源共振 → 最高优先级
4. 生成对应的短贴 Prompt，注入期货合约标签和内容挖矿 cashtag
5. 输出给内容层（content.py）使用
"""

import json
import os
import logging
from typing import Optional

try:
    from utils.price_sync import PRICE_FRESHNESS_TTL, is_price_fresh
except Exception:  # pragma: no cover - keep adapter importable in minimal test envs
    PRICE_FRESHNESS_TTL = 600

    def is_price_fresh(price_info: Optional[dict], max_age: float = PRICE_FRESHNESS_TTL) -> bool:
        return False

logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
SMART_MONEY_CACHE = os.path.join(DATA_DIR, "smart_money_signal.json")

# 代币 → 期货合约标签映射
FUTURES_TAG_MAP = {
    "BTC":   "#BTCUSDT #BTC合约",
    "ETH":   "#ETHUSDT #ETH合约",
    "SOL":   "#SOLUSDT #SOL合约",
    "BNB":   "#BNBUSDT #BNB合约",
    "XRP":   "#XRPUSDT #XRP合约",
    "DOGE":  "#DOGEUSDT #DOGE合约",
    "ADA":   "#ADAUSDT #ADA合约",
    "AVAX":  "#AVAXUSDT #AVAX合约",
    "LINK":  "#LINKUSDT #LINK合约",
    "DOT":   "#DOTUSDT #DOT合约",
    "MATIC": "#MATICUSDT #MATIC合约",
    "UNI":   "#UNIUSDT #UNI合约",
    "AAVE":  "#AAVEUSDT #AAVE合约",
    "OP":    "#OPUSDT #OP合约",
    "ARB":   "#ARBUSDT #ARB合约",
    "SUI":   "#SUIUSDT #SUI合约",
    "APT":   "#APTUSDT #APT合约",
    "INJ":   "#INJUSDT #INJ合约",
    "TIA":   "#TIAUSDT #TIA合约",
    "HYPE":  "#HYPEUSDT #HYPE合约",
    "WIF":   "#WIFUSDT #WIF合约",
    "PEPE":  "#PEPEUSDT #PEPE合约",
    "BONK":  "#BONKUSDT #BONK合约",
    "MON":   "#MONUSDT #MON合约",
    "ZEC":   "#ZECUSDT #ZEC合约",
    "TRUMP": "#TRUMPUSDT #TRUMP合约",
    "ORCA":  "#ORCAUSDT #ORCA合约",
    "HYPER": "#HYPERUSDT #HYPER合约",
}

# ── 格式常量 ──────────────────────────────────────────────────────────────────

# 固定免责声明
DISCLAIMER = "⚠️免责声明：\n本文仅为个人行情观点分享，不构成任何投资建议，加密货币市场高波动、高风险，请理性交易、自行承担风险。"
CTA_FIXED = "💡 点击下方币种标签🏷️ 查看实时行情，广场内交易还能给我贡献一点挖矿收益😄"

# 通用话题标签
GENERAL_TAGS = "#加密货币 #合约分析 #山寨币观察 #交易策略分享"

# ── Prompt 模板 ────────────────────────────────────────────────────────────────

PROMPT_TEMPLATES = {
    "LONG_HIGH": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，风格犀利、有观点、真实可信。

当前链上聪明钱信号：
- 代币：{coin}
- {whale_count} 个 Hyperliquid 顶级大户中 {long_count} 个正在做多，多头占比 {long_ratio:.0f}%
- 大户总持仓规模：${total_size_m:.1f}M
- {price_context_line}
- 资金费率：{funding_rate:+.4f}%（{funding_signal}）

请生成一条币安广场短贴，严格按以下格式输出（不要加任何额外内容）：

[正文]
（80-150字，有观点有态度，像真人在说话，开头必须有强力Hook，不要空话套话）

{general_tags}
{disclaimer} {cashtag_line}
{cta_fixed}
只输出上述格式的内容，不要输出任何解释、前缀或引号。
禁止使用的词：「值得关注」「仅供参考」「据悉」「据了解」
""",

    "SHORT_HIGH": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，风格犀利、有观点、真实可信。

当前链上聪明钱信号：
- 代币：{coin}
- {whale_count} 个 Hyperliquid 顶级大户中 {short_count} 个正在做空，空头占比 {short_ratio:.0f}%
- 大户总持仓规模：${total_size_m:.1f}M
- {price_context_line}
- 资金费率：{funding_rate:+.4f}%（{funding_signal}）

请生成一条币安广场短贴，严格按以下格式输出（不要加任何额外内容）：

[正文]
（80-150字，有观点有态度，像真人在说话，开头必须有强力Hook，不要空话套话）

{general_tags}
{disclaimer} {cashtag_line}
{cta_fixed}
只输出上述格式的内容，不要输出任何解释、前缀或引号。
禁止使用的词：「值得关注」「仅供参考」「据悉」「据了解」
""",

    "FUNDING_EXTREME": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，专门研究资金费率和市场情绪。

当前资金费率异常信号：
- 代币：{coin}
- 资金费率：{funding_rate:+.4f}%（{funding_signal}）
- {price_context_line}

请生成一条币安广场短贴，严格按以下格式输出（不要加任何额外内容）：

[正文]
（80-150字，解释资金费率异常意味着什么，给出个人交易观点，像真人在说话）

{general_tags}
{disclaimer} {cashtag_line}
{cta_fixed}
只输出上述格式的内容，不要输出任何解释、前缀或引号。
禁止使用的词：「值得关注」「仅供参考」
""",

    "OI_SURGE": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，专门研究链上持仓数据。

当前 OI：
- 代币：{coin}
- 当前 OI：${oi_usd_m:.1f}M，24H成交量：${day_vol_m:.1f}M
- {price_context_line}

请生成一条币安广场短贴，严格按以下格式输出（不要加任何额外内容）：

[正文]
（80-150字，用OI数据说明主力资金动向，给出后市判断，像真人在说话）

{general_tags}
{disclaimer} {cashtag_line}
{cta_fixed}
只输出上述格式的内容，不要输出任何解释、前缀或引号。
禁止使用的词：「值得关注」「仅供参考」
""",

    # ── TG 专属模板 ────────────────────────────────────────────────────────────

    "TG_WHALE_LONG": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，专门追踪链上巨鲸动向。

最新链上巨鲸信号（来自 HyperInsight）：
- 代币：{coin}
- 操作：{action}（做多），规模：约 ${size_usd_m:.2f}M
- {price_context_line}，当前浮盈：{pnl_pct:+.1f}%
- 巨鲸背景：{note}

请生成一条币安广场短贴，严格按以下格式输出（不要加任何额外内容）：

[正文]
（80-150字，突出巨鲸真实操作金额，结合背景讲故事，像真人在说话）

{general_tags}
{disclaimer} {cashtag_line}
{cta_fixed}
只输出上述格式的内容，不要输出任何解释、前缀或引号。
禁止使用的词：「值得关注」「仅供参考」「据悉」「据了解」
""",

    "TG_WHALE_SHORT": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，专门追踪链上巨鲸动向。

最新链上巨鲸信号（来自 HyperInsight）：
- 代币：{coin}
- 操作：{action}（做空），规模：约 ${size_usd_m:.2f}M
- {price_context_line}，当前浮盈：{pnl_pct:+.1f}%
- 巨鲸背景：{note}

请生成一条币安广场短贴，严格按以下格式输出（不要加任何额外内容）：

[正文]
（80-150字，突出巨鲸真实做空操作，分析为何在此价位做空，像真人在说话）

{general_tags}
{disclaimer} {cashtag_line}
{cta_fixed}
只输出上述格式的内容，不要输出任何解释、前缀或引号。
禁止使用的词：「值得关注」「仅供参考」「据悉」「据了解」
""",

    "TG_OI_SURGE": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，专门研究 OI 和价格异动。

最新币安合约异动信号（来自方程式 OI 监控）：
- 代币：{coin}
- OI 变化：{oi_change_pct:+.1f}%，价格变化：{price_change_pct:+.1f}%（过去 1 小时）
- 24H涨跌：{h24_change_pct:+.1f}%，当前 OI：${oi_usd_m:.1f}M

请生成一条币安广场短贴，严格按以下格式输出（不要加任何额外内容）：

[正文]
（80-150字，突出OI和价格同步暴涨的异常信号，解释主力快速建仓，语气紧迫真实）

{general_tags}
{disclaimer} {cashtag_line}
{cta_fixed}
只输出上述格式的内容，不要输出任何解释、前缀或引号。
禁止使用的词：「值得关注」「仅供参考」
""",

    "TG_OI_DROP": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，专门研究 OI 和价格异动。

最新币安合约异动信号（来自方程式 OI 监控）：
- 代币：{coin}
- OI 变化：{oi_change_pct:+.1f}%，价格变化：{price_change_pct:+.1f}%（过去 1 小时）
- 24H涨跌：{h24_change_pct:+.1f}%，当前 OI：${oi_usd_m:.1f}M

请生成一条币安广场短贴，严格按以下格式输出（不要加任何额外内容）：

[正文]
（80-150字，突出OI骤降+价格下跌的异常信号，解释主力平仓或做空，语气真实自然）

{general_tags}
{disclaimer} {cashtag_line}
{cta_fixed}
只输出上述格式的内容，不要输出任何解释、前缀或引号。
禁止使用的词：「值得关注」「仅供参考」
""",

    "TG_COMBINED": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，同时追踪链上巨鲸和合约 OI 数据。

多源信号共振（Hyperliquid 巨鲸 + 币安 OI 双重确认）：
- 代币：{coin}
- 巨鲸信号：{whale_action}（${whale_size_m:.1f}M）
- OI 异动：{oi_change_pct:+.1f}%，价格 {price_change_pct:+.1f}%，24H涨跌 {h24_change_pct:+.1f}%

请生成一条币安广场短贴，严格按以下格式输出（不要加任何额外内容）：

[正文]
（80-150字，突出「链上巨鲸+OI双重共振」的稀有信号，用数据说明两个信号共同指向，语气充满信心）

{general_tags}
{disclaimer} {cashtag_line}
{cta_fixed}
只输出上述格式的内容，不要输出任何解释、前缀或引号。
禁止使用的词：「值得关注」「仅供参考」
""",
}

# CTA 已内嵌在格式要求中（免责声明末尾的 $CashTag），不再单独追加
CTA_TEMPLATES = [""]  # 保留兼容性，实际不使用


def _price_metadata_from_signal(signal: dict) -> dict:
    """Extract root-level freshness metadata produced by utils.price_sync."""
    data = signal.get("data", {}) if isinstance(signal, dict) else {}
    price_ts = signal.get("_price_ts", data.get("_price_ts") or data.get("ts"))
    synced = signal.get("_price_synced", data.get("_price_synced"))
    is_live = signal.get("is_live", data.get("is_live"))
    if is_live is None and synced is not None:
        is_live = is_price_fresh({"ts": price_ts}, max_age=PRICE_FRESHNESS_TTL)
    source = signal.get("_price_source", data.get("_price_source", "binance_futures"))
    return {
        "_price_synced": bool(synced),
        "_price_source": source,
        "_price_ts": price_ts,
        "source": signal.get("source", data.get("source", source)),
        "is_live": bool(is_live),
        "warning_reason": signal.get("warning_reason", data.get("warning_reason")),
    }


def _fresh_price_fields(signal: dict) -> dict:
    """Return exact price fields only when signal carries a fresh futures price contract."""
    data = signal.get("data", {}) if isinstance(signal, dict) else {}
    meta = _price_metadata_from_signal(signal)
    mark_px = data.get("mark_px", data.get("price", 0))
    if mark_px and meta["is_live"]:
        return {
            "mark_px": mark_px,
            "price": data.get("price", mark_px),
            "change_24h": data.get("change_24h", data.get("h24_change_pct", 0)),
            "high_24h": data.get("high_24h"),
            "low_24h": data.get("low_24h"),
        }
    return {
        "mark_px": 0,
        "price": 0,
        "change_24h": data.get("change_24h", data.get("h24_change_pct", 0)),
        "high_24h": None,
        "low_24h": None,
    }




# ── 信号融合 ───────────────────────────────────────────────────────────────────

def _merge_and_rank_signals(hl_signals: list, tg_signals: list) -> list:
    """
    融合 Hyperliquid 信号和 TG 信号，按优先级排序。

    规则：
    - 同一币种在两个来源均有信号 → 「多源共振」，优先级最高（0）
    - Hyperliquid HIGH confidence → 优先级 1
    - TG 巨鲸开仓/加仓（priority>=4） → 优先级 2
    - TG OI 异动（priority>=4） → 优先级 3
    - 其余信号 → 优先级 4+
    """
    merged = []

    # 建立 TG 信号索引（按 coin）
    tg_by_coin: dict = {}
    for ts in tg_signals:
        coin = ts["coin"]
        tg_by_coin.setdefault(coin, []).append(ts)

    # 建立 HL 信号索引
    hl_by_coin: dict = {}
    for hs in hl_signals:
        hl_by_coin[hs["coin"]] = hs

    # 检测多源共振
    combined_coins = set(tg_by_coin.keys()) & set(hl_by_coin.keys())
    for coin in combined_coins:
        hl_sig = hl_by_coin[coin]
        best_tg = max(tg_by_coin[coin], key=lambda x: x["priority"])
        merged.append({
            "type": "TG_COMBINED",
            "coin": coin,
            "data": {
                "hl": hl_sig["data"],
                "tg": best_tg,
                "whale_action": f"{best_tg.get('action', '操作')} ({hl_sig['type']})",
                "whale_size_m": hl_sig["data"].get("total_size_usd", 0) / 1e6,
                "oi_change_pct": best_tg.get("oi_change_pct", 0),
                "price_change_pct": best_tg.get("price_change_pct", 0),
                "h24_change_pct": best_tg.get("h24_change_pct", 0),
            },
            "priority": 0,
        })

    # 添加未共振的 HL 信号
    for coin, hs in hl_by_coin.items():
        if coin not in combined_coins:
            merged.append(hs)

    # 添加未共振的高优先级 TG 信号
    for coin, tg_list in tg_by_coin.items():
        if coin not in combined_coins:
            for ts in tg_list:
                if ts["priority"] >= 4:
                    if ts["source"] == "tg_hyper_insight":
                        sig_type = "TG_WHALE_LONG" if ts["type"] == "long" else "TG_WHALE_SHORT"
                        merged.append({
                            "type": sig_type,
                            "coin": coin,
                            "data": ts,
                            "priority": 2,
                        })
                    elif ts["source"] == "tg_bwe_oi":
                        sig_type = "TG_OI_SURGE" if ts["type"] == "oi_surge" else "TG_OI_DROP"
                        merged.append({
                            "type": sig_type,
                            "coin": coin,
                            "data": ts,
                            "priority": 3,
                        })

    merged.sort(key=lambda x: x["priority"])
    return merged


def get_all_signals() -> list:
    """获取所有有效信号（Hyperliquid + TG 融合），按优先级排序"""
    hl_signals = []

    # ── Hyperliquid 信号 ──
    if os.path.exists(SMART_MONEY_CACHE):
        try:
            with open(SMART_MONEY_CACHE, "r", encoding="utf-8") as f:
                data = json.load(f)

            for sig in data.get("top_signals", []):
                if sig.get("confidence") in ["HIGH", "MEDIUM"]:
                    hl_signals.append({
                        "type": "LONG_HIGH" if sig["net_direction"] == "LONG" else "SHORT_HIGH",
                        "coin": sig["coin"],
                        "data": sig,
                        "priority": 1 if sig["confidence"] == "HIGH" else 2,
                    })

            for fs in data.get("funding_rate_signals", []):
                if abs(fs.get("funding_rate", 0)) > 0.02:
                    hl_signals.append({
                        "type": "FUNDING_EXTREME",
                        "coin": fs["coin"],
                        "data": fs,
                        "priority": 2,
                    })

            for c in data.get("market_overview", {}).get("hot_coins_24h", []):
                if abs(c.get("change_24h", 0)) > 8:
                    hl_signals.append({
                        "type": "OI_SURGE",
                        "coin": c["coin"],
                        "data": c,
                        "priority": 3,
                    })
        except Exception as e:
            logger.warning(f"[信号融合] HL 信号读取失败: {e}")

    # ── TG 信号 ──
    tg_signals = []
    try:
        import sys
        import os as _os
        _pkg_root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
        if _pkg_root not in sys.path:
            sys.path.insert(0, _pkg_root)
        from smart_money.telegram_scanner import scan_telegram_signals
        tg_signals = scan_telegram_signals()
        logger.info(f"[信号融合] TG 信号: {len(tg_signals)} 个")
    except Exception as e:
        logger.warning(f"[信号融合] TG 扫描失败: {e}")

    logger.info(f"[信号融合] HL 信号: {len(hl_signals)} 个, TG 信号: {len(tg_signals)} 个")

    # ── 融合 + 排序 ──
    return _merge_and_rank_signals(hl_signals, tg_signals)


def get_top_signal() -> Optional[dict]:
    """获取当前最强的聪明钱信号"""
    signals = get_all_signals()
    return signals[0] if signals else None


# ── Prompt 构建 ────────────────────────────────────────────────────────────────

def _signal_price_ts(signal: dict, data: dict) -> Optional[float]:
    """Return the best-known price timestamp from signal/data metadata."""
    for container in (data, signal):
        for key in ("_price_ts", "price_ts", "ts"):
            value = container.get(key) if isinstance(container, dict) else None
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return None


def _signal_has_price_freshness_contract(signal: dict, data: dict) -> bool:
    """Whether this signal carries explicit freshness/sync metadata."""
    for container in (data, signal):
        if not isinstance(container, dict):
            continue
        if any(key in container for key in ("_price_synced", "is_live", "price_age_sec")):
            return True
        if any(key in container for key in ("_price_ts", "price_ts", "ts")):
            return True
    return False


def _is_signal_price_fresh(signal: dict, data: dict) -> bool:
    """Only explicit fresh/synced metadata may justify embedding exact prices."""
    has_contract = _signal_has_price_freshness_contract(signal, data)
    if not has_contract:
        return False

    explicit_live = data.get("is_live", signal.get("is_live"))
    if explicit_live is False:
        return False

    explicit_synced = data.get("_price_synced", signal.get("_price_synced"))
    if explicit_synced is False:
        return False

    ts = _signal_price_ts(signal, data)
    if ts is None:
        return False
    return is_price_fresh({"ts": ts}, max_age=PRICE_FRESHNESS_TTL)


def _price_context_line(signal: dict, data: dict, price_field: str = "mark_px", change_field: str = "change_24h") -> str:
    """
    Build a prompt line that avoids exact stale/unsynced prices.

    Historical HL/TG payloads often contain mark/entry prices without freshness
    metadata. Those can be useful for internal ranking but must not be described
    to the LLM as current/exact prices unless price_sync has marked them fresh.
    """
    price = data.get(price_field)
    if price is None and price_field != "price":
        price = data.get("price")
    change = data.get(change_field, data.get("change_24h", data.get("h24_change_pct", 0)))

    if price and _is_signal_price_fresh(signal, data):
        source = data.get("_price_source") or signal.get("_price_source")
        prefix = "币安期货实时价格" if source == "binance_futures" else "当前价格"
        return f"{prefix}：${float(price):,.2f}，24H涨跌 {float(change):+.1f}%"

    return f"价格状态：未获取到新鲜同步价格，禁止在正文中编造或引用具体当前价；可只使用24H涨跌 {float(change):+.1f}% 和其他非价格信号"

def build_content_prompt(signal: dict, cta_index: int = 0) -> dict:
    """
    将聪明钱信号转化为内容层 Prompt
    返回 {prompt, coin, futures_tags, signal_type}
    """
    sig_type = signal["type"]
    coin = signal["coin"]
    data = signal["data"]

    futures_tags = FUTURES_TAG_MAP.get(coin, f"#{coin}USDT #{coin}合约")
    price_meta = _price_metadata_from_signal(signal)
    fresh_price = _fresh_price_fields(signal)
    # 构建 $CashTag 行（触发合约卡片）
    cashtag_line = f"${coin} $BTC $ETH $BNB"
    # 不再使用独立 CTA，格式已内嵌在 Prompt 中
    cta = ""

    prompt = ""

    if sig_type in ["LONG_HIGH", "SHORT_HIGH"]:
        total_count  = data.get("whale_count", 0)
        long_count   = data.get("long_count", data.get("whale_count", 1))
        short_count  = total_count - long_count
        long_ratio   = data.get("long_ratio", 50)
        short_ratio  = 100 - long_ratio
        total_size_m = data.get("total_size_usd", 0) / 1e6
        mark_px      = data.get("mark_px", 0)
        change_24h   = data.get("change_24h", 0)
        funding_rate = data.get("funding_rate", 0)
        funding_signal = "多头付费" if funding_rate > 0 else "空头付费"

        prompt = PROMPT_TEMPLATES[sig_type].format(
            coin=coin,
            whale_count=total_count,
            long_count=long_count,
            short_count=short_count,
            long_ratio=long_ratio,
            short_ratio=short_ratio,
            total_size_m=total_size_m,
            price_context_line=_price_context_line(signal, data, "mark_px", "change_24h"),
            funding_rate=funding_rate,
            funding_signal=funding_signal,
            futures_tags=futures_tags,
            general_tags=GENERAL_TAGS,
            disclaimer=DISCLAIMER,
            cashtag_line=cashtag_line,
            cta_fixed=CTA_FIXED,
        )

    elif sig_type == "FUNDING_EXTREME":
        funding_rate   = data.get("funding_rate", 0)
        funding_signal = "多头过热" if funding_rate > 0 else "空头过热"
        mark_px        = data.get("mark_px", 0)
        change_24h     = data.get("change_24h", 0)

        prompt = PROMPT_TEMPLATES["FUNDING_EXTREME"].format(
            coin=coin,
            funding_rate=funding_rate,
            funding_signal=funding_signal,
            price_context_line=_price_context_line(signal, data, "mark_px", "change_24h"),
            futures_tags=futures_tags,
            general_tags=GENERAL_TAGS,
            disclaimer=DISCLAIMER,
            cashtag_line=cashtag_line,
            cta_fixed=CTA_FIXED,
        )

    elif sig_type == "OI_SURGE":
        mark_px    = data.get("mark_px", 0)
        change_24h = data.get("change_24h", 0)
        oi_usd_m   = data.get("oi_usd", 0) / 1e6
        day_vol_m  = data.get("day_volume", 0) / 1e6

        prompt = PROMPT_TEMPLATES["OI_SURGE"].format(
            coin=coin,
            price_context_line=_price_context_line(signal, data, "mark_px", "change_24h"),
            oi_usd_m=oi_usd_m,
            day_vol_m=day_vol_m,
            futures_tags=futures_tags,
            general_tags=GENERAL_TAGS,
            disclaimer=DISCLAIMER,
            cashtag_line=cashtag_line,
            cta_fixed=CTA_FIXED,
        )

    elif sig_type in ["TG_WHALE_LONG", "TG_WHALE_SHORT"]:
        size_usd_m = data.get("size_usd", 0) / 1e6
        price      = data.get("price", 0)
        pnl_pct    = data.get("pnl_pct", 0)
        action     = data.get("action", "操作")
        note       = data.get("note", "")[:80]

        prompt = PROMPT_TEMPLATES[sig_type].format(
            coin=coin,
            action=action,
            size_usd_m=size_usd_m,
            price_context_line=_price_context_line(signal, data, "price", "change_24h"),
            pnl_pct=pnl_pct,
            note=note,
            futures_tags=futures_tags,
            general_tags=GENERAL_TAGS,
            disclaimer=DISCLAIMER,
            cashtag_line=cashtag_line,
            cta_fixed=CTA_FIXED,
        )

    elif sig_type in ["TG_OI_SURGE", "TG_OI_DROP"]:
        oi_change_pct    = data.get("oi_change_pct", 0)
        price_change_pct = data.get("price_change_pct", 0)
        h24_change_pct   = data.get("h24_change_pct", 0)
        oi_usd_m         = data.get("size_usd", 0) / 1e6

        prompt = PROMPT_TEMPLATES[sig_type].format(
            coin=coin,
            oi_change_pct=oi_change_pct,
            price_change_pct=price_change_pct,
            h24_change_pct=h24_change_pct,
            oi_usd_m=oi_usd_m,
            futures_tags=futures_tags,
            general_tags=GENERAL_TAGS,
            disclaimer=DISCLAIMER,
            cashtag_line=cashtag_line,
            cta_fixed=CTA_FIXED,
        )

    elif sig_type == "TG_COMBINED":
        whale_action     = data.get("whale_action", "操作")
        whale_size_m     = data.get("whale_size_m", 0)
        oi_change_pct    = data.get("oi_change_pct", 0)
        price_change_pct = data.get("price_change_pct", 0)
        h24_change_pct   = data.get("h24_change_pct", 0)

        prompt = PROMPT_TEMPLATES["TG_COMBINED"].format(
            coin=coin,
            whale_action=whale_action,
            whale_size_m=whale_size_m,
            oi_change_pct=oi_change_pct,
            price_change_pct=price_change_pct,
            h24_change_pct=h24_change_pct,
            futures_tags=futures_tags,
            general_tags=GENERAL_TAGS,
            disclaimer=DISCLAIMER,
            cashtag_line=cashtag_line,
            cta_fixed=CTA_FIXED,
        )

    else:
        prompt = f"请生成一条关于 {coin} 的高吸引力广场短贴，包含标签 {futures_tags}"

    coin_info_patch = {
        "mark_px": fresh_price["mark_px"],
        "price": fresh_price["price"],
        "change_24h": fresh_price["change_24h"],
        "high_24h": fresh_price["high_24h"],
        "low_24h": fresh_price["low_24h"],
        "_price_synced": price_meta["_price_synced"],
        "_price_source": price_meta["_price_source"],
        "_price_ts": price_meta["_price_ts"],
        "source": price_meta["source"],
        "is_live": price_meta["is_live"],
    }
    if price_meta.get("warning_reason"):
        coin_info_patch["warning_reason"] = price_meta["warning_reason"]

    return {
        "prompt":      prompt,
        "coin":        coin,
        "futures_tags": futures_tags,
        "signal_type": sig_type,
        "cta":         cta,
        "price_metadata": price_meta,
        "coin_info_patch": coin_info_patch,
    }


def get_content_hints_for_display() -> list:
    """获取简洁的内容提示（用于日志显示）"""
    if not os.path.exists(SMART_MONEY_CACHE):
        return []
    try:
        with open(SMART_MONEY_CACHE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("content_hints", [])
    except Exception:
        return []


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print("🧪 测试聪明钱信号适配器（含 TG 融合）...")

    signals = get_all_signals()
    if signals:
        print(f"✅ 获取到 {len(signals)} 个融合信号")
        for i, sig in enumerate(signals[:5]):
            print(f"\n信号 {i+1}: [{sig['type']}] {sig['coin']}  优先级={sig['priority']}")
            content = build_content_prompt(sig, cta_index=i)
            print(f"  Prompt 长度: {len(content['prompt'])} 字符")
            print(f"  期货标签: {content['futures_tags']}")
    else:
        print("⚠️  暂无信号")
