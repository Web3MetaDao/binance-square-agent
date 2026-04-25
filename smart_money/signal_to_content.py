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

# ── Prompt 模板 ────────────────────────────────────────────────────────────────

PROMPT_TEMPLATES = {
    "LONG_HIGH": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，风格犀利、有数据支撑、真实可信。

当前链上聪明钱信号：
- 代币：{coin}
- 信号：{whale_count} 个 Hyperliquid 顶级大户中 {long_count} 个正在做多
- 多空比：{long_ratio:.0f}% 做多
- 大户总持仓规模：${total_size_m:.1f}M
- 当前价格：${mark_px:,.2f}
- 24小时涨跌：{change_24h:+.1f}%
- 资金费率：{funding_rate:+.4f}%（{funding_signal}）

请生成一条 80-150 字的高吸引力广场短贴，要求：
1. 开头必须有强力 Hook（让人忍不住继续读）
2. 用真实数据说话，不要空话套话
3. 结尾引导用户点击合约链接
4. 语气真实自然，像真人在说话，不要 AI 八股文
5. 必须包含这些标签：{futures_tags} #链上数据 #聪明钱

禁止使用的词：「值得关注」「不构成投资建议」「仅供参考」「据悉」「据了解」
""",

    "SHORT_HIGH": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，风格犀利、有数据支撑、真实可信。

当前链上聪明钱信号：
- 代币：{coin}
- 信号：{whale_count} 个 Hyperliquid 顶级大户中 {short_count} 个正在做空
- 多空比：{short_ratio:.0f}% 做空
- 大户总持仓规模：${total_size_m:.1f}M
- 当前价格：${mark_px:,.2f}
- 24小时涨跌：{change_24h:+.1f}%
- 资金费率：{funding_rate:+.4f}%（{funding_signal}）

请生成一条 80-150 字的高吸引力广场短贴，要求：
1. 开头必须有强力 Hook（让人感到紧迫或好奇）
2. 用真实链上数据支撑观点
3. 结尾引导用户点击合约链接
4. 语气真实自然，像真人在说话
5. 必须包含这些标签：{futures_tags} #链上数据 #聪明钱

禁止使用的词：「值得关注」「不构成投资建议」「仅供参考」「据悉」「据了解」
""",

    "FUNDING_EXTREME": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，专门研究资金费率和市场情绪。

当前资金费率异常信号：
- 代币：{coin}
- 资金费率：{funding_rate:+.4f}%（{funding_signal}）
- 当前价格：${mark_px:,.2f}
- 24小时涨跌：{change_24h:+.1f}%

请生成一条 80-150 字的高吸引力广场短贴，要求：
1. 解释资金费率异常意味着什么（多头/空头过热）
2. 给出你的交易观点（是否可以反向操作）
3. 结尾引导用户点击合约链接
4. 语气真实自然，有个人观点
5. 必须包含这些标签：{futures_tags} #资金费率 #合约交易

禁止使用的词：「值得关注」「不构成投资建议」「仅供参考」
""",

    "OI_SURGE": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，专门研究链上持仓数据。

当前 OI 异动信号：
- 代币：{coin}
- 当前 OI：${oi_usd_m:.1f}M（Hyperliquid 全市场排名 Top）
- 当前价格：${mark_px:,.2f}
- 24小时涨跌：{change_24h:+.1f}%
- 24小时成交量：${day_vol_m:.1f}M

请生成一条 80-150 字的高吸引力广场短贴，要求：
1. 用 OI 数据说明主力资金的动向
2. 给出你对后市的判断
3. 结尾引导用户点击合约链接
4. 语气真实自然
5. 必须包含这些标签：{futures_tags} #OI数据 #合约交易

禁止使用的词：「值得关注」「不构成投资建议」「仅供参考」
""",

    # ── TG 专属模板 ────────────────────────────────────────────────────────────

    "TG_WHALE_LONG": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，专门追踪链上巨鲸动向。

最新链上巨鲸信号（来自 HyperInsight）：
- 代币：{coin}
- 操作：{action}（做多）
- 操作规模：约 ${size_usd_m:.2f}M 美元
- 当前价格：${price:,.2f}
- 当前浮盈：{pnl_pct:+.1f}%
- 巨鲸背景：{note}

请生成一条 80-150 字的高吸引力广场短贴，要求：
1. 开头突出巨鲸的真实操作（加仓/开仓金额）
2. 结合巨鲸背景讲故事，让读者感受到信号的可信度
3. 结尾引导用户点击合约链接
4. 语气真实自然，像真人在说话
5. 必须包含这些标签：{futures_tags} #链上巨鲸 #聪明钱

禁止使用的词：「值得关注」「不构成投资建议」「仅供参考」「据悉」「据了解」
""",

    "TG_WHALE_SHORT": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，专门追踪链上巨鲸动向。

最新链上巨鲸信号（来自 HyperInsight）：
- 代币：{coin}
- 操作：{action}（做空）
- 操作规模：约 ${size_usd_m:.2f}M 美元
- 当前价格：${price:,.2f}
- 当前浮盈：{pnl_pct:+.1f}%
- 巨鲸背景：{note}

请生成一条 80-150 字的高吸引力广场短贴，要求：
1. 开头突出巨鲸的真实做空操作
2. 分析巨鲸为何在此价位做空
3. 结尾引导用户点击合约链接
4. 语气真实自然，有个人观点
5. 必须包含这些标签：{futures_tags} #链上巨鲸 #聪明钱

禁止使用的词：「值得关注」「不构成投资建议」「仅供参考」「据悉」「据了解」
""",

    "TG_OI_SURGE": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，专门研究 OI 和价格异动。

最新币安合约异动信号（来自方程式 OI 监控）：
- 代币：{coin}
- OI 变化：{oi_change_pct:+.1f}%（过去 1 小时）
- 价格变化：{price_change_pct:+.1f}%（过去 1 小时）
- 24 小时涨跌：{h24_change_pct:+.1f}%
- 当前 OI 规模：约 ${oi_usd_m:.1f}M

请生成一条 80-150 字的高吸引力广场短贴，要求：
1. 开头突出 OI 和价格同步暴涨的异常信号
2. 解释这意味着主力在快速建仓
3. 结尾引导用户点击合约链接
4. 语气紧迫真实，让读者感受到机会
5. 必须包含这些标签：{futures_tags} #OI异动 #合约交易

禁止使用的词：「值得关注」「不构成投资建议」「仅供参考」
""",

    "TG_OI_DROP": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，专门研究 OI 和价格异动。

最新币安合约异动信号（来自方程式 OI 监控）：
- 代币：{coin}
- OI 变化：{oi_change_pct:+.1f}%（过去 1 小时）
- 价格变化：{price_change_pct:+.1f}%（过去 1 小时）
- 24 小时涨跌：{h24_change_pct:+.1f}%
- 当前 OI 规模：约 ${oi_usd_m:.1f}M

请生成一条 80-150 字的高吸引力广场短贴，要求：
1. 开头突出 OI 骤降 + 价格下跌的异常信号
2. 解释这意味着主力在快速平仓或做空
3. 结尾引导用户点击合约链接
4. 语气真实自然，有个人观点
5. 必须包含这些标签：{futures_tags} #OI异动 #合约交易

禁止使用的词：「值得关注」「不构成投资建议」「仅供参考」
""",

    "TG_COMBINED": """
你是一个在币安广场上拥有百万粉丝的加密货币 KOL，同时追踪链上巨鲸和合约 OI 数据。

多源信号共振（Hyperliquid 巨鲸 + 币安 OI 双重确认）：
- 代币：{coin}
- 巨鲸信号：{whale_action}（${whale_size_m:.1f}M）
- OI 异动：{oi_change_pct:+.1f}%，价格 {price_change_pct:+.1f}%
- 24 小时涨跌：{h24_change_pct:+.1f}%

请生成一条 80-150 字的高吸引力广场短贴，要求：
1. 开头突出「链上巨鲸 + OI 双重共振」的稀有信号
2. 用数据说明两个信号共同指向同一方向
3. 结尾引导用户点击合约链接
4. 语气真实自然，充满信心
5. 必须包含这些标签：{futures_tags} #链上巨鲸 #OI异动 #聪明钱

禁止使用的词：「值得关注」「不构成投资建议」「仅供参考」
""",
}

# 内容挖矿 CTA 模板（Write to Earn，随机轮换）
CTA_TEMPLATES = [
    "\n\n💡 点击上方 ${cashtag} 标签查看实时行情，直接在广场交易更方便。",
    "\n\n📊 感兴趣的话点击 ${cashtag} 看看实时价格，广场内交易还能给我贡献一点挖矿收益😄",
    "\n\n🔍 点击帖子里的 ${cashtag} 标签可以直接跳转行情页，欢迎交流讨论！",
    "\n\n⚡ 觉得分析有用的话，点击 ${cashtag} 标签看看行情，你的每一笔交易都是对创作者最好的支持。",
    "\n\n🎯 广场内容挖矿进行中——点击 ${cashtag} 标签参与交易，我们一起在链上留下痕迹！",
]


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

def build_content_prompt(signal: dict, cta_index: int = 0) -> dict:
    """
    将聪明钱信号转化为内容层 Prompt
    返回 {prompt, coin, futures_tags, signal_type}
    """
    sig_type = signal["type"]
    coin = signal["coin"]
    data = signal["data"]

    futures_tags = FUTURES_TAG_MAP.get(coin, f"#{coin}USDT #{coin}合约")
    cta = CTA_TEMPLATES[cta_index % len(CTA_TEMPLATES)].format(cashtag=coin)

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
            mark_px=mark_px,
            change_24h=change_24h,
            funding_rate=funding_rate,
            funding_signal=funding_signal,
            futures_tags=futures_tags,
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
            mark_px=mark_px,
            change_24h=change_24h,
            futures_tags=futures_tags,
        )

    elif sig_type == "OI_SURGE":
        mark_px    = data.get("mark_px", 0)
        change_24h = data.get("change_24h", 0)
        oi_usd_m   = data.get("oi_usd", 0) / 1e6
        day_vol_m  = data.get("day_volume", 0) / 1e6

        prompt = PROMPT_TEMPLATES["OI_SURGE"].format(
            coin=coin,
            mark_px=mark_px,
            change_24h=change_24h,
            oi_usd_m=oi_usd_m,
            day_vol_m=day_vol_m,
            futures_tags=futures_tags,
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
            price=price,
            pnl_pct=pnl_pct,
            note=note,
            futures_tags=futures_tags,
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
        )

    else:
        prompt = f"请生成一条关于 {coin} 的高吸引力广场短贴，包含标签 {futures_tags}"

    # 在 Prompt 末尾附加 CTA 指令
    prompt += f"\n\n在短贴末尾加上这条 CTA（原文保留）：{cta}"

    return {
        "prompt":      prompt,
        "coin":        coin,
        "futures_tags": futures_tags,
        "signal_type": sig_type,
        "cta":         cta,
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
