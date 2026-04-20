#!/usr/bin/env python3
"""
聪明钱信号 → 内容层适配器
将链上聪明钱信号转化为高转化率的短贴素材

核心逻辑：
1. 读取聪明钱信号（smart_money_signal.json）
2. 识别最强信号（HIGH confidence）
3. 生成对应的短贴 Prompt，注入期货合约标签和返佣 CTA
4. 输出给内容层（content.py）使用
"""

import json
import os
from typing import Optional

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
SMART_MONEY_CACHE = os.path.join(DATA_DIR, "smart_money_signal.json")

# 代币 → 期货合约标签映射
FUTURES_TAG_MAP = {
    "BTC": "#BTCUSDT #BTC合约",
    "ETH": "#ETHUSDT #ETH合约",
    "SOL": "#SOLUSDT #SOL合约",
    "BNB": "#BNBUSDT #BNB合约",
    "XRP": "#XRPUSDT #XRP合约",
    "DOGE": "#DOGEUSDT #DOGE合约",
    "ADA": "#ADAUSDT #ADA合约",
    "AVAX": "#AVAXUSDT #AVAX合约",
    "LINK": "#LINKUSDT #LINK合约",
    "DOT": "#DOTUSDT #DOT合约",
    "MATIC": "#MATICUSDT #MATIC合约",
    "UNI": "#UNIUSDT #UNI合约",
    "AAVE": "#AAVEUSDT #AAVE合约",
    "OP": "#OPUSDT #OP合约",
    "ARB": "#ARBUSDT #ARB合约",
    "SUI": "#SUIUSDT #SUI合约",
    "APT": "#APTUSDT #APT合约",
    "INJ": "#INJUSDT #INJ合约",
    "TIA": "#TIAUSDT #TIA合约",
    "HYPE": "#HYPEUSDT #HYPE合约",
    "WIF": "#WIFUSDT #WIF合约",
    "PEPE": "#PEPEUSDT #PEPE合约",
    "BONK": "#BONKUSDT #BONK合约",
    "MON": "#MONUSDT #MON合约",
    "ZEC": "#ZECUSDT #ZEC合约",
}

# 短贴 Prompt 模板（按信号类型分类）
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
}

# 返佣 CTA 模板（随机轮换）
CTA_TEMPLATES = [
    "\n\n👉 看好这波行情？点击下方链接开通币安合约，享高达20%手续费返佣 → {referral_link}",
    "\n\n💰 想跟单这波机会？用我的专属链接注册币安，手续费立减20% → {referral_link}",
    "\n\n🔥 这波行情来了！点击链接开通合约账户，新用户专属返佣福利 → {referral_link}",
    "\n\n⚡ 机会稍纵即逝！点击下方链接，用我的邀请码开通合约，享专属返佣 → {referral_link}",
    "\n\n📈 跟着聪明钱走！注册币安合约，我的专属邀请码帮你省手续费 → {referral_link}",
]


def get_top_signal() -> Optional[dict]:
    """获取当前最强的聪明钱信号"""
    if not os.path.exists(SMART_MONEY_CACHE):
        return None
    try:
        with open(SMART_MONEY_CACHE, "r", encoding="utf-8") as f:
            data = json.load(f)

        # 优先返回 HIGH confidence 信号
        for sig in data.get("top_signals", []):
            if sig.get("confidence") == "HIGH":
                return sig

        # 其次返回 MEDIUM confidence
        for sig in data.get("top_signals", []):
            if sig.get("confidence") == "MEDIUM":
                return sig

        return None
    except Exception:
        return None


def get_all_signals() -> list:
    """获取所有有效信号，按优先级排序"""
    if not os.path.exists(SMART_MONEY_CACHE):
        return []
    try:
        with open(SMART_MONEY_CACHE, "r", encoding="utf-8") as f:
            data = json.load(f)

        signals = []

        # 聪明钱持仓信号
        for sig in data.get("top_signals", []):
            if sig.get("confidence") in ["HIGH", "MEDIUM"]:
                signals.append({
                    "type": "LONG_HIGH" if sig["net_direction"] == "LONG" else "SHORT_HIGH",
                    "coin": sig["coin"],
                    "data": sig,
                    "priority": 1 if sig["confidence"] == "HIGH" else 2,
                })

        # 资金费率异常信号
        for fs in data.get("funding_rate_signals", []):
            if abs(fs.get("funding_rate", 0)) > 0.02:
                signals.append({
                    "type": "FUNDING_EXTREME",
                    "coin": fs["coin"],
                    "data": fs,
                    "priority": 2,
                })

        # OI 异动信号
        for c in data.get("market_overview", {}).get("hot_coins_24h", []):
            if abs(c.get("change_24h", 0)) > 8:
                signals.append({
                    "type": "OI_SURGE",
                    "coin": c["coin"],
                    "data": c,
                    "priority": 3,
                })

        # 按优先级排序
        signals.sort(key=lambda x: x["priority"])
        return signals

    except Exception:
        return []


def build_content_prompt(signal: dict, referral_link: str = "https://www.binance.com/referral/xxx",
                         cta_index: int = 0) -> dict:
    """
    将聪明钱信号转化为内容层 Prompt
    返回 {prompt, coin, futures_tags, signal_type}
    """
    sig_type = signal["type"]
    coin = signal["coin"]
    data = signal["data"]

    futures_tags = FUTURES_TAG_MAP.get(coin, f"#{coin}USDT #{coin}合约")
    cta = CTA_TEMPLATES[cta_index % len(CTA_TEMPLATES)].format(referral_link=referral_link)

    if sig_type in ["LONG_HIGH", "SHORT_HIGH"]:
        total_count = data.get("whale_count", 0)
        long_count = data.get("long_count", data.get("whale_count", 1))
        short_count = total_count - long_count
        long_ratio = data.get("long_ratio", 50)
        short_ratio = 100 - long_ratio
        total_size_m = data.get("total_size_usd", 0) / 1e6
        mark_px = data.get("mark_px", 0)
        change_24h = data.get("change_24h", 0)
        funding_rate = data.get("funding_rate", 0)
        funding_signal = "多头付费" if funding_rate > 0 else "空头付费"

        template = PROMPT_TEMPLATES[sig_type]
        prompt = template.format(
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
        funding_rate = data.get("funding_rate", 0)
        funding_signal = "多头过热" if funding_rate > 0 else "空头过热"
        mark_px = data.get("mark_px", 0)
        change_24h = data.get("change_24h", 0)

        template = PROMPT_TEMPLATES["FUNDING_EXTREME"]
        prompt = template.format(
            coin=coin,
            funding_rate=funding_rate,
            funding_signal=funding_signal,
            mark_px=mark_px,
            change_24h=change_24h,
            futures_tags=futures_tags,
        )

    elif sig_type == "OI_SURGE":
        mark_px = data.get("mark_px", 0)
        change_24h = data.get("change_24h", 0)
        oi_usd_m = data.get("oi_usd", 0) / 1e6
        day_vol_m = data.get("day_volume", 0) / 1e6

        template = PROMPT_TEMPLATES["OI_SURGE"]
        prompt = template.format(
            coin=coin,
            mark_px=mark_px,
            change_24h=change_24h,
            oi_usd_m=oi_usd_m,
            day_vol_m=day_vol_m,
            futures_tags=futures_tags,
        )
    else:
        prompt = f"请生成一条关于 {coin} 的高吸引力广场短贴，包含标签 {futures_tags}"

    # 在 Prompt 末尾附加 CTA 指令
    prompt += f"\n\n在短贴末尾加上这条 CTA（原文保留）：{cta}"

    return {
        "prompt": prompt,
        "coin": coin,
        "futures_tags": futures_tags,
        "signal_type": sig_type,
        "cta": cta,
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
    # 测试适配器
    print("🧪 测试聪明钱信号适配器...")

    signals = get_all_signals()
    if signals:
        print(f"✅ 获取到 {len(signals)} 个信号")
        for i, sig in enumerate(signals[:3]):
            print(f"\n信号 {i+1}: {sig['type']} - {sig['coin']}")
            content = build_content_prompt(sig, referral_link="https://www.binance.com/referral/test123")
            print(f"Prompt 长度: {len(content['prompt'])} 字符")
            print(f"期货标签: {content['futures_tags']}")
            print(f"Prompt 预览:\n{content['prompt'][:300]}...")
    else:
        print("⚠️  暂无聪明钱信号，请先运行 smart_money_monitor.py")
