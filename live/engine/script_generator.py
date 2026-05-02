#!/usr/bin/env python3
"""
数字人直播模块 — 话术生成器
功能：根据市场数据自动生成直播话术脚本
包含：开场白、大盘分析、主流币分析、热点推荐、小车引导、结束语
"""
import os
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from openai import OpenAI

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
LIVE_SCRIPT_FILE = DATA_DIR / "live_script.json"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
WRITE_TO_EARN_URL = "https://www.binance.com/zh-CN/square/write-to-earn"
LLM_MODEL      = os.getenv("LLM_MODEL", "gpt-5.5")

# 话术类型
SCRIPT_TYPES = {
    "opening":    "开场白（30秒，欢迎观众，预告今天内容）",
    "overview":   "大盘行情分析（60秒，BTC+ETH走势，市场情绪）",
    "major":      "主流币分析（每个币30秒，价格+信号+操作建议）",
    "trending":   "热点币推荐（每个币20秒，热度原因+期货合约+风险提示）",
    "cart_push":  "内容挖矿引导（20秒，引导读者点击 cashtag 参与交易，触发内容挖矿返佣）",
    "closing":    "结束语（20秒，总结+关注+下播预告）",
}


def _format_price(price: float) -> str:
    """格式化价格显示"""
    if price >= 1000:
        return f"${price:,.0f}"
    elif price >= 1:
        return f"${price:.4f}"
    else:
        return f"${price:.6f}"


def _format_change(pct: float) -> str:
    """格式化涨跌幅"""
    emoji = "📈" if pct >= 0 else "📉"
    return f"{emoji} {pct:+.2f}%"


def generate_opening(market_report: dict) -> str:
    """生成开场白话术"""
    overview = market_report.get("overview", {})
    btc_price = overview.get("btc_price", 0)
    sentiment = overview.get("market_sentiment", "中性")
    trend = overview.get("market_trend", "震荡")

    if OPENAI_API_KEY:
        try:
            client = OpenAI(
                api_key=OPENAI_API_KEY,
                base_url=OPENAI_BASE_URL,
            )
            resp = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{
                    "role": "user",
                    "content": f"""你是一个加密货币直播主播，请生成一段30秒的开场白话术。
当前BTC价格：${btc_price:,.0f}
市场情绪：{sentiment}
市场趋势：{trend}

要求：
- 热情欢迎观众
- 提到今天的市场状态
- 预告今天要分析的内容（大盘、主流币、热点币）
- 提到广场内容挖矿：点击帖子里的 cashtag 参与交易，支持创作者
- 口语化，自然流畅，100字以内"""
                }],
                max_tokens=200,
                temperature=0.9,
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            pass

    # 规则模板
    return (
        f"老铁们好！欢迎来到今天的直播间！"
        f"现在BTC报价{_format_price(btc_price)}，市场整体{trend}，情绪{sentiment}。"
        f"今天我们要分析大盘走势、主流币机会，还有几个热点币值得重点关注！"
        f"点击帖子里的 cashtag 参与交易，支持内容挖矿！我们开始！"
    )


def generate_market_overview_script(market_report: dict) -> str:
    """生成大盘分析话术"""
    overview = market_report.get("overview", {})
    btc_price = overview.get("btc_price", 0)
    btc_change = overview.get("btc_change", 0)
    eth_price = overview.get("eth_price", 0)
    eth_change = overview.get("eth_change", 0)
    sentiment = overview.get("market_sentiment", "中性")
    trend = overview.get("market_trend", "震荡")
    vol = overview.get("btc_volume_b", 0)

    if OPENAI_API_KEY:
        try:
            client = OpenAI(
                api_key=OPENAI_API_KEY,
                base_url=OPENAI_BASE_URL,
            )
            resp = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{
                    "role": "user",
                    "content": f"""请生成一段60秒的大盘行情分析直播话术。
数据：
- BTC: {_format_price(btc_price)} {_format_change(btc_change)}，24h成交量{vol}B美元
- ETH: {_format_price(eth_price)} {_format_change(eth_change)}
- 市场情绪：{sentiment}，趋势：{trend}

要求：
- 先说BTC，再说ETH
- 给出明确的市场判断（不模棱两可）
- 提示关键支撑/压力位（根据价格估算）
- 给出操作建议（轻仓/观望/可以布局）
- 150字以内，口语化"""
                }],
                max_tokens=250,
                temperature=0.85,
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            pass

    # 规则模板
    btc_signal = "可以轻仓做多" if btc_change > 1 else ("观望为主" if abs(btc_change) < 1 else "注意风险")
    return (
        f"来看大盘！BTC现在{_format_price(btc_price)}，24小时{_format_change(btc_change)}，"
        f"成交量{vol}B美元，{'量能充足' if vol > 20 else '量能偏低'}。"
        f"ETH跟随BTC走势，现在{_format_price(eth_price)}，{_format_change(eth_change)}。"
        f"整体市场{sentiment}，{trend}。我的判断是{btc_signal}，仓位控制好！"
    )


def generate_major_coin_script(coin_data: dict) -> str:
    """生成单个主流币分析话术"""
    symbol = coin_data.get("symbol", "BTC")
    price = coin_data.get("price", 0)
    change = coin_data.get("change_pct", 0)
    signal = coin_data.get("signal", "中性")
    action = coin_data.get("action", "观望")
    high = coin_data.get("high", 0)
    low = coin_data.get("low", 0)

    futures = f"{symbol}USDT"

    if OPENAI_API_KEY:
        try:
            client = OpenAI(
                api_key=OPENAI_API_KEY,
                base_url=OPENAI_BASE_URL,
            )
            resp = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{
                    "role": "user",
                    "content": f"""请生成一段30秒的{symbol}分析直播话术。
数据：
- 价格：{_format_price(price)}
- 24h涨跌：{_format_change(change)}
- 24h最高：{_format_price(high)}，最低：{_format_price(low)}
- 信号：{signal}，建议：{action}
- 期货合约：#{futures}

要求：
- 报价格和涨跌
- 给出简单技术判断（支撑/压力）
- 给出操作建议
- 提到期货合约标签 #{futures}
- 80字以内"""
                }],
                max_tokens=150,
                temperature=0.85,
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            pass

    return (
        f"来看{symbol}！现在{_format_price(price)}，{_format_change(change)}。"
        f"24h高点{_format_price(high)}，低点{_format_price(low)}。"
        f"信号{signal}，{action}。合约玩家关注 #{futures}，做好止损！"
    )


def generate_trending_recommendation_script(trending_coins: list) -> str:
    """生成热点币推荐话术"""
    if not trending_coins:
        return "今天热点币暂时没有特别突出的机会，大家继续关注主流币就好。"

    top = trending_coins[0]
    symbol = top.get("symbol", "")
    price = top.get("price", 0)
    change = top.get("change_pct", 0)
    reason = top.get("recommend_reason", "热度较高")
    futures = f"{symbol}USDT"

    others = [c["symbol"] for c in trending_coins[1:3]]
    others_str = "、".join(others) if others else ""

    if OPENAI_API_KEY:
        try:
            client = OpenAI(
                api_key=OPENAI_API_KEY,
                base_url=OPENAI_BASE_URL,
            )
            resp = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{
                    "role": "user",
                    "content": f"""请生成一段热点币推荐直播话术。
今日最热门推荐：{symbol}
- 价格：{_format_price(price)}，{_format_change(change)}
- 推荐原因：{reason}
- 期货合约：#{futures}
其他值得关注：{others_str}

要求：
- 重点介绍{symbol}，说明为什么热
- 提到期货合约 #{futures}
- 必须加风险提示
- 顺带提一下其他热点
- 100字以内，有感染力"""
                }],
                max_tokens=200,
                temperature=0.9,
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            pass

    return (
        f"热点币来了！今天最值得关注的是{symbol}，现在{_format_price(price)}，{_format_change(change)}，"
        f"{reason}。合约玩家关注 #{futures}！"
        f"{'另外' + others_str + '也有机会，大家自己研究一下。' if others_str else ''}"
        f"记住：热点币波动大，仓位要轻，止损要设！"
    )


def generate_cart_push_script(cart_items: list) -> str:
    """生成内容挖矿引导话术（引导读者点击 cashtag 交易）"""
    if not cart_items:
        return (
            f"老铁们！点击帖子里的 $BTC、$ETH 等 cashtag 标签，"
            f"直接跳转行情页参与交易，你的每一笔交易都是对创作者最好的支持！"
            f"每笔交易都省钱！现在点击小车就能看到，不要错过！"
        )

    # 有具体商品时
    items_desc = "、".join([item.get("name", "") for item in cart_items[:3]])
    return (
        f"老铁们注意！直播间小车里有{items_desc}，"
        f"点击帖子里的 cashtag 参与交易，广场内容挖矿最高返佣50%！"
        f"点击下方小车，现在就能领取福利！"
    )


def generate_closing_script(market_report: dict, post_count: int = 0) -> str:
    """生成结束语话术"""
    overview = market_report.get("overview", {})
    btc_price = overview.get("btc_price", 0)

    return (
        f"好了老铁们，今天的直播就到这里！"
        f"BTC收在{_format_price(btc_price)}，记住我们今天的判断。"
        f"{'今天已经发了' + str(post_count) + '篇分析贴，' if post_count else ''}"
        f"关注我不迷路，明天同一时间继续直播！"
        f"有问题在弹幕问，我都会回答。下播！"
    )


def generate_full_live_script(market_report: dict, cart_items: list = None) -> dict:
    """生成完整直播脚本"""
    print("[话术生成器] 正在生成完整直播脚本...")

    majors = market_report.get("major_coins", [])
    trending = market_report.get("trending", [])

    scripts = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
        "opening": generate_opening(market_report),
        "market_overview": generate_market_overview_script(market_report),
        "major_coins": [generate_major_coin_script(c) for c in majors],
        "trending_recommendation": generate_trending_recommendation_script(trending),
        "cart_push": generate_cart_push_script(cart_items or []),
        "closing": generate_closing_script(market_report),
    }

    # 保存脚本
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(LIVE_SCRIPT_FILE, "w", encoding="utf-8") as f:
        json.dump(scripts, f, ensure_ascii=False, indent=2)

    print(f"[话术生成器] 脚本生成完成，共 {len(scripts)} 个段落")
    return scripts


if __name__ == "__main__":
    try:
        from live.engine.market_analyzer import get_full_market_report
    except ModuleNotFoundError:
        from market_analyzer import get_full_market_report

    report = get_full_market_report()
    scripts = generate_full_live_script(report)

    print("\n=== 直播脚本预览 ===\n")
    print(f"【开场白】\n{scripts['opening']}\n")
    print(f"【大盘分析】\n{scripts['market_overview']}\n")
    if scripts["major_coins"]:
        print(f"【BTC分析】\n{scripts['major_coins'][0]}\n")
    print(f"【热点推荐】\n{scripts['trending_recommendation']}\n")
    print(f"【小车引导】\n{scripts['cart_push']}\n")
    print(f"【结束语】\n{scripts['closing']}\n")
