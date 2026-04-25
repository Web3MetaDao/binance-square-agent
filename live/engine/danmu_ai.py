#!/usr/bin/env python3
"""
数字人直播模块 — 弹幕问答 AI 引擎
功能：实时读取直播弹幕，用 LLM 生成加密货币专业回答
支持：币价查询、行情分析、合约建议、内容挖矿介绍
"""
import os
import json
import time
import re
from typing import Optional
from openai import OpenAI

# ── 配置 ──────────────────────────────────────────────────
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
WRITE_TO_EARN_URL = "https://www.binance.com/zh-CN/square/write-to-earn"
LLM_MODEL       = os.getenv("LLM_MODEL", "gpt-5.5")

# 弹幕问题分类关键词
PRICE_KEYWORDS   = ["价格", "多少钱", "现在多少", "币价", "price", "涨了", "跌了"]
ANALYSIS_KEYWORDS = ["分析", "行情", "走势", "看法", "预测", "判断", "怎么看", "能涨"]
CONTRACT_KEYWORDS = ["合约", "做多", "做空", "期货", "杠杆", "爆仓", "开仓", "平仓"]
EARN_KEYWORDS     = ["内容挖矿", "写作赚钱", "广场收益", "cashtag", "返佣", "怎么赚"]
GENERAL_KEYWORDS  = ["什么", "为什么", "怎么", "如何", "介绍", "解释", "是什么"]

# 直播主播人设（从 persona.md 加载）
PERSONA_PROMPT = """你是一位专业的加密货币直播主播，有5年以上的交易经验。
你的风格：
- 说话简洁有力，不废话，每次回答控制在80字以内
- 敢于给出明确的观点，不模棱两可
- 对新手友好，用大白话解释复杂概念
- 适时介绍币安广场内容挖矿（Write to Earn），引导用户点击帖子中的 cashtag 参与交易
- 每次提到合约时，必须提醒风险控制
你的口头禅：
- "这波行情我觉得..."
- "老铁注意了..."
- "风险控制第一，收益第二"
"""


def classify_danmu(text: str) -> str:
    """对弹幕内容进行分类"""
    text_lower = text.lower()
    if any(k in text_lower for k in PRICE_KEYWORDS):
        return "price_query"
    if any(k in text_lower for k in ANALYSIS_KEYWORDS):
        return "analysis"
    if any(k in text_lower for k in CONTRACT_KEYWORDS):
        return "contract"
    if any(k in text_lower for k in REFERRAL_KEYWORDS):
        return "earn_info"
    return "general"


def extract_coin_from_text(text: str) -> Optional[str]:
    """从弹幕文本中提取提到的币种"""
    coin_map = {
        "比特币": "BTC", "btc": "BTC", "BTC": "BTC",
        "以太坊": "ETH", "eth": "ETH", "ETH": "ETH",
        "bnb": "BNB", "BNB": "BNB", "币安币": "BNB",
        "sol": "SOL", "SOL": "SOL", "solana": "SOL",
        "xrp": "XRP", "XRP": "XRP", "瑞波": "XRP",
        "doge": "DOGE", "DOGE": "DOGE", "狗狗币": "DOGE",
        "pepe": "PEPE", "PEPE": "PEPE",
        "op": "OP", "OP": "OP", "optimism": "OP",
        "arb": "ARB", "ARB": "ARB", "arbitrum": "ARB",
        "sui": "SUI", "SUI": "SUI",
        "not": "NOT", "NOT": "NOT",
    }
    text_lower = text.lower()
    for key, coin in coin_map.items():
        if key.lower() in text_lower:
            return coin
    return None


def get_coin_price_quick(coin: str) -> Optional[float]:
    """快速获取币价（用于弹幕实时回复）"""
    import requests
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": f"{coin}USDT"},
            timeout=5
        )
        if r.status_code == 200:
            return float(r.json()["price"])
    except Exception:
        pass
    return None


def generate_danmu_reply(
    danmu_text: str,
    username: str,
    market_report: dict,
) -> str:
    """
    用 LLM 生成弹幕回复
    :param danmu_text: 弹幕内容
    :param username: 发弹幕的用户名
    :param market_report: 当前市场报告
    :return: 主播回复文本（供 TTS 朗读）
    """
    if not OPENAI_API_KEY:
        return _fallback_reply(danmu_text, username, market_report)

    category = classify_danmu(danmu_text)
    coin = extract_coin_from_text(danmu_text)

    # 构建上下文
    overview = market_report.get("overview", {})
    context_parts = [
        f"当前BTC价格: ${overview.get('btc_price', 0):,.0f}",
        f"市场情绪: {overview.get('market_sentiment', '中性')}",
        f"BTC 24h涨跌: {overview.get('btc_change', 0):+.2f}%",
    ]

    if coin:
        price = get_coin_price_quick(coin)
        if price:
            context_parts.append(f"{coin}当前价格: ${price:,.4f}")

    if category == "earn_info":
        context_parts.append(f"内容挖矿说明: 点击帖子中的 cashtag（如 $BTC）后交易即可给创作者贡献挖矿收益，基础返佣20%，周榜Top30最高50%")

    context = "\n".join(context_parts)

    system_prompt = f"""{PERSONA_PROMPT}

当前市场数据：
{context}

内容挖矿说明（需要时介绍）：点击帖子中的 $BTC/$ETH 等 cashtag 后交易，即可给创作者贡献挖矿收益

回复规则：
1. 直接称呼用户名 "{username}"
2. 回答控制在60-100字
3. 如果问合约，必须加一句风险提示
4. 如果问内容挖矿/广场收益，介绍 cashtag 机制：点击帖子里的 $BTC 等标签后交易，创作者可获得手续费分成（最高50%）
5. 语气自然，像真人主播说话
"""

    try:
        client = OpenAI(
            api_key=OPENAI_API_KEY,
            base_url=OPENAI_BASE_URL,
        )
        resp = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"弹幕内容：{danmu_text}"}
            ],
            max_tokens=150,
            temperature=0.8,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return _fallback_reply(danmu_text, username, market_report)


def _fallback_reply(danmu_text: str, username: str, market_report: dict) -> str:
    """无 LLM 时的规则回复（保底方案）"""
    category = classify_danmu(danmu_text)
    coin = extract_coin_from_text(danmu_text)
    overview = market_report.get("overview", {})

    if category == "price_query" and coin:
        price = get_coin_price_quick(coin)
        if price:
            return f"{username} 问得好！{coin} 现在 ${price:,.4f}，今天市场{overview.get('market_trend', '震荡')}，注意风控！"
        return f"{username}，{coin} 价格稍后播报，关注直播不错过！"

    if category == "analysis":
        return f"{username}，目前市场{overview.get('market_sentiment', '中性')}，BTC ${overview.get('btc_price', 0):,.0f}，{overview.get('market_trend', '震荡')}，具体分析稍后详细讲！"

    if category == "contract":
        return f"{username}，合约高风险！建议仓位不超过总资金20%，做好止损。点击帖子里的 cashtag 直接查看行情！"

    if category == "earn_info":
        return f"{username}，广场内容挖矿很简单：点击帖子里的 $BTC/$ETH 等 cashtag 后交易，就能给创作者贡献挖矿收益，创作者基础返佣20%，周榜Top30最高50%！"

    return f"{username}，好问题！这个话题我们直播中会详细讲，关注不迷路！"


class DanmuQueue:
    """弹幕队列管理器（模拟直播弹幕流）"""

    def __init__(self):
        self._queue = []
        self._processed = set()

    def add(self, username: str, text: str):
        """添加弹幕到队列"""
        msg_id = f"{username}:{text}:{time.time()}"
        if msg_id not in self._processed:
            self._queue.append({"id": msg_id, "username": username, "text": text, "ts": time.time()})

    def get_next(self) -> Optional[dict]:
        """获取下一条待处理弹幕"""
        if self._queue:
            msg = self._queue.pop(0)
            self._processed.add(msg["id"])
            return msg
        return None

    def size(self) -> int:
        return len(self._queue)


# ── 模拟弹幕数据（测试用）────────────────────────────────
MOCK_DANMUS = [
    ("老铁666", "BTC现在多少钱？"),
    ("币圈新手", "以太坊能涨到5000吗？"),
    ("合约大佬", "SOL合约怎么操作？"),
    ("想赚钱的", "广场内容挖矿怎么玩？"),
    ("韭菜本菜", "现在行情怎么样？"),
    ("DOGE粉丝", "狗狗币还能买吗？"),
    ("量化交易员", "BTC今天走势分析一下"),
    ("新人小白", "怎么注册币安？"),
]


if __name__ == "__main__":
    from market_analyzer import get_full_market_report
    report = get_full_market_report()

    print("\n=== 弹幕问答 AI 测试 ===\n")
    for username, text in MOCK_DANMUS[:4]:
        print(f"[弹幕] {username}: {text}")
        reply = generate_danmu_reply(text, username, report)
        print(f"[主播] {reply}")
        print()
        time.sleep(1)
