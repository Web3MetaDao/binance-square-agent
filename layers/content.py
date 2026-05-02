"""
内容层（Content Layer）
========================
将感知层输出的市场数据 + 灵魂层的 persona.md 融合，
通过 LLM 生成更像真人交易复盘/计划的短贴内容。
"""

import pathlib
import random
import re
from openai import OpenAI
from typing import Optional

from utils.price_sync import PRICE_FRESHNESS_TTL, is_price_fresh

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    LLM_MODEL, LLM_TEMPERATURE, LLM_MAX_TOKENS,
    PERSONA_FILE, POST_MIN_CHARS, POST_MAX_CHARS,
    OPENAI_API_KEY, OPENAI_BASE_URL,
)

STYLE_POOL = [
    {
        "name": "交易计划型",
        "desc": "先写你看到的盘面变化，再写自己准备怎么做，补一句失效条件，像真人盘前笔记。",
        "hook_example": "{coin} 这里我先不追，除非 4H 重新站回关键位。",
    },
    {
        "name": "复盘纠错型",
        "desc": "从一次没做好的判断切入，但重点放在这次学到什么、这次会怎么应对，少讲戏剧化情绪。",
        "hook_example": "昨晚 {coin} 那段拉升我没跟，问题不在胆小，在于位置根本不舒服。",
    },
    {
        "name": "结构观察型",
        "desc": "直接描述量价、支撑压力、周期结构，让读者一眼看出你在看什么。",
        "hook_example": "{coin} 这段最关键的不是涨没涨，而是这个位置的承接还在不在。",
    },
    {
        "name": "风险收益型",
        "desc": "强调盈亏比和失效条件，告诉读者什么位置值得等，什么位置宁愿错过。",
        "hook_example": "{coin} 这位置不是不能做，只是追进去的盈亏比已经不划算了。",
    },
    {
        "name": "情绪降噪型",
        "desc": "市场越吵越要写得克制，把热度放一边，回到价格、时间和计划本身。",
        "hook_example": "现在聊 {coin} 的人很多，但我真正关心的还是下一根 4H 收在哪里。",
    },
    {
        "name": "多空对照型",
        "desc": "把多空两边的逻辑都摆出来，各给一个触发条件，让读者自己判断哪边更有道理。",
        "hook_example": "{coin} 我现在两边都不站，等一个明确的触发条件出来再说。",
    },
    {
        "name": "量化观察型",
        "desc": "从量能、持仓变化、资金费率等数据切入，用数字说话，少说观点多说事实。",
        "hook_example": "{coin} 这波拉上去的量能和前面那波不一样，你看看成交量就清楚了。",
    },
    {
        "name": "时间维度型",
        "desc": "从不同周期聊矛盾，比如 15m 偏多但 4H 还在压，讲清楚你更看重哪个级别。",
        "hook_example": "{coin} 小级别看着挺好，但切到日线你就知道为什么我还在等。",
    },
]

# 随机种子初始化，让每次进程重启后的风格顺序不一致
random.shuffle(STYLE_POOL)

CTA_POOL = [
    "💡 点下方币种标签看实时行情，也欢迎留言说说你的交易计划。",
]

FIXED_DISCLAIMER = (
    "⚠️免责声明：\n"
    "本文仅为个人行情观点分享，不构成任何投资建议，加密货币市场高波动、高风险，请理性交易、自行承担风险。"
)
FIXED_HASHTAGS = "#交易复盘 #行情分析 #交易计划"

BANNED_PHRASES = [
    "值得关注", "需要注意", "不得不说", "首先", "其次", "综上所述",
    "总的来说", "不可忽视", "值得一提", "毋庸置疑", "显而易见",
    "众所周知", "不言而喻", "有目共睹",
]


class ContentGenerator:
    """LLM 驱动的高转化短贴生成器。"""

    def __init__(self):
        self.client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL)
        self._style_idx = 0
        self._persona_cache = None

    def _load_persona(self) -> str:
        if self._persona_cache:
            return self._persona_cache
        if PERSONA_FILE.exists():
            with open(PERSONA_FILE) as f:
                self._persona_cache = f.read()
        else:
            self._persona_cache = (
                "你是一位长期盯盘的加密交易者，习惯用价格结构、节奏和交易计划来表达观点，"
                "说话直接，但不过度表演情绪，偶尔会复盘自己没做好的地方。"
            )
        return self._persona_cache

    def _next_style(self) -> dict:
        style = STYLE_POOL[self._style_idx % len(STYLE_POOL)]
        self._style_idx += 1
        return style

    def _build_prompt(self, coin_info: dict, context: dict, style: dict) -> str:
        coin = coin_info["coin"]
        futures = coin_info["futures"]
        tier = coin_info["tier"]
        persona = self._load_persona()

        tw_samples = [t["text"][:120] for t in context.get("raw_tweets", []) if coin.upper() in t["text"].upper()][:2]
        sq_samples = [p["title"][:80] for p in context.get("hot_posts", []) if coin.upper() in p["title"].upper()][:2]
        topic_samples = [t["topic"] for t in context.get("topics", [])][:3]

        w2e_data = context.get("w2e_top_creators", {})
        w2e_samples = []
        for creator in w2e_data.get("top_creators", [])[:5]:
            for post_text in creator.get("top_posts", [])[:1]:
                if post_text and len(post_text) > 20:
                    w2e_samples.append(
                        f"[#{creator['rank']} {creator['nickname']} 收益{creator.get('earnings_usdc', 0):.0f}USDC] {post_text[:100]}"
                    )

        context_lines = []
        if tw_samples:
            context_lines.append(f"Twitter KOL 最新观点：{'；'.join(tw_samples)}")
        if sq_samples:
            context_lines.append(f"广场热帖内容：{'；'.join(sq_samples)}")
        if topic_samples:
            context_lines.append(f"当前热门叙事：{'、'.join(topic_samples)}")
        if w2e_samples:
            context_lines.append("本周内容挖矿高收益帖子风格参考（学习风格，不要抄袭）：\n" + "\n".join("  · " + s for s in w2e_samples))

        tier_desc = {"S": "Twitter 和币安广场双端同时爆热", "A": "Twitter 头部 KOL 密集讨论", "B": "币安广场内部热度飙升"}
        hook_hint = style["hook_example"].format(coin=coin)
        banned_str = "、".join(BANNED_PHRASES[:8])
        context_str = "\n".join(context_lines) if context_lines else "（暂无额外上下文）"

        price_line = ""
        mark_px = coin_info.get("mark_px") or coin_info.get("price")
        change_24h = coin_info.get("change_24h")
        high_24h = coin_info.get("high_24h")
        low_24h = coin_info.get("low_24h")
        price_ts = coin_info.get("_price_ts") or coin_info.get("ts")
        has_freshness_contract = (
            coin_info.get("_price_synced")
            or coin_info.get("is_live") is not None
            or price_ts is not None
        )
        price_is_fresh = (
            bool(mark_px)
            and has_freshness_contract
            and coin_info.get("_price_synced") is not False
            and coin_info.get("is_live") is not False
            and is_price_fresh({"ts": price_ts}, max_age=PRICE_FRESHNESS_TTL)
        )
        if price_is_fresh:
            price_line = (
                f"\n币安期货实时行情（必须使用这些真实数据，不能编造）："
                f"\n- {coin} 当前期货价格: ${mark_px:,.4f}"
                + (f"\n- 24h涨跌幅: {change_24h:+.2f}%" if change_24h is not None else "")
                + (f"\n- 24h最高: ${high_24h:,.4f}，最低: ${low_24h:,.4f}" if high_24h and low_24h else "")
            )

        return f"""你的人设背景：
{persona}

当前任务：为币安广场写一条像真人交易复盘/计划的短贴，不要写成营销文，也不要写出明显 AI 套路感。

热点信息：
- 目标代币：{coin}（期货合约：{futures}）
- 热点等级：{tier_desc.get(tier, '热点')}
- 当前市场上下文：
{context_str}{price_line}
写作风格：{style['name']}
风格说明：{style['desc']}
开头参考（可改写，不要照抄）：{hook_hint}

严格要求（违反任何一条则重写）：
1. 正文字数 {POST_MIN_CHARS}~{POST_MAX_CHARS} 字
2. 第一段要像真人正在看盘，允许直接下判断，但不要硬写“强力 Hook”套路
3. 正文中必须包含至少一个具体数字，如果有实时行情则优先使用
4. 优先写具体交易细节：点位、周期、仓位或计划、判断依据、失效条件
5. 少用夸张情绪词和老韭菜人设，语气口语化但克制，像真实交易员复盘
6. 不要为了吸睛硬写夸张亏损、暴富、爆仓故事
7. 不要每条都套用“热点+经历+数字+情绪+悬念”同一结构
8. 结尾可以是自然问句或自然收束，不要强行互动诱导
9. 禁止使用：{banned_str}
10. 禁止任何八股文结构（首先/其次/综上等）
11. 不要输出免责声明、CTA、标签行、话题标签，这些由程序统一追加
12. 不要出现 ${{future}}({futures})、${futures}、#{futures}、#币安广场、#内容挖矿 等模板化痕迹
只输出短贴正文，不要输出任何解释、前缀或引号。"""

    def _canonical_cashtags(self, coin: str) -> list[str]:
        primary = (coin or "BTC").upper()
        tags = []
        for item in [primary, "BSB"]:
            token = f"${item}"
            if token not in tags:
                tags.append(token)
        return tags

    def _live_price_line(self, coin_info: dict) -> str:
        coin = coin_info["coin"].upper()
        mark_px = coin_info.get("mark_px") or coin_info.get("price")
        change_24h = coin_info.get("change_24h")
        high_24h = coin_info.get("high_24h")
        low_24h = coin_info.get("low_24h")
        price_ts = coin_info.get("_price_ts") or coin_info.get("ts")
        has_freshness_contract = (
            coin_info.get("_price_synced") is not None
            or coin_info.get("is_live") is not None
            or price_ts is not None
        )
        if not mark_px:
            return ""
        if has_freshness_contract and (
            coin_info.get("_price_synced") is False
            or coin_info.get("is_live") is False
            or not is_price_fresh({"ts": price_ts}, max_age=PRICE_FRESHNESS_TTL)
        ):
            return ""
        line = f"现在 {coin} 期货最新价 ${mark_px:,.4f}"
        if change_24h is not None:
            line += f"，24h {change_24h:+.2f}%"
        if high_24h and low_24h:
            line += f"，日内区间 ${low_24h:,.4f}-${high_24h:,.4f}。"
        else:
            line += "。"
        return line

    def _strip_template_noise(self, text: str, coin: str, futures: str) -> list[str]:
        cleaned = (text or "").replace("\r", "\n")
        cleaned = re.sub(r"\{future\}\(([A-Z][A-Z0-9]{0,19})USDT\)", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\$([A-Z][A-Z0-9]{0,19})USDT\b", lambda m: f"${m.group(1).upper()}", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"#[A-Z][A-Z0-9]{0,19}USDT\b", "", cleaned, flags=re.IGNORECASE)
        for bad, good in [
            (f"{{future}}({futures})", ""),
            (f"${futures}", f"${coin}"),
            (f"#{futures}", ""),
            ("#币安广场", ""),
            ("#内容挖矿", ""),
            (CTA_POOL[0], ""),
            (FIXED_DISCLAIMER, ""),
        ]:
            cleaned = cleaned.replace(bad, good)
        lines, seen = [], set()
        for line in cleaned.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("⚠️免责声明") or stripped.startswith("本文仅为个人行情观点分享"):
                continue
            if stripped.startswith("#") or stripped.startswith("💡"):
                continue
            key = stripped.lower()
            if key in seen:
                continue
            seen.add(key)
            lines.append(stripped)
        return lines

    def _dedupe_semantic_lines(self, lines: list[str], coin: str) -> list[str]:
        prefix = f"看 {coin.upper()} 这段节奏，"
        seen = set()
        deduped = []
        for line in lines:
            stripped = (line or "").strip()
            if not stripped:
                continue
            canonical = stripped
            if canonical.startswith(prefix):
                canonical = canonical[len(prefix):]
            canonical = re.sub(r"[\s，。！？!?,、：:；;…]+", "", canonical.lower())
            if not canonical or canonical in seen:
                continue
            seen.add(canonical)
            deduped.append(stripped)
        return deduped

    def _has_untrusted_price_claim(self, line: str, trusted_live_line: str = "") -> bool:
        stripped = (line or "").strip()
        trusted = (trusted_live_line or "").strip()
        if not stripped:
            return False
        if trusted and stripped == trusted:
            return False
        return bool(
            re.search(r"\$\s*\d[\d,]*(?:\.\d+)?", stripped)
            or re.search(r"\b\d[\d,]*(?:\.\d+)?\s*(?:美元|美金|u|U|usdt|USDT)\b", stripped)
        )

    def _format_final_post(self, body: str, coin_info: dict, cta: str = "") -> str:
        coin = coin_info["coin"].upper()
        futures = coin_info["futures"].upper()
        lines = self._strip_template_noise(body, coin, futures)
        if not lines:
            lines = [f"{coin} 这段走势别只看情绪", f"${coin} 真正决定方向的还是成交和位置，你怎么看？"]

        title = lines[0]
        content_lines = []
        for line in lines[1:]:
            if line != title:
                content_lines.append(line)
        live_line = self._live_price_line(coin_info)
        if self._has_untrusted_price_claim(title, trusted_live_line=live_line):
            title = ""
        if live_line and live_line not in content_lines:
            content_lines.append(live_line)
        if not any(f"${coin}" in line for line in [title] + content_lines):
            if content_lines:
                content_lines[0] = f"看 {coin} 这段节奏，{content_lines[0]}"
            else:
                content_lines.append(f"看 {coin} 这波节奏还没走完，你怎么看？")

        normalized_content_lines = []
        for line in content_lines:
            stripped = line.strip()
            if stripped == f"${coin}":
                continue
            if stripped.startswith(f"${coin} "):
                stripped = f"{coin}：{stripped[len(coin)+2:]}"
            if self._has_untrusted_price_claim(stripped, trusted_live_line=live_line):
                continue
            normalized_content_lines.append(stripped)
        content_lines = self._dedupe_semantic_lines(normalized_content_lines, coin)
        if not title:
            if content_lines:
                title = content_lines.pop(0)
            else:
                title = f"{coin} 这段走势别只看情绪"

        body_block = "\n".join(content_lines[:3]).strip()
        parts = [
            title,
            body_block,
            " ".join(self._canonical_cashtags(coin)),
            FIXED_HASHTAGS,
            FIXED_DISCLAIMER,
            cta.strip() if cta else "",
        ]
        return "\n\n".join(part for part in parts if part)

    def generate(self, coin_info: dict, context: dict) -> str:
        style = self._next_style()
        try:
            prompt = self._build_prompt(coin_info, context, style)
            # ── 注入近期已发帖子(内容去重提示) ──
            recent_texts = self._load_recent_post_texts()
            if recent_texts:
                prompt = prompt.replace(
                    "只输出短贴正文，不要输出任何解释、前缀或引号。",
                    f"注意避免与以下近期已发帖子的内容重复（包括句式和结构）：\n"
                    + "\n".join(f"- \"{t}\"" for t in recent_texts)
                    + "\n\n只输出短贴正文，不要输出任何解释、前缀或引号。"
                )
            response = self.client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=LLM_MAX_TOKENS,
                temperature=LLM_TEMPERATURE,
            )
            body = response.choices[0].message.content.strip()
        except Exception as e:
            print(f"  [内容层] LLM 调用失败，使用降级模板: {e}")
            body = self._fallback_template(coin_info, style)
        return self._format_final_post(body, coin_info)

    def _load_recent_post_texts(self, max_count: int = 6) -> list[str]:
        """从 agent_state.json 的 post_history 字段加载近期已发帖子正文，
        用于 LLM 提示词中禁止重复句式。"""
        try:
            state_path = DATA_DIR / "agent_state.json"
            if not state_path.exists():
                return []
            import json
            with open(state_path) as f:
                state = json.load(f)
            history = state.get("post_history") or []
            texts = []
            for entry in reversed(history):
                content = (entry.get("content") or "").strip()
                if not content or len(content) < 20:
                    continue
                preview = content[:100].replace("\n", " ")
                texts.append(preview)
                if len(texts) >= max_count:
                    break
            return texts
        except Exception:
            return []

    def _fallback_template(self, coin_info: dict, style: dict) -> str:
        coin = coin_info["coin"]
        tier = coin_info["tier"]
        tier_label = {"S": "双端爆热", "A": "KOL密集讨论", "B": "广场热度飙升"}.get(tier, "热点")
        import random
        _FALLBACK_TEMPLATES = [
            f"{coin} 这轮结构走得还算规矩，高位没追到也不急，等一个回调确认再说。当前 {tier_label}，我更关心量价配合而不是情绪。\n\n"
            f"计划很简单：站稳关键位就加，放量滞涨就减。周期看4H以上，不进场就被动等。如果跌破支撑位那就按纪律走，不扛单。",

            f"{coin} 盘面正在收敛，快出方向了。当前 {tier_label}，但情绪面和中线结构还差一个共振确认。\n\n"
            f"我一般是等二次测试再做决定——第一次突破不跟，等回踩确认结构完整了再说。吃鱼身不吃鱼头鱼尾。失效条件就是结构走坏，那时候再认错也不晚。",

            f"{coin} 这波走得有点急，追高的性价比不高。{tier_label} 行情最忌讳的就是冲动上头。\n\n"
            f"我的仓位计划：底仓不超过3%，止损设在前低下方。如果继续走强再加，但每加一次止损同步上移。拿周期看波段，不奢求吃满。",

            f"{coin} 我盯了几天了，这个位置不上不下，但结构上有点意思。{tier_label} 阶段没必要抢跑。\n\n"
            f"如果先回踩，我会在关键支撑附近挂单接，止损放在结构破坏位下方。如果直接拉，那就等第一波回落确认后再考虑。耐心不等同于犹豫。",

            f"{coin} 短线上多空都有道理，但中线我更偏中性，不急于站队。{tier_label} 不代表趋势就要延续。\n\n"
            f"我的入场条件：价格回到区间下沿 + 出现缩量止跌信号，或者放量突破上沿 + 回踩确认。两种场景提前想好怎么应对，比猜方向更靠得住。",

            f"{coin} 这位置多空博弈很激烈，{tier_label} 环境下很容易被带着走。\n\n"
            f"我给自己定的规矩：不看短时波动、不追脉冲、不扛单。结构破了就反向思考，没破就按原计划执行。判断周期拉长一点，噪音就少一点。",
        ]
        return random.choice(_FALLBACK_TEMPLATES)


# ── 双模式：不走 LLM 的纯模板发帖 ──────────────────────────


def make_data_post(coin: str, price_info: dict) -> str:
    """
    模式A：现货/期货 API 可用 → 纯数据行情简报帖，不走 LLM。

    以格式化模板直接输出精确价格数据。
    """
    px = price_info["price"]
    chg = price_info.get("change_24h", 0)
    hi = price_info.get("high_24h")
    lo = price_info.get("low_24h")
    vol = price_info.get("volume_24h", 0)
    source = price_info.get("_source", "binance")

    body_parts = [f"${coin} 实时行情"]

    arrow = "▲" if chg >= 0 else "▼"
    body_parts.append(f"当前价格：${px:,.2f}（24h {arrow}{abs(chg):.2f}%）")

    if hi and lo:
        body_parts.append(f"24h最高：${hi:,.2f}")
        body_parts.append(f"24h最低：${lo:,.2f}")

    if vol:
        unit = "亿" if vol >= 1e8 else "万"
        val = vol / 1e8 if vol >= 1e8 else vol / 1e4
        body_parts.append(f"24h成交额：{val:.2f}{unit} USDT")

    source = price_info.get("_source", "")
    if "coingecko" in source:
        data_source = "CoinGecko"
    elif "futures" in source:
        data_source = "币安期货"
    elif "spot" in source:
        data_source = "币安现货"
    else:
        data_source = "市场"

    body_parts.append(f"⏱ 数据来源：{data_source}")

    body = "\n".join(body_parts)
    cashtags = f"${coin} $BSB"
    hashtags = "#交易复盘 #行情分析 #交易计划"
    disclaimer = "⚠️免责声明：\n本文仅为个人行情观点分享，不构成任何投资建议，加密货币市场高波动、高风险，请理性交易、自行承担风险。"

    return "\n\n".join([body, cashtags, hashtags, disclaimer])


def make_analysis_post(coin: str, tier: str = "B") -> str:
    """
    模式B：所有价格 API 均不可用 → 纯走势分析帖，不走 LLM。

    仅讨论结构/逻辑/计划，不出任何精确价格数字。
    使用多样性内容池替代硬编码单模板，避免被识别为 AI 生成。
    """
    tier_label = {"S": "双端爆热", "A": "KOL密集讨论", "B": "广场热度飙升"}.get(tier, "热点")

    import random
    _ANALYSIS_TEMPLATES = [
        # 结构型
        f"{coin} 这轮结构走得还算规矩，高位没追到也不急，等一个回调确认再说。当前 {tier_label}，我更关心量价配合而不是情绪。\n\n"
        f"计划很简单：站稳关键位就加，放量滞涨就减。周期看4H以上，不进场就被动等。如果跌破支撑位那就按纪律走，不扛单。",

        f"{coin} 盘面正在收敛，快出方向了。当前 {tier_label}，但情绪面和中线结构还差一个共振确认。\n\n"
        f"我一般是等二次测试再做决定——第一次突破不跟，等回踩确认结构完整了再说。吃鱼身不吃鱼头鱼尾。失效条件就是结构走坏，那时候再认错也不晚。",

        f"{coin} 这波走得有点急，追高的性价比不高。{tier_label} 行情最忌讳的就是冲动上头。\n\n"
        f"我的仓位计划：底仓不超过3%，止损设在前低下方。如果继续走强再加，但每加一次止损同步上移。拿周期看波段，不奢求吃满。",

        f"{coin} 这段走势让我想起之前几次类似的结构演变。{tier_label} 之下，多空都有理由。\n\n"
        f"判断依据很简单：看当前位置是否能形成有效支撑，如果反复试探不破，那我倾向顺着大方向做。失效条件是放量破位，那时候不管盈亏都走。",

        f"{coin} 我盯了几天了，这个位置不上不下，但结构上有点意思。{tier_label} 阶段没必要抢跑。\n\n"
        f"如果先回踩，我会在关键支撑附近挂单接，止损放在结构破坏位下方。如果直接拉，那就等第一波回落确认后再考虑。耐心不等同于犹豫。",

        # 时间维度型
        f"{coin} 最近一段时间走势比较磨人，但结构框架其实挺清楚的。上方阻力位多次测试不过，下方支撑也在慢慢上移。\n\n"
        f"等一个放量突破或者缩量回踩。现在追进去胜率不高，不如等结构确认再说。计划做出来了，执行看纪律。",

        f"{coin} 短线上多空都有道理，但中线我更偏中性，不急于站队。{tier_label} 不代表趋势就要延续。\n\n"
        f"我的入场条件：价格回到区间下沿 + 出现缩量止跌信号，或者放量突破上沿 + 回踩确认。两种场景提前想好怎么应对，比猜方向更靠得住。",

        # 情绪+结构型
        f"{coin} 这位置多空博弈很激烈，{tier_label} 环境下很容易被带着走。\n\n"
        f"我给自己定的规矩：不看短时波动、不追脉冲、不扛单。结构破了就反向思考，没破就按原计划执行。判断周期拉长一点，噪音就少一点。",

        f"{coin} 盘面给了我一个相对清晰的信号——结构在收敛，但还没到动手的时候。{tier_label} 行情缺的不是机会，是确认。\n\n"
        f"等一个明确的进场条件触发：要么等放量突破后回踩确认，要么等缩量回调到结构支撑位。两个条件都不满足就继续等，反正资金在自己手里。",
    ]
    body = random.choice(_ANALYSIS_TEMPLATES)
    cashtags = f"${coin} $BSB"
    hashtags = "#交易复盘 #行情分析 #交易计划"
    disclaimer = "⚠️免责声明：\n本文仅为个人行情观点分享，不构成任何投资建议，加密货币市场高波动、高风险，请理性交易、自行承担风险。"

    return "\n\n".join([body, cashtags, hashtags, disclaimer])

    def generate_from_smart_money_prompt(self, coin_info: dict, sm_prompt: str, cta: str = "") -> str:
        try:
            response = self.client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": sm_prompt}],
                max_tokens=LLM_MAX_TOKENS,
                temperature=LLM_TEMPERATURE,
            )
            body = response.choices[0].message.content.strip()
        except Exception as e:
            print(f"  [内容层-聪明钱] LLM 调用失败，使用降级模板: {e}")
            body = self._sm_fallback_template(coin_info)
        return self._format_final_post(body, coin_info, cta=cta)

    def _sm_fallback_template(self, coin_info: dict) -> str:
        coin = coin_info["coin"]
        direction = coin_info.get("net_direction", "NEUTRAL")
        whale_count = coin_info.get("whale_count", 0)
        long_ratio = coin_info.get("long_ratio", 50)
        live_line = self._live_price_line(coin_info)
        if direction == "LONG":
            direction_cn, emoji = "做多", "🟢"
        elif direction == "SHORT":
            direction_cn, emoji = "做空", "🔴"
        else:
            direction_cn, emoji = "观望", "⚪"
        lines = [
            f"{emoji} 链上聪明钱信号出现！",
            f"Hyperliquid 排行榜 {whale_count} 个顶级大户中，{long_ratio:.0f}% 正在{direction_cn} ${coin}。",
        ]
        if live_line:
            lines.append(f"{live_line}大资金已经表态，你怎么看？")
        else:
            lines.append("大资金已经表态，你怎么看？")
        return "\n".join(lines)
