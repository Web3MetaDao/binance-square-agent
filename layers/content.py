"""
内容层（Content Layer）
========================
将感知层输出的市场数据 + 灵魂层的 persona.md 融合，
通过 LLM 生成高转化率的短贴内容。

核心特性：
  - 5 种写作风格轮换（防内容同质化）
  - 5 种返佣 CTA 模板轮换
  - 强制 Hook + 数据支撑 + 互动问句结构
  - 自动植入期货合约标签（#BTCUSDT）
  - 反八股文 Prompt 约束
  - 降级模板（LLM 失败时兜底）
"""

import time
import pathlib
from openai import OpenAI

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    LLM_MODEL, LLM_TEMPERATURE, LLM_MAX_TOKENS,
    PERSONA_FILE, POST_MIN_CHARS, POST_MAX_CHARS,
    OPENAI_API_KEY, OPENAI_BASE_URL,
    WRITE_TO_EARN_URL,
)

# ──────────────────────────────────────────────
# 写作风格定义
# ──────────────────────────────────────────────
STYLE_POOL = [
    {
        "name": "犀利预判型",
        "desc": "用强烈的个人判断开头，给出具体价格目标或时间节点，语气笃定、有争议性",
        "hook_example": "我敢打赌，接下来72小时{coin}会有大动作。",
    },
    {
        "name": "数据驱动型",
        "desc": "用一个让人意外的链上数据或历史数据开头，引发好奇心，然后给出自己的解读",
        "hook_example": "刚看到一个数据，{coin}的鲸鱼地址在过去24小时净买入了X亿美元。",
    },
    {
        "name": "情绪共鸣型",
        "desc": "用一个真实的亏损或错过机会的故事开头，引发共情，然后转向当前机会",
        "hook_example": "上次{coin}这个信号出现时，我没上车，亏了一套房。",
    },
    {
        "name": "反向思维型",
        "desc": "用一个反直觉的观点开头，挑战市场主流看法，引发争论",
        "hook_example": "所有人都在看涨{coin}，但我要说一个没人想听的真相。",
    },
    {
        "name": "紧迫感营造型",
        "desc": "制造时间紧迫感，暗示窗口期即将关闭，让读者产生FOMO情绪",
        "hook_example": "{coin}的这个机会窗口可能只剩下不到48小时。",
    },
]

# ──────────────────────────────────────────────
# 内容挖矿 CTA 模板池（Write to Earn）
# 核心逻辑：引导读者点击帖子中的 cashtag（$BTC 等）后进行交易
# 读者点击 cashtag → 完成交易 → 创作者获得手续费返佣（最高 50%）
# ──────────────────────────────────────────────
CTA_POOL = [
    "💡 点击上方 ${{coin}} 标签查看实时行情，直接在广场交易更方便。",
    "📊 感兴趣的话点击 ${{coin}} 看看实时价格，广场内交易还能给我贡献一点挖矿收益😄",
    "🔍 点击帖子里的 ${{coin}} 标签可以直接跳转行情页，欢迎交流讨论！",
    "⚡ 觉得分析有用的话，点击 ${{coin}} 标签看看行情，你的每一笔交易都是对创作者最好的支持。",
    "🎯 广场内容挖矿进行中——点击 ${{coin}} 标签参与交易，我们一起在链上留下痕迹！",
]

# 禁用词（反八股文）
BANNED_PHRASES = [
    "值得关注", "需要注意", "不得不说", "首先", "其次", "综上所述",
    "总的来说", "不可忽视", "值得一提", "毋庸置疑", "显而易见",
    "众所周知", "不言而喻", "有目共睹",
]


class ContentGenerator:
    """
    LLM 驱动的高转化短贴生成器。
    每次调用 generate() 返回一条完整的短贴（含标签和CTA）。
    """

    def __init__(self):
        self.client = OpenAI(
            api_key=OPENAI_API_KEY,
            base_url=OPENAI_BASE_URL,
        )
        self._style_idx = 0
        self._cta_idx = 0
        self._persona_cache = None

    def _load_persona(self) -> str:
        """加载用户人设文件，若不存在则返回默认人设。"""
        if self._persona_cache:
            return self._persona_cache
        if PERSONA_FILE.exists():
            with open(PERSONA_FILE) as f:
                self._persona_cache = f.read()
        else:
            self._persona_cache = (
                "你是一位在加密货币市场摸爬滚打多年的老韭菜，"
                "经历过牛熊轮回，有自己独到的市场判断，说话直接、不废话，"
                "偶尔会分享真实的亏损经历，让人觉得真实可信。"
            )
        return self._persona_cache

    def _next_style(self) -> dict:
        style = STYLE_POOL[self._style_idx % len(STYLE_POOL)]
        self._style_idx += 1
        return style

    def _next_cta(self, coin: str = "") -> str:
        cta = CTA_POOL[self._cta_idx % len(CTA_POOL)]
        self._cta_idx += 1
        return cta.format(coin=coin) if "{coin}" in cta else cta

    def _build_prompt(self, coin_info: dict, context: dict, style: dict) -> str:
        coin    = coin_info["coin"]
        futures = coin_info["futures"]
        tier    = coin_info["tier"]
        persona = self._load_persona()

        # 提取相关推文与广场帖子作为上下文
        tw_samples = [
            t["text"][:120]
            for t in context.get("raw_tweets", [])
            if coin.upper() in t["text"].upper()
        ][:2]
        sq_samples = [
            p["title"][:80]
            for p in context.get("hot_posts", [])
            if coin.upper() in p["title"].upper()
        ][:2]
        topic_samples = [
            t["topic"]
            for t in context.get("topics", [])
        ][:3]

        # 提取 Write to Earn 排行榜博主的优质帖子作为写作参考
        w2e_data = context.get("w2e_top_creators", {})
        w2e_samples = []
        for creator in w2e_data.get("top_creators", [])[:5]:
            for post_text in creator.get("top_posts", [])[:1]:
                if post_text and len(post_text) > 20:
                    w2e_samples.append(
                        f"[#{creator['rank']} {creator['nickname']} "
                        f"收益{creator.get('earnings_usdc', 0):.0f}USDC] {post_text[:100]}"
                    )

        context_lines = []
        if tw_samples:
            context_lines.append(f"Twitter KOL 最新观点：{'；'.join(tw_samples)}")
        if sq_samples:
            context_lines.append(f"广场热帖内容：{'；'.join(sq_samples)}")
        if topic_samples:
            context_lines.append(f"当前热门叙事：{'、'.join(topic_samples)}")
        if w2e_samples:
            w2e_header = "本周内容挖矿收益前5博主的高收益帖子风格参考（学习其写作风格，不要抄袭）："
            w2e_body = chr(10).join("  · " + s for s in w2e_samples)
            context_lines.append(w2e_header + chr(10) + w2e_body)
        tier_desc = {
            "S": "Twitter 和币安广场双端同时爆热（最高优先级）",
            "A": "Twitter 头部 KOL 密集讨论",
            "B": "币安广场内部热度飙升",
        }

        hook_hint = style["hook_example"].format(coin=coin)
        banned_str = "、".join(BANNED_PHRASES[:8])
        context_str = "\n".join(context_lines) if context_lines else "（暂无额外上下文）"

        return f"""你的人设背景：
{persona}

当前任务：为币安广场写一条高转化率的短贴。

热点信息：
- 目标代币：{coin}（期货合约：{futures}）
- 热点等级：{tier_desc.get(tier, '热点')}
- 当前市场上下文：
{context_str}

写作风格：{style['name']}
风格说明：{style['desc']}
开头参考（可改写，不要照抄）：{hook_hint}

严格要求（违反任何一条则重写）：
1. 正文字数 {POST_MIN_CHARS}~{POST_MAX_CHARS} 字（不含标签行和CTA）
2. 第一句话必须是强力 Hook，让人忍不住继续读
3. 正文中必须包含至少一个具体数字（价格、涨跌幅、时间等）
4. 结尾必须是一个引导互动的问句（如"你怎么看？"、"你上车了吗？"）
5. 语气口语化、像真人说话，绝对禁止使用：{banned_str}
6. 禁止任何八股文结构（首先/其次/综上等）
7. 最后一行必须是标签：#{coin} #{futures} #币安广场 #合约交易

只输出短贴正文（含最后的标签行），不要输出任何解释、前缀或引号。"""

    def generate(self, coin_info: dict, context: dict) -> str:
        """
        生成一条完整短贴（正文 + 标签 + CTA）。
        失败时自动降级到模板内容。
        """
        style = self._next_style()
        coin  = coin_info["coin"]
        cta   = self._next_cta(coin=coin)
        futures = coin_info["futures"]

        try:
            prompt = self._build_prompt(coin_info, context, style)
            response = self.client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=LLM_MAX_TOKENS,
                temperature=LLM_TEMPERATURE,
            )
            body = response.choices[0].message.content.strip()

            # 确保标签存在（兜底补全）
            if f"#{coin}" not in body:
                body += f"\n#{coin} #{futures} #币安广场 #合约交易"
            if f"#{futures}" not in body:
                body = body.replace(f"#{coin}", f"#{coin} #{futures}")

        except Exception as e:
            print(f"  [内容层] LLM 调用失败，使用降级模板: {e}")
            body = self._fallback_template(coin_info, style)

        full_post = f"{body}\n\n{cta}"
        return full_post

    def _fallback_template(self, coin_info: dict, style: dict) -> str:
        """LLM 失败时的降级内容模板。"""
        coin    = coin_info["coin"]
        futures = coin_info["futures"]
        tier    = coin_info["tier"]
        tier_label = {"S": "双端爆热", "A": "KOL密集讨论", "B": "广场热度飙升"}.get(tier, "热点")
        return (
            f"🚨 ${coin} 正在成为全场最热话题（{tier_label}）！\n"
            f"这种信号历史上出现几次都是大行情前兆，"
            f"我看短期内 ${coin} 有机会突破关键压力位。\n"
            f"你现在的仓位准备好了吗？\n\n"
            f"#{coin} #{futures} #币安广场 #合约交易"
        )

    def generate_from_smart_money_prompt(
        self,
        coin_info: dict,
        sm_prompt: str,
        cta: str,
    ) -> str:
        """
        使用聪明钱专属 Prompt 生成短贴。
        直接使用 signal_to_content 构建的 Prompt，绕过热点模式的 _build_prompt。
        失败时自动降级到聪明钱模板内容。
        """
        coin    = coin_info["coin"]
        futures = coin_info["futures"]

        try:
            response = self.client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": sm_prompt}],
                max_tokens=LLM_MAX_TOKENS,
                temperature=LLM_TEMPERATURE,
            )
            body = response.choices[0].message.content.strip()

            # 确保标签存在（兜底补全）
            if f"#{coin}" not in body:
                body += f"\n#{coin} #{futures} #链上数据 #聪明钱"
            if f"#{futures}" not in body:
                body = body.replace(f"#{coin}", f"#{coin} #{futures}")

        except Exception as e:
            print(f"  [内容层-聪明钱] LLM 调用失败，使用降级模板: {e}")
            body = self._sm_fallback_template(coin_info)

        # 检查 body 是否已包含 cta（Prompt 末尾已含 CTA 指令，LLM 可能已输出）
        if cta.strip() and cta.strip() in body:
            full_post = body  # CTA 已在 body 中，不重复追加
        else:
            full_post = f"{body}\n\n{cta}"
        return full_post

    def _sm_fallback_template(self, coin_info: dict) -> str:
        """聪明钱模式 LLM 失败时的降级内容模板。"""
        coin        = coin_info["coin"]
        futures     = coin_info["futures"]
        direction   = coin_info.get("net_direction", "NEUTRAL")
        whale_count = coin_info.get("whale_count", 0)
        long_ratio  = coin_info.get("long_ratio", 50)
        mark_px     = coin_info.get("mark_px", 0)

        if direction == "LONG":
            direction_cn = "做多"
            emoji = "🟢"
        elif direction == "SHORT":
            direction_cn = "做空"
            emoji = "🔴"
        else:
            direction_cn = "观望"
            emoji = "⚪"

        return (
            f"{emoji} 链上聪明钱信号出现！\n"
            f"Hyperliquid 排行榜 {whale_count} 个顶级大户中，"
            f"{long_ratio:.0f}% 正在{direction_cn} ${coin}。\n"
            f"当前价格 ${mark_px:,.2f}，大资金已经表态，你怎么看？\n\n"
            f"#{coin} #{futures} #链上数据 #聪明钱"
        )
