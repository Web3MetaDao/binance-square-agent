"""
w2e_post_generator.py
=====================
W2E 排行榜博主帖子内容提取 → LLM 改写 → 原创短贴生成器

功能：
1. 从 data/w2e_top_creators.json 读取排行榜前10博主的帖子
2. 按收益权重随机选取一篇高质量帖子作为参考素材
3. 通过 LLM 改写为完全原创的短贴（不抄袭，融入自身人设）
4. 确保帖子包含 cashtag、CTA 和内容挖矿引导
5. 直接调用 SquarePoster 发布到币安广场

数据流：
  data/w2e_top_creators.json
    → 选取参考帖子（按博主收益加权随机）
    → LLM 改写（结合 persona.md 人设）
    → SquarePoster.post() 发布
    → 写入发帖日志

调用方式：
  - 独立运行：python3 w2e_post_generator.py
  - 集成调用：from w2e_post_generator import W2EPostGenerator; gen.run_once()
  - 调度模式：gen.run_scheduler(interval_minutes=20)
"""

import json
import os
import random
import re
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

import utils.price_sync as price_sync

sys.path.insert(0, os.path.dirname(__file__))

from openai import OpenAI
from config.settings import (
    OPENAI_API_KEY, OPENAI_BASE_URL, LLM_MODEL,
    POST_MIN_CHARS, POST_MAX_CHARS, DAILY_LIMIT,
    WRITE_TO_EARN_URL,
)
from layers.executor import (
    SquarePoster,
    QuotaController,
    log_post,
    _posting_transaction,
    _refresh_state_binding,
    _reserve_post_intent,
    _finalize_post_success,
    _clear_posting_intent,
    _is_ambiguous_post_failure,
)
from core.state import load_state, save_state, update_state

# ── 日志配置 ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [W2E-Generator] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("w2e_generator")


def _quota_reason_code(coin: str, reason: str) -> str:
    coin = (coin or "").upper().strip() or "UNKNOWN"
    normalized = (reason or "").upper()
    if "BANNED" in normalized:
        return "banned"
    if "每日上限" in reason:
        return "daily_limit_reached"
    if "分钟" in reason and "需再等" in reason:
        return f"global_interval:{coin}"
    if "一天只能发一次" in reason or "今日已发过" in reason:
        return f"coin_daily_unique:{coin}"
    if "h" in reason and "需再等" in reason:
        return f"coin_cooldown:{coin}"
    return f"quota_blocked:{coin}"

# ── 常量 ──────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
DATA_DIR   = BASE_DIR / "data"
W2E_FILE   = DATA_DIR / "w2e_top_creators.json"
PERSONA_FILE = BASE_DIR / "data" / "persona.md"

# 禁用词（与 content.py 保持一致）
BANNED_PHRASES = [
    "综上所述", "首先", "其次", "最后", "总的来说",
    "不得不说", "毋庸置疑", "值得注意的是",
    "在当前市场环境下", "作为投资者",
]

# CTA 池（内容挖矿引导）
W2E_CTA_POOL = [
    "💡 点下方币种标签看实时行情，也欢迎留言说说你的交易计划。",
]
FIXED_DISCLAIMER = (
    "⚠️免责声明：\n"
    "本文仅为个人行情观点分享，不构成任何投资建议，加密货币市场高波动、高风险，请理性交易、自行承担风险。"
)
FIXED_HASHTAGS = "#交易复盘 #行情分析 #交易计划"


# ── 核心类 ────────────────────────────────────────────────────────────────────

class W2EPostGenerator:
    """
    从 W2E 排行榜博主帖子中提取素材，通过 LLM 改写为原创短贴并发布。
    """

    def __init__(self):
        self.client = OpenAI(
            api_key=OPENAI_API_KEY,
            base_url=OPENAI_BASE_URL,
        )
        self.poster = SquarePoster()
        self.state  = load_state()
        self._cta_index = 0

    # ── 数据加载 ──────────────────────────────────────────────────────────────

    def _load_w2e_data(self) -> list[dict]:
        """加载 W2E 排行榜数据，返回博主列表。"""
        if not W2E_FILE.exists():
            logger.warning(f"W2E 数据文件不存在: {W2E_FILE}，请先运行 write_to_earn_crawler.py")
            return []
        with open(W2E_FILE, encoding="utf-8") as f:
            data = json.load(f)
        raw_creators = data.get("top_creators") or data.get("creators", [])
        creators = []
        for creator in raw_creators:
            creators.append({
                "rank": creator.get("rank"),
                "nickname": creator.get("nickname", ""),
                "earnings_usdc": creator.get("earnings_usdc", creator.get("earn_usdc", 0)),
                "recent_posts": [
                    {
                        "id": post.get("id"),
                        "text": post.get("text", ""),
                        "views": post.get("views", post.get("view_count", 0)),
                        "likes": post.get("likes", post.get("like_count", 0)),
                        "hashtags": post.get("hashtags", []),
                    }
                    for post in creator.get("recent_posts", creator.get("posts", []))
                    if post.get("text", "").strip()
                ],
            })
        logger.info(f"加载 {len(creators)} 位 W2E 博主数据")
        return creators

    def _load_persona(self) -> str:
        """加载用户人设文件。"""
        if PERSONA_FILE.exists():
            return PERSONA_FILE.read_text(encoding="utf-8")[:800]
        return "加密货币交易内容创作者，偏交易复盘和市场分析，重视价格结构、计划和风险收益表达。"

    def _load_recent_post_texts(self, max_count: int = 6) -> list[str]:
        """从 agent_state.json 的 post_history 字段加载近期已发帖子正文，
        用于 LLM 提示词中禁止重复句式。"""
        try:
            from core.state import DATA_DIR
            state_path = DATA_DIR / "agent_state.json"
            if not state_path.exists():
                return []
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

    # ── 素材选取 ──────────────────────────────────────────────────────────────

    def _get_tg_hot_coins(self) -> list[dict]:
        """
        从 TG 频道获取热点币种信号列表。
        返回格式：[{"coin": "BTC", "type": "OI_SURGE", "oi_change": 14.4, "price_change": 14.6, "source": "bwe_oi"}, ...]
        """
        try:
            from smart_money.telegram_scanner import TelegramChannelScanner
            scanner = TelegramChannelScanner()
            signals = scanner.scan_all_channels()
            logger.info(f"[W2E-TG] 获取到 {len(signals)} 个 TG 信号")
            return signals
        except Exception as e:
            logger.warning(f"[W2E-TG] TG 信号获取失败，使用普通选帖: {e}")
            return []

    def _select_reference_post_with_tg(
        self, creators: list[dict], tg_signals: list[dict]
    ) -> tuple[dict | None, dict | None]:
        """
        方案B：优先选择 TG 热点币种的 KOL 帖子。
        若无匹配则降级为普通加权随机选取。
        返回 (reference_post_dict, matched_tg_signal_or_None)
        """
        # 构建 TG 热点币种集合（大写）
        tg_coin_map = {}
        for sig in tg_signals:
            coin = sig.get("coin", "").upper()
            if coin:
                # 保留优先级最高（数值最小）的信号
                if coin not in tg_coin_map or sig.get("priority", 99) < tg_coin_map[coin].get("priority", 99):
                    tg_coin_map[coin] = sig

        if tg_coin_map:
            logger.info(f"[W2E-TG] TG 热点币种: {list(tg_coin_map.keys())}")

        # 先尝试找 TG 热点币种的 KOL 帖子
        matched_candidates = []
        matched_weights = []
        all_candidates = []
        all_weights = []

        for creator in creators:
            posts = creator.get("recent_posts", [])
            if not posts:
                continue
            earnings = creator.get("earnings_usdc", 1.0)
            for post in posts:
                text = post.get("text", "").strip()
                if len(text) < 30:
                    continue
                item = {"creator": creator, "post": post}
                all_candidates.append(item)
                all_weights.append(earnings)
                # 检查是否与 TG 热点币种匹配
                post_coin = self._extract_main_coin(text).upper()
                if post_coin in tg_coin_map:
                    matched_candidates.append((item, tg_coin_map[post_coin]))
                    matched_weights.append(earnings * 3)  # 匹配帖子权重 ×3

        if matched_candidates:
            # 有匹配：从匹配帖子中加权随机选取
            idx = random.choices(range(len(matched_candidates)), weights=matched_weights, k=1)[0]
            selected_item, matched_signal = matched_candidates[idx]
            creator_name = selected_item["creator"]["nickname"]
            coin = self._extract_main_coin(selected_item["post"]["text"])
            logger.info(f"[W2E-TG] ✅ TG共振匹配！币种: {coin} | 博主: {creator_name} | 信号: {matched_signal.get('type')}")
            return selected_item, matched_signal
        else:
            # 无匹配：降级为普通加权随机
            if not all_candidates:
                logger.warning("没有可用的参考帖子")
                return None, None
            selected = random.choices(all_candidates, weights=all_weights, k=1)[0]
            creator_name = selected["creator"]["nickname"]
            earnings = selected["creator"]["earnings_usdc"]
            post_preview = selected["post"]["text"][:60]
            logger.info(f"[W2E-TG] 无TG匹配，普通选帖: [{creator_name} | 收益 {earnings:.0f} USDC] {post_preview}...")
            return selected, None

    def _select_reference_post(self, creators: list[dict]) -> dict | None:
        """
        按博主收益加权随机选取一篇参考帖子。
        收益越高的博主，其帖子被选中的概率越大。
        返回格式：{"creator": {...}, "post": {...}}
        """
        candidates = []
        weights = []

        for creator in creators:
            posts = creator.get("recent_posts", [])
            if not posts:
                continue
            earnings = creator.get("earnings_usdc", 1.0)
            for post in posts:
                text = post.get("text", "").strip()
                # 过滤过短或无实质内容的帖子
                if len(text) < 30:
                    continue
                candidates.append({"creator": creator, "post": post})
                # 权重 = 博主收益（收益越高权重越大）
                weights.append(earnings)

        if not candidates:
            logger.warning("没有可用的参考帖子")
            return None

        selected = random.choices(candidates, weights=weights, k=1)[0]
        creator_name = selected["creator"]["nickname"]
        earnings = selected["creator"]["earnings_usdc"]
        post_preview = selected["post"]["text"][:60]
        logger.info(f"选取参考帖子: [{creator_name} | 收益 {earnings:.0f} USDC] {post_preview}...")
        return selected

    # ── LLM 改写 ──────────────────────────────────────────────────────────────

    def _next_cta(self, coin: str) -> str:
        """轮换 CTA 模板。"""
        cta = W2E_CTA_POOL[self._cta_index % len(W2E_CTA_POOL)]
        self._cta_index += 1
        return cta.replace("${coin}", f"${coin}")

    def _extract_main_coin(self, text: str) -> str:
        """从帖子文本中提取主要提及的代币。"""
        known_non_coins = {"USD", "USDT", "USDC", "BUSD", "COIN", "TOKEN"}
        future_markers = re.findall(r"\{future\}\(([A-Z0-9]{1,20})USDT\)", text.upper())
        if future_markers:
            return future_markers[0]
        pair_markers = re.findall(r"\b([A-Z0-9]{1,20})USDT\b", text.upper())
        if pair_markers:
            return pair_markers[0]
        cashtags = re.findall(r"\$([A-Z0-9]{1,15})", text.upper())
        valid_tags = [t for t in cashtags if t not in known_non_coins]
        if valid_tags:
            from collections import Counter
            return Counter(valid_tags).most_common(1)[0][0]
        # 扩展常见代币关键词匹配（按出现频率统计，取最多的）
        common_coins = [
            "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "AVAX",
            "MATIC", "DOT", "LINK", "UNI", "ATOM", "LTC", "BCH", "NEAR",
            "APT", "ARB", "OP", "SUI", "TON", "TRUMP", "PEPE", "WIF",
            "BONK", "NOT", "HYPE", "BSB", "TRADOOR", "AUCTION", "MAVIA",
            "ORCA", "HYPER", "MOVR",
        ]
        text_upper = text.upper()
        # 统计每个代币出现次数，返回出现最多的
        counts = {}
        for coin in common_coins:
            matches = re.findall(r"\b" + coin + r"\b", text_upper)
            if matches:
                counts[coin] = len(matches)
        if counts:
            return max(counts, key=counts.get)
        return "BTC"  # 默认

    def _canonical_cashtags(self, coin: str) -> list[str]:
        primary = (coin or "BTC").upper()
        tags = []
        for item in [primary, "BSB"]:
            token = f"${item}"
            if token not in tags:
                tags.append(token)
        return tags

    def _live_price_line(self, coin: str) -> str:
        try:
            fp = price_sync.get_futures_price(coin)
        except Exception as e:
            logger.warning(f"[W2E生成器] 价格同步失败: {e}")
            return ""
        if not fp or not price_sync.is_price_fresh(fp, max_age=price_sync.PRICE_FRESHNESS_TTL):
            return ""
        return (
            f"现在 {coin} 期货最新价 ${fp['price']:,.4f}，24h {fp['change_24h']:+.2f}%"
            f"，日内区间 ${fp['low_24h']:,.4f}-${fp['high_24h']:,.4f}。"
        )

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

    def _format_fixed_template_post(self, raw: str, coin: str) -> str:
        coin = (coin or "BTC").upper()
        futures = f"{coin}USDT"
        cleaned = (raw or "").replace("\r", "\n")
        cleaned = re.sub(r"\{future\}\(([A-Z][A-Z0-9]{0,19})USDT\)", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\$([A-Z][A-Z0-9]{0,19})USDT\b", lambda m: f"${m.group(1).upper()}", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"#[A-Z][A-Z0-9]{0,19}USDT\b", "", cleaned, flags=re.IGNORECASE)
        for bad, good in [
            (f"{{future}}({futures})", ""),
            (f"${futures}", f"${coin}"),
            (f"#{futures}", ""),
            ("#币安广场", ""),
            ("#内容挖矿", ""),
            (FIXED_DISCLAIMER, ""),
            (W2E_CTA_POOL[0], ""),
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

        if not lines:
            # ── 多样性内容池：当 LLM 输出全部被过滤或不可用时 ──
            _FALLBACK_TEMPLATES = [
                f"{coin} 目前盘面结构比较清晰，关键位看量价配合再动手。",
                f"{coin} 这个位置多空都有理由，自己要有主见，别跟风。",
                f"{coin} 波动空间还在，但需要耐心等结构确认。",
                f"{coin} 短线上多空争夺激烈，等一个方向选择。",
                f"{coin} 盘面在收敛，快出方向了，提前想好应对比猜测重要。",
                f"{coin} 价格在这个区间磨了很久，盯着量看，放量破位才是真。",
                f"{coin} 这轮调整还没确认结束，等一个二次测试。",
                f"{coin} 趋势还在，但节奏开始放缓了，别追，等回调接。",
            ]
            template = random.choice(_FALLBACK_TEMPLATES)
            lines = [template, f"${coin} 先看量价配合，再决定方向。"]

        title = lines[0]
        body_lines = [line for line in lines[1:4] if line != title]
        live_line = self._live_price_line(coin)
        if self._has_untrusted_price_claim(title, trusted_live_line=live_line):
            title = ""
        if live_line and live_line not in body_lines:
            body_lines.append(live_line)
        if not any(f"${coin}" in line for line in [title] + body_lines):
            # 加 $COIN cashtag 到正文中确保 W2E 计费触发
            if body_lines:
                body_lines.append(f"关注 ${coin} 的结构演变，等确认。")
            else:
                body_lines.append(f"等 {coin} 的关键位出来再看方向，提前猜没意义。")

        normalized_body_lines = []
        for line in body_lines:
            stripped = line.strip()
            if stripped == f"${coin}":
                continue
            if stripped.startswith(f"${coin} "):
                stripped = f"{coin}：{stripped[len(coin)+2:]}"
            if self._has_untrusted_price_claim(stripped, trusted_live_line=live_line):
                continue
            normalized_body_lines.append(stripped)
        body_lines = self._dedupe_semantic_lines(normalized_body_lines, coin)
        if not title:
            if body_lines:
                title = body_lines.pop(0)
            else:
                title = f"{coin} 这波节奏还没走完"

        return "\n\n".join([
            title,
            "\n".join(body_lines).strip(),
            " ".join(self._canonical_cashtags(coin)),
            FIXED_HASHTAGS,
            FIXED_DISCLAIMER,
        ])

    def _build_rewrite_prompt(
        self,
        reference_post: dict,
        creator: dict,
        persona: str,
        tg_signal: dict | None = None,
    ) -> str:
        """构建 LLM 改写 Prompt（含币安期货合约实时价格 + TG 信号数据）。"""
        ref_text = reference_post.get("text", "")
        ref_views = reference_post.get("views", 0)
        ref_likes = reference_post.get("likes", 0)
        creator_name = creator.get("nickname", "")
        creator_earnings = creator.get("earnings_usdc", 0)
        coin = self._extract_main_coin(ref_text)
        banned_str = "、".join(BANNED_PHRASES[:6])

        # ── 期货合约实时价格同步 ──
        price_line = ""
        try:
            fp = price_sync.get_futures_price(coin)
            if fp and price_sync.is_price_fresh(fp, max_age=price_sync.PRICE_FRESHNESS_TTL):
                price_line = (
                    f"\n币安期货实时行情（必须使用这些真实数据，不能编造）："
                    f"\n- {coin} 当前期货价格: ${fp['price']:,.4f}"
                    f"\n- 24h涨跌幅: {fp['change_24h']:+.2f}%"
                    f"\n- 24h最高: ${fp['high_24h']:,.4f}，最低: ${fp['low_24h']:,.4f}"
                )
                logger.info(f"[W2E生成器] 💹 {coin} 期货实时价格已注入: ${fp['price']:,.4f} ({fp['change_24h']:+.2f}%)")
            elif fp:
                logger.warning(f"[W2E生成器] ⚠️ {coin} 价格存在但已过 freshness 窗口，跳过实时行情注入")
        except Exception as e:
            logger.warning(f"[W2E生成器] 价格同步失败: {e}")

        # ── TG 信号数据注入（方案A）──
        tg_line = ""
        if tg_signal:
            sig_type = tg_signal.get("type", "")
            sig_source = tg_signal.get("source", "")
            sig_data = tg_signal.get("data", {})
            if sig_type in ("OI_SURGE", "OI_PRICE_SURGE"):
                oi_chg = sig_data.get("oi_change_pct", 0)
                px_chg = sig_data.get("price_change_pct", 0)
                oi_val = sig_data.get("oi_value", 0)
                tg_line = (
                    f"\n链上实时信号（来自 TG 监控频道，必须融入帖子内容）："
                    f"\n- 信号类型: 合约持仓量(OI)异动"
                    f"\n- {coin} OI 变化: +{oi_chg:.1f}%，价格变化: {px_chg:+.1f}%"
                    + (f"\n- 当前 OI 规模: ${oi_val/1e6:.1f}M" if oi_val else "")
                )
            elif sig_type in ("WHALE_LONG", "WHALE_SHORT", "WHALE_CLOSE"):
                direction = "做多" if "LONG" in sig_type else ("做空" if "SHORT" in sig_type else "平仓")
                size = sig_data.get("position_size_usd", 0)
                win_rate = sig_data.get("win_rate", 0)
                tg_line = (
                    f"\n链上实时信号（来自 TG 监控频道，必须融入帖子内容）："
                    f"\n- 信号类型: 巨鲸{direction}信号"
                    + (f"\n- 持仓规模: ${size/1e6:.1f}M" if size else "")
                    + (f"\n- 历史胜率: {win_rate:.0f}%" if win_rate else "")
                )
            elif sig_type == "TG_COMBINED":
                tg_line = (
                    f"\n链上实时信号（来自 TG 监控频道，必须融入帖子内容）："
                    f"\n- 信号类型: 多源共振（Hyperliquid巨鲸 + 币安OI同步异动）"
                    f"\n- 这是最强烈的买入/卖出信号，请在帖子中强调多源确认的重要性"
                )
            if tg_line:
                logger.info(f"[W2E-TG] 📡 TG信号数据已注入: {coin} {sig_type}")

        # ── 注入近期已发帖子（内容去重） ──
        recent_texts = self._load_recent_post_texts()
        recent_block = ""
        if recent_texts:
            recent_block = (
                "\n\n⚠️ 注意避免与以下近期已发帖子的内容重复（包括句式和结构）：\n"
                + "\n".join(f"- \"{t}\"" for t in recent_texts)
            )

        return f"""你是一位币安广场内容创作者，正在参与内容挖矿（Write to Earn）活动。{recent_block}

你的人设背景：
{persona}

任务：将下面这篇高收益博主的帖子改写为完全原创的短贴。

参考素材：
- 原作者：{creator_name}（上周内容挖矿收益：{creator_earnings:.0f} USDC）
- 原帖数据：{ref_views:,} 次浏览，{ref_likes} 个点赞
- 原帖内容：
{ref_text}{price_line}{tg_line}

改写要求（严格遵守，违反任何一条则重写）：
1. 字数 {POST_MIN_CHARS}~{POST_MAX_CHARS} 字（不含标签行和 CTA）
2. 必须完全原创，不能抄袭原帖，要用自己的语言和视角重新表达
3. 保留原帖的核心观点或市场信息，但角度、结构、措辞必须不同
4. 可以写故事，但必须有真实交易细节支撑，不要空喊情绪
5. 正文中必须包含至少一个具体数字（价格、涨跌幅、时间等），如果有上面的实时行情数据则优先使用
6. 优先补足点位、周期、仓位计划、判断依据、失效条件
7. 结尾可以是自然问句，也可以自然收住，不要为了互动强行提问
8. 语气口语化、像真人说话，但不要硬凹老韭菜、神预测、逆天收益这类人设
9. 不要连续复用“强钩子+惨痛经历+大数字+留悬念”套路
10. 绝对禁止使用：{banned_str}
11. 禁止任何八股文结构（首先/其次/综上等）
12. 正文中必须包含 ${coin} cashtag，触发内容挖矿手续费返佣
13. 不要输出免责声明、CTA、模板化标签行，这些由程序统一格式化
14. 不要出现 ${coin}USDT、#{coin}USDT、{{future}}({coin}USDT)、#币安广场、#内容挖矿 等旧格式
只输出改写后的短贴正文，不要输出任何解释、前缀或引号。"""

    def _rewrite_with_llm(self, reference: dict) -> tuple[str, str]:
        """
        调用 LLM 将参考帖子改写为原创短贴。
        返回 (改写后的正文, 主代币符号)
        """
        creator = reference["creator"]
        post    = reference["post"]
        persona = self._load_persona()
        coin    = self._extract_main_coin(post.get("text", ""))

        prompt = self._build_rewrite_prompt(post, creator, persona, tg_signal=reference.get("tg_signal"))

        try:
            response = self.client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.85,
                max_tokens=600,
            )
            body = response.choices[0].message.content.strip()
            logger.info(f"LLM 改写成功，字数: {len(body)}")
            return body, coin
        except Exception as e:
            logger.error(f"LLM 改写失败: {e}")
            return "", coin

    # ── 完整发帖流程 ──────────────────────────────────────────────────────────

    def run_once(self) -> dict:
        """
        执行一次完整的「提取 → 改写 → 发布」流程。
        返回执行结果字典。
        """
        self.state = load_state()

        # 配额检查
        quota = QuotaController(self.state)
        if self.state.get("daily_count", 0) >= DAILY_LIMIT:
            logger.info(f"今日配额已满 ({DAILY_LIMIT} 贴)，跳过")
            return {"success": False, "reason": "daily_limit_reached"}

        # Step 1: 加载 W2E 数据
        creators = self._load_w2e_data()
        if not creators:
            # 触发爬虫抓取
            logger.info("W2E 数据为空，正在触发爬虫...")
            try:
                from write_to_earn_crawler import run_crawler
                run_crawler()
                creators = self._load_w2e_data()
            except Exception as e:
                logger.error(f"爬虫失败: {e}")
                return {"success": False, "reason": "no_w2e_data"}

        # Step 2: 获取 TG 信号并选取参考帖子（TG共振优先）
        tg_signals = self._get_tg_hot_coins()
        reference, tg_signal = self._select_reference_post_with_tg(creators, tg_signals)
        if not reference:
            return {"success": False, "reason": "no_reference_post"}
        # 将 TG 信号附加到 reference 中，供 _rewrite_with_llm 使用
        reference["tg_signal"] = tg_signal

        # Step 2.5: 提前提取 coin，检查同币种冷却（避免重复发帖）
        _preview_coin = self._extract_main_coin(reference["post"].get("text", ""))
        _ok, _reason = quota.can_post(_preview_coin)
        if not _ok:
            logger.info(f"[W2E] 跳过 {_preview_coin}: {_reason}")
            return {"success": False, "reason": _quota_reason_code(_preview_coin, _reason)}

        # Step 3: 双模式 ── 价格可用 → 数据帖；不可用 → LLM 改写
        _price_info = price_sync.get_authoritative_price(_preview_coin)
        if _price_info:
            # 模式A：有价格 → 纯数据行情帖（不走LLM）
            from layers.content import make_data_post
            body = make_data_post(_preview_coin, _price_info)
            coin = _preview_coin
            creator_name = reference["creator"]["nickname"]
            earnings = reference["creator"].get("earnings_usdc", 0)
            print(f"\n{'─'*55}")
            print(f"[W2E生成器] 📊 模式A — 数据帖 ({coin})")
            print(body)
            print(f"{'─'*55}")
        else:
            # 模式B：无价格 → LLM 改写（走原流程）
            body, coin = self._rewrite_with_llm(reference)
            if not body:
                # 模式B降级：LLM不可用时，使用纯分析模板（不含精确价格）
                from layers.content import make_analysis_post
                coin = self._extract_main_coin(reference["post"].get("text", ""))
                body = make_analysis_post(coin)
                print(f"\n{'─'*55}")
                print(f"[W2E生成器] 📝 模式B降级 — 纯分析帖 (LLM不可用, {coin})")
                print(body)
                print(f"{'─'*55}")
            body = self._format_fixed_template_post(body, coin)

            creator_name = reference["creator"]["nickname"]
            earnings = reference["creator"].get("earnings_usdc", 0)
            print(f"\n{'─'*55}")
            print(f"[W2E生成器] 参考博主: {creator_name} (收益 {earnings:.0f} USDC)")
            print(f"[W2E生成器] 改写内容预览:")
            print(body)
            print(f"{'─'*55}")

        # Step 6: 发布（发布事务锁内二次校验，避免并发重复发与全局间隔穿透）
        coin = quota._normalize_coin(coin)
        full_content = body
        with _posting_transaction(coin):
            _refresh_state_binding(self.state, quota)
            ok, reason = quota.can_post(coin)
            if not ok:
                logger.info(f"[W2E] 二次校验跳过 {coin}: {reason}")
                return {"success": False, "reason": _quota_reason_code(coin, reason)}

            intent, blocked_reason = _reserve_post_intent(
                state=self.state,
                content=full_content,
                coin=coin,
                source="w2e",
                tier="W2E",
                mock=getattr(self.poster, "mock_mode", False),
            )
            if blocked_reason:
                logger.info(f"[W2E] 意图拦截跳过 {coin}: {blocked_reason}")
                return {"success": False, "reason": blocked_reason}

            result = self.poster.post(full_content)
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if result["success"]:
                quota.record_post(coin)
                _refresh_state_binding(self.state, quota)
                _finalize_post_success(intent=intent, result=result, content=full_content)
                _refresh_state_binding(self.state, quota)
                logger.info(f"✅ 发帖成功 | {coin} | {result.get('url', '')}")
                logger.info(f"📊 今日进度: {self.state['daily_count']}/{DAILY_LIMIT}")
                log_post({
                    "time":     now_str,
                    "coin":     coin,
                    "tier":     "W2E",
                    "source":   f"W2E排行榜-{creator_name}",
                    "post_id":  result.get("post_id", ""),
                    "url":      result.get("url", ""),
                    "mock":     result.get("mock", False),
                    "preview":  full_content[:100],
                    "status":   "SUCCESS",
                })
            else:
                code = result.get("code", "")
                msg  = result.get("message", "")
                logger.error(f"❌ 发帖失败 | code={code} | {msg}")
                if code == "2000001":
                    latest = update_state(lambda current: {**current, "status": "BANNED"})
                    if latest is not self.state:
                        self.state.clear()
                        self.state.update(latest)
                    logger.critical("🚨 账号封禁，系统熔断！")
                if not _is_ambiguous_post_failure(result):
                    _clear_posting_intent(intent["id"])
                    _refresh_state_binding(self.state, quota)
                log_post({
                    "time":       now_str,
                    "coin":       coin,
                    "tier":       "W2E",
                    "status":     "FAILED",
                    "error_code": code,
                    "error_msg":  msg,
                })

            return result

    # ── 调度循环 ──────────────────────────────────────────────────────────────

    def run_scheduler(self, interval_minutes: int = 30):
        """
        每 interval_minutes 分钟自动执行一次「提取 → 改写 → 发布」。
        独立运行时使用此函数。
        """
        logger.info(f"W2E 发帖调度器启动，每 {interval_minutes} 分钟执行一次")
        logger.info(f"数据来源: 币安广场 Write to Earn 排行榜前10博主")
        logger.info(f"发帖模式: {'模拟模式' if self.poster.mock_mode else '真实发帖'}")

        while True:
            try:
                result = self.run_once()
                if result.get("reason") == "daily_limit_reached":
                    logger.info("今日配额已满，调度器停止")
                    break
            except KeyboardInterrupt:
                logger.info("收到停止信号，调度器退出")
                break
            except Exception as e:
                logger.error(f"发帖异常: {e}", exc_info=True)

            logger.info(f"下次执行时间: {interval_minutes} 分钟后")
            time.sleep(interval_minutes * 60)


# ── 入口 ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    gen = W2EPostGenerator()

    if len(sys.argv) > 1 and sys.argv[1] == "once":
        # 单次运行
        gen.run_once()
    else:
        # 持续调度（默认30分钟）
        gen.run_scheduler(30)
