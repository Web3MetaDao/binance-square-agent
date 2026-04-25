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
  - 调度模式：gen.run_scheduler(interval_minutes=30)
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

sys.path.insert(0, os.path.dirname(__file__))

from openai import OpenAI
from config.settings import (
    OPENAI_API_KEY, OPENAI_BASE_URL, LLM_MODEL,
    POST_MIN_CHARS, POST_MAX_CHARS, DAILY_LIMIT,
    WRITE_TO_EARN_URL,
)
from layers.executor import SquarePoster, QuotaController, log_post
from core.state import load_state, save_state

# ── 日志配置 ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [W2E-Generator] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("w2e_generator")

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
    "💡 点击上方 ${coin} 标签查看实时行情，广场内交易还能给我贡献一点挖矿收益😄",
    "📊 感兴趣的话点击 ${coin} 看看实时价格，广场内容挖矿进行中！",
    "🎯 广场内容挖矿进行中——点击 ${coin} 标签参与交易，我们一起在链上留下痕迹！",
    "🔥 点击 ${coin} 标签直接在广场交易，手续费的一部分会变成我的创作收益，感谢支持！",
    "👇 点击 ${coin} 查看实时K线，广场交易即挖矿，一起赚！",
]


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
        creators = data.get("top_creators", [])
        logger.info(f"加载 {len(creators)} 位 W2E 博主数据")
        return creators

    def _load_persona(self) -> str:
        """加载用户人设文件。"""
        if PERSONA_FILE.exists():
            return PERSONA_FILE.read_text(encoding="utf-8")[:800]
        return "加密货币内容创作者，专注市场分析，风格直接犀利，有自己的独到见解。"

    # ── 素材选取 ──────────────────────────────────────────────────────────────

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
        # 优先提取 $SYMBOL 格式
        cashtags = re.findall(r"\$([A-Z]{2,10})", text.upper())
        if cashtags:
            return cashtags[0]
        # 常见代币关键词匹配
        common_coins = ["BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "AVAX", "MATIC", "DOT"]
        text_upper = text.upper()
        for coin in common_coins:
            if coin in text_upper:
                return coin
        return "BTC"  # 默认

    def _build_rewrite_prompt(
        self,
        reference_post: dict,
        creator: dict,
        persona: str,
    ) -> str:
        """构建 LLM 改写 Prompt（含币安期货合约实时价格）。"""
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
            from utils.price_sync import get_futures_price
            fp = get_futures_price(coin)
            if fp:
                price_line = (
                    f"\n币安期货实时行情（必须使用这些真实数据，不能编造）："
                    f"\n- {coin} 当前期货价格: ${fp['price']:,.4f}"
                    f"\n- 24h涨跌幅: {fp['change_24h']:+.2f}%"
                    f"\n- 24h最高: ${fp['high_24h']:,.4f}，最低: ${fp['low_24h']:,.4f}"
                )
                logger.info(f"[W2E生成器] 💹 {coin} 期货实时价格已注入: ${fp['price']:,.4f} ({fp['change_24h']:+.2f}%)")
        except Exception as e:
            logger.warning(f"[W2E生成器] 价格同步失败: {e}")

        return f"""你是一位币安广场内容创作者，正在参与内容挖矿（Write to Earn）活动。

你的人设背景：
{persona}

任务：将下面这篇高收益博主的帖子改写为完全原创的短贴。

参考素材：
- 原作者：{creator_name}（上周内容挖矿收益：{creator_earnings:.0f} USDC）
- 原帖数据：{ref_views:,} 次浏览，{ref_likes} 个点赞
- 原帖内容：
{ref_text}{price_line}

改写要求（严格遵守，违反任何一条则重写）：
1. 字数 {POST_MIN_CHARS}~{POST_MAX_CHARS} 字（不含标签行和 CTA）
2. 必须完全原创，不能抄袍原帖，要用自己的语言和视角重新表达
3. 保留原帖的核心观点或市场信息，但角度、结构、措辞必须不同
4. 第一句必须是强力 Hook，让人忍不住继续读（可以是反问、惊人数据、或争议性观点）
5. 正文中必须包含至少一个具体数字（价格、涨跌幅、时间等），如果有上面的实时行情数据则优先使用
6. 结尾必须是一个引导互动的问句（如“你怎么看？”、“你上车了吗？”）
7. 语气口语化、像真人说话，绝对禁止使用：{banned_str}
8. 禁止任何八股文结构（首先/其次/综上等）
9. 正文中必须包含 ${coin} cashtag，触发内容挖矿手续费返佣
10. 最后一行必须是标签：#{coin} #币安广场 #内容挖矿 #加密货币
11. 结尾加上免责声明：⚠️免责声明：\n本文仅为个人行情观点分享，不构成任何投资建议，加密货币市场高波动、高风险，请理性交易、自行承担风险。 $BTC $ETH $BNB

只输出改写后的短贴正文（含最后的标签行和免责声明），不要输出任何解释、前缀或引号。"""

    def _rewrite_with_llm(self, reference: dict) -> tuple[str, str]:
        """
        调用 LLM 将参考帖子改写为原创短贴。
        返回 (改写后的正文, 主代币符号)
        """
        creator = reference["creator"]
        post    = reference["post"]
        persona = self._load_persona()
        coin    = self._extract_main_coin(post.get("text", ""))

        prompt = self._build_rewrite_prompt(post, creator, persona)

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

        # Step 2: 选取参考帖子
        reference = self._select_reference_post(creators)
        if not reference:
            return {"success": False, "reason": "no_reference_post"}

        # Step 3: LLM 改写
        body, coin = self._rewrite_with_llm(reference)
        if not body:
            return {"success": False, "reason": "llm_failed"}

        # Step 4: 拼接 CTA
        cta = self._next_cta(coin)
        full_content = f"{body}\n\n{cta}"

        # Step 5: 打印预览
        creator_name = reference["creator"]["nickname"]
        earnings = reference["creator"]["earnings_usdc"]
        print(f"\n{'─'*55}")
        print(f"[W2E生成器] 参考博主: {creator_name} (收益 {earnings:.0f} USDC)")
        print(f"[W2E生成器] 改写内容预览:")
        print(full_content)
        print(f"{'─'*55}")

        # Step 6: 发布
        result = self.poster.post(full_content)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if result["success"]:
            # 更新配额（使用虚拟 coin_info）
            coin_info = {"coin": coin, "tier": "W2E", "futures": f"{coin}USDT"}
            quota.record_post(coin)
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
                self.state["status"] = "BANNED"
                save_state(self.state)
                logger.critical("🚨 账号封禁，系统熔断！")
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
