"""
PaperParser — 原始文档 → 结构化策略字典

调用 Hermes API（OpenAI 兼容格式）执行结构化抽取，
返回符合 strategy_library schema 的 dict。
"""

import json
import time
import logging
from typing import Optional

from openai import OpenAI

from config.settings import OPENAI_API_KEY, OPENAI_BASE_URL, LLM_MODEL

logger = logging.getLogger(__name__)

# ── 结构化抽取提示词 ────────────────────────────────────────────────
EXTRACTION_PROMPT = """你是一位资深量化交易策略分析师。请从以下文档中提取完整的交易策略信息，以 JSON 格式返回。

请严格提取以下字段（如果某字段在原文中不存在，使用空值 null 或空数组 []）：
1. strategy_name: 策略的名称（中文或英文，取最正式的名称）
2. author_institution: 作者或所属机构
3. core_indicators: 核心使用的技术指标列表（如 ["EMA(20)", "RSI(14)", "Bollinger Bands(20,2)"]）
4. entry_conditions: 入场条件列表，每个条件为描述性字符串
5. exit_conditions: 出场条件列表，每个条件为描述性字符串
6. risk_management: 风险管理描述（止损、仓位管理等）
7. backtest_results: 回测结果对象，包含回测周期、收益率、夏普比率、最大回撤等（如果原文有的话）
8. innovation_points: 创新点列表
9. applicable_markets: 适用市场/交易对列表

文档内容：
{text}

请只返回 JSON 对象，不要包含其他任何内容。
"""


class PaperParser:
    """原始文档 → 结构化策略字典"""

    def __init__(self, model: Optional[str] = None):
        if not OPENAI_API_KEY:
            raise RuntimeError(
                "OPENAI_API_KEY is not set. "
                "Check your .env file or environment variables."
            )
        self.client = OpenAI(
            api_key=OPENAI_API_KEY,
            base_url=OPENAI_BASE_URL or "https://api.openai.com/v1",
        )
        self.model = model or LLM_MODEL or "deepseek-chat"

    # ── 公开接口 ──────────────────────────────────────────────────

    def parse(self, text: str) -> dict:
        """解析文档内容，返回符合 strategy_library schema 的 dict。

        Args:
            text: 原始文档文本内容。

        Returns:
            结构化策略 dict，字段与 strategy_library 表一致。

        Raises:
            ValueError: 如果输入为空或解析后得到空结果。
            RuntimeError: API 调用全部失败。
        """
        if not text or not text.strip():
            raise ValueError("Empty document text — nothing to parse.")

        attempts = 0
        last_error = None

        while attempts < 3:
            attempts += 1
            try:
                raw = self._call_hermes(text)
                parsed = self._parse_response(raw)
                if parsed and parsed.get("strategy_name"):
                    return parsed
                # 解析成功但缺少策略名称 — 自动生成fallback名称
                if parsed:
                    # 从文本中提取文件名或前10个词作为策略名称
                    words = text.split()[:6]
                    fallback = " ".join(w for w in words if not w.startswith(("http", "@", "#")))
                    if len(fallback) > 40:
                        fallback = fallback[:40]
                    if not fallback:
                        fallback = f"策略_{attempts}"
                    parsed["strategy_name"] = fallback
                    logger.info(
                        "Auto-generated strategy_name: %s (attempt %d/3)",
                        fallback, attempts,
                    )
                    return parsed
                else:
                    last_error = "空响应"
            except (json.JSONDecodeError, KeyError) as e:
                last_error = f"JSON解析错误: {e}"
                logger.warning("Parse attempt %d/3 failed: %s", attempts, last_error)
            except Exception as e:
                last_error = str(e)
                logger.warning("API call attempt %d/3 failed: %s", attempts, last_error)
                if "timeout" in str(e).lower() or "429" in str(e):
                    time.sleep(2 ** attempts)  # 指数退避
                    continue
                if attempts < 3:
                    time.sleep(2 ** attempts)
                    continue
                raise

            if attempts < 3:
                time.sleep(2 ** attempts)  # 指数退避

        raise RuntimeError(
            f"Failed to parse document after {attempts} attempts. Last error: {last_error}"
        )

    # ── 内部方法 ──────────────────────────────────────────────────

    def _call_hermes(self, text: str) -> str:
        """调用 Hermes API（OpenAI 兼容格式），返回原始 JSON 字符串。"""
        prompt = EXTRACTION_PROMPT.format(text=text)

        import httpx
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=4096,
            timeout=httpx.Timeout(120.0, connect=15.0),
        )

        content = response.choices[0].message.content
        if not content or not content.strip():
            raise ValueError("Hermes returned empty response")

        return content.strip()

    def _parse_response(self, raw: str) -> dict:
        """将 Hermes 返回的 JSON 字符串解析为 dict。"""
        # 先尝试直接解析
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass

        # 尝试提取 JSON 块（如果模型额外包裹了 markdown 代码块）
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            # 移除 ```json 或 ``` 包裹
            lines = cleaned.splitlines()
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned = "\n".join(lines).strip()

        return json.loads(cleaned)
