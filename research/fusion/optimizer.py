"""
StrategyOptimizer — 策略融合优化器

取一个基础策略 + 多个候选策略，通过 Hermes 生成融合优化方案，
并提取 vectorbt 回测代码和预测指标。
"""

import json
import os
import logging
from typing import Optional

from openai import OpenAI

from config.settings import OPENAI_API_KEY, OPENAI_BASE_URL, LLM_MODEL

logger = logging.getLogger(__name__)

# ── 策略融合提示词 ────────────────────────────────────────────────
FUSION_PROMPT = """你是一位顶级的加密货币量化交易科学家（首席加密货币量化交易科学家）。
你的任务是将以下多个量化交易策略融合成一个更优的策略。

【基础策略】
{base_strategy}

【候选策略（可参考其优点）】
{candidate_strategies}

请按以下步骤进行：

## 第一步：深度分析
分析每个策略的核心逻辑、优势、劣势、适用市场条件。找出它们之间的互补性。

## 第二步：融合优化方案
给出至少3种不同的融合方案，每种方案应：
- 说明融合思路
- 详细参数设置
- 预期优势
- 潜在风险

## 第三步：最佳方案选择
从以上方案中选出最佳的一个，说明理由。

## 第四步：Python回测代码（vectorbt）
为最佳方案编写完整的 vectorbt 回测代码。代码必须：
1. 使用 vectorbt 库（import vectorbt as vbt）
2. 包含完整的数据获取、指标计算、信号生成、回测执行
3. 输出详细绩效指标（夏普比率、最大回撤、胜率、盈亏比等）
4. 代码应可直接运行

## 第五步：预测绩效指标
给出融合策略的预期绩效指标：
- 年化收益率
- 夏普比率
- 最大回撤
- 胜率
- 盈亏比
- 适合的市场环境

请严格按以下 JSON 格式返回，不要包含其他内容：
{{
    "analysis": "详细分析...",
    "optimization_schemes": [
        {{
            "name": "方案1名称",
            "description": "方案描述...",
            "parameters": {{...}},
            "advantages": ["优势1", "优势2"],
            "risks": ["风险1", "风险2"]
        }}
    ],
    "best_scheme": "最佳方案名称",
    "best_scheme_reason": "选择理由...",
    "vectorbt_code": "完整的 Python 回测代码...",
    "predicted_metrics": {{
        "annual_return": 0.0,
        "sharpe_ratio": 0.0,
        "max_drawdown": 0.0,
        "win_rate": 0.0,
        "profit_factor": 0.0,
        "suitable_market": "描述..."
    }}
}}
"""


class StrategyOptimizer:
    """策略融合优化器 — Hermes 驱动"""

    def __init__(self, model: Optional[str] = None):
        # 延迟从环境变量读取（兼容模块加载前 .env 未就绪的情况）
        self.client = OpenAI(
            api_key=os.environ.get("DEEPSEEK_API_KEY") or OPENAI_API_KEY,
            base_url=os.environ.get("DEEPSEEK_BASE_URL") or OPENAI_BASE_URL or "https://api.openai.com/v1",
        )
        self.model = model or os.environ.get("DEEPSEEK_MODEL") or LLM_MODEL or "deepseek-chat"

    # ── 公开接口 ──────────────────────────────────────────────────

    def optimize(
        self,
        base_strategy: dict,
        candidate_strategies: list[dict],
    ) -> dict:
        """融合优化策略，返回包含 vectorbt 代码的结果 dict。

        Args:
            base_strategy: 基础策略 dict（至少包含 strategy_name 和核心字段）。
            candidate_strategies: 候选策略 dict 列表。

        Returns:
            fusion_result dict:
                - hermes_output: Hermes 返回的完整 JSON
                - vectorbt_code: 提取的 Python 回测代码（字符串）
                - predicted_metrics: 预测绩效指标（dict）
                - best_scheme: 最佳方案名称
                - analysis: 分析文本

        Raises:
            ValueError: 输入无效。
            RuntimeError: API 调用失败。
        """
        if not base_strategy:
            raise ValueError("base_strategy is required")
        if not candidate_strategies:
            logger.warning("No candidate strategies provided — optimizer may produce limited results")

        # 构建提示
        prompt = self._build_prompt(base_strategy, candidate_strategies)

        # 调用 Hermes
        raw_output = self._call_hermes(prompt)

        # 解析响应
        parsed = self._parse_response(raw_output)

        # 提取 vectorbt 代码
        code = self._extract_code(parsed)

        # 构建结果
        return {
            "hermes_output": parsed,
            "vectorbt_code": code,
            "predicted_metrics": parsed.get("predicted_metrics", {}),
            "best_scheme": parsed.get("best_scheme", ""),
            "analysis": parsed.get("analysis", ""),
        }

    # ── 内部方法 ──────────────────────────────────────────────────

    def _build_prompt(self, base: dict, candidates: list[dict]) -> str:
        """构建融合提示词。"""
        base_str = json.dumps(base, ensure_ascii=False, indent=2)
        cand_str = json.dumps(candidates, ensure_ascii=False, indent=2)

        return FUSION_PROMPT.format(
            base_strategy=base_str,
            candidate_strategies=cand_str,
        )

    def _call_hermes(self, prompt: str) -> str:
        """调用 Hermes API，返回原始响应文本。"""
        import httpx
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.3,
            max_tokens=8192,
            timeout=httpx.Timeout(120.0, connect=15.0),
        )

        content = response.choices[0].message.content
        if not content or not content.strip():
            raise ValueError("Hermes returned empty response")

        return content.strip()

    def _parse_response(self, raw: str) -> dict:
        """解析 Hermes 返回的 JSON。"""
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass

        # 尝试提取 JSON 块
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            lines = cleaned.splitlines()
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned = "\n".join(lines).strip()

        return json.loads(cleaned)

    def _extract_code(self, parsed: dict) -> str:
        """从解析结果中提取 vectorbt 代码。"""
        code = parsed.get("vectorbt_code", "")

        if not code:
            logger.warning("No vectorbt_code found in Hermes output")
            return ""

        # 如果代码被包裹在 markdown 代码块中，剥离外层的 ``` 标记
        code = code.strip()
        if code.startswith("```"):
            lines = code.splitlines()
            # 移除第一行的 ```python 或 ```
            if lines[0].startswith("```"):
                lang = lines[0][3:].strip()
                lines = lines[1:]
            # 移除最后一行的 ```
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            code = "\n".join(lines)

        return code
