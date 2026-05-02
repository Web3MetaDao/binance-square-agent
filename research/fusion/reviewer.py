"""
OverfitReviewer — 过拟合风险审核器

对深度学习优化后的策略参数进行过拟合风险评估，
给出审核结论、理由和替代参数建议。
"""

import json
import os
import logging
from typing import Optional

from openai import OpenAI

from config.settings import OPENAI_API_KEY, OPENAI_BASE_URL, LLM_MODEL

logger = logging.getLogger(__name__)

# ── 过拟合审核提示词 ──────────────────────────────────────────────
REVIEW_PROMPT = """你是一位严谨的量化策略风控审核专家。你的任务是审核深度学习优化后的策略参数是否存在过拟合风险。

【原始参数】
{original_params}

【优化后参数】
{optimized_params}

【回测结果（优化后）】
{backtest_results}

请从以下几个维度深度分析过拟合风险：

## 1. 参数敏感性
- 参数是否过于精细（如 0.01 级别的微调）？
- 参数组合是否过于复杂（3个以上参数同时优化）？
- 参数是否落在常见数值区间外（如周期参数 > 200）？

## 2. 回测合理性
- 夏普比率是否异常高（> 3.0）？
- 最大回撤是否过低（与收益不匹配）？
- 胜率是否过高（> 70%）？
- 交易次数是否过少（< 30次）？

## 3. 过拟合迹象
- 参数是否存在 data snooping 嫌疑？
- 优化后的参数是否与原始参数差异过大？
- 是否存在"曲线拟合"的特征？

请严格按以下 JSON 格式返回，不要包含其他内容：
{{
    "verdict": "pass" | "warning" | "fail",
    "risk_score": 0-100,
    "analysis": {{
        "parameter_sensitivity": "分析...",
        "backtest_rationality": "分析...",
        "overfitting_signs": "分析...",
        "overall_assessment": "综合评估..."
    }},
    "reason": "审核结论的理由...",
    "suggested_alternative_params": {{
        "alternative": {{
            "param_name": "调整后的值..."
        }},
        "reason": "调整理由..."
    }},
    "recommendation": "pass/retune/reject 的具体行动建议..."
}}
"""


class OverfitReviewer:
    """过拟合风险审核器 — Hermes 驱动"""

    def __init__(self, model: Optional[str] = None):
        self.client = OpenAI(
            api_key=os.environ.get("DEEPSEEK_API_KEY") or OPENAI_API_KEY,
            base_url=os.environ.get("DEEPSEEK_BASE_URL") or OPENAI_BASE_URL or "https://api.openai.com/v1",
        )
        self.model = model or os.environ.get("DEEPSEEK_MODEL") or LLM_MODEL or "deepseek-chat"

    # ── 公开接口 ──────────────────────────────────────────────────

    def review(
        self,
        original_params: dict,
        optimized_params: dict,
        backtest_results: dict,
    ) -> dict:
        """审核优化后策略的过拟合风险。

        Args:
            original_params: 原始策略参数字典。
            optimized_params: 优化后的策略参数字典。
            backtest_results: 回测结果字典（至少包含 sharpe_ratio,
                             max_drawdown, win_rate 等关键指标）。

        Returns:
            review_result dict:
                - verdict: "pass" | "warning" | "fail"
                - risk_score: 0-100 风险评分
                - reason: 审核结论理由
                - analysis: 多维度分析 dict
                - suggested_alternative_params: 建议的替代参数（如果过拟合风险高）
                - recommendation: 行动建议

        Raises:
            ValueError: 输入无效。
            RuntimeError: API 调用失败。
        """
        # 构建提示
        prompt = self._build_prompt(
            original_params or {},
            optimized_params or {},
            backtest_results or {},
        )

        # 调用 Hermes
        raw_output = self._call_hermes(prompt)

        # 解析响应
        result = self._parse_response(raw_output)

        # 验证核心字段
        if "verdict" not in result:
            logger.warning("Hermes response missing 'verdict' field — treating as warning")
            result["verdict"] = "warning"
        if "risk_score" not in result:
            result["risk_score"] = 50  # 默认中等风险
        if "reason" not in result:
            result["reason"] = result.get("analysis", {}).get("overall_assessment", "No reason provided")

        return result

    # ── 内部方法 ──────────────────────────────────────────────────

    def _build_prompt(
        self,
        original_params: dict,
        optimized_params: dict,
        backtest_results: dict,
    ) -> str:
        """构建审核提示词。"""
        return REVIEW_PROMPT.format(
            original_params=json.dumps(original_params, ensure_ascii=False, indent=2),
            optimized_params=json.dumps(optimized_params, ensure_ascii=False, indent=2),
            backtest_results=json.dumps(backtest_results, ensure_ascii=False, indent=2),
        )

    def _call_hermes(self, prompt: str) -> str:
        """调用 Hermes API，返回原始响应文本。"""
        import httpx
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.2,
            max_tokens=4096,
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
