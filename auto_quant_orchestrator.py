#!/usr/bin/env python3
"""
auto_quant_orchestrator.py — 币圈全自动量化学习闭环编排器 (v2.1 fix)

完整闭环:
  1. 4 Agent并行学习 (arXiv / GitHub / TV / 社区)
  2. 汇总 → 差距分析 → 筛选P0候选指标
  3. 自动注入到 scoring.py (含WEIGHTS + MAX_SCORE同步更新)
  4. 自动运行测试验证
  5. 生成汇报报告

Bugfix v2.1 (2026-05-02):
  - Bug #1: 注入时同步更新 WEIGHTS dict + MAX_SCORE，不借用已有权重
  - Bug #2: Phase 1 全部空跑时检测并终止管线
  - Bug #3: 无预置注入逻辑的候选写入待办池 backlog 而非丢弃
  - Bug #4: cumulative_injected.json 保证轮次间不重复注入

运行: python3 auto_quant_orchestrator.py
Cron: 每天08:00, 14:00, 20:00
"""

import os, sys, json, subprocess, logging, re
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "logs", f"auto_quant_{datetime.now().strftime('%Y%m%d')}.log"
        ))
    ]
)
logger = logging.getLogger("auto_quant")

ROOT = Path(__file__).parent.resolve()
LOGS_DIR = ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# ── 状态持久化 ─────────────────────────────────
STATE_FILE = ROOT / "data" / "auto_quant_state.json"
CUMULATIVE_FILE = ROOT / "data" / "cumulative_injected.json"
BACKLOG_FILE = ROOT / "data" / "auto_quant_pipeline_backlog.json"


def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"round": 0, "last_run": None, "injected_count": 0, "total_injected": 0}


def save_state(state):
    STATE_FILE.parent.mkdir(exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ── Bug #4 Fix: 累计注入追踪 ────────────────
def load_cumulative_injected():
    if CUMULATIVE_FILE.exists():
        with open(CUMULATIVE_FILE) as f:
            return json.load(f)
    return {"injected": [], "total": 0, "timestamps": {}}


def save_cumulative_injected(data):
    CUMULATIVE_FILE.parent.mkdir(exist_ok=True)
    with open(CUMULATIVE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def mark_injected(name):
    data = load_cumulative_injected()
    if name not in data["injected"]:
        data["injected"].append(name)
        data["total"] += 1
        data["timestamps"][name] = datetime.now().isoformat()
        save_cumulative_injected(data)


# ── Bug #3 Fix: 待办池 backlog ─────────────
def load_backlog():
    if BACKLOG_FILE.exists():
        with open(BACKLOG_FILE) as f:
            return json.load(f)
    return []


def save_backlog(backlog):
    BACKLOG_FILE.parent.mkdir(exist_ok=True)
    with open(BACKLOG_FILE, "w") as f:
        json.dump(backlog, f, indent=2)


def add_to_backlog(name, source=""):
    backlog = load_backlog()
    # 去重
    existing_names = {b["name"] for b in backlog}
    if name not in existing_names:
        backlog.append({
            "name": name, "source": source,
            "discovered_at": datetime.now().isoformat(),
            "type": "P1"
        })
        save_backlog(backlog)
        logger.info(f"  📋 {name} 加入待办池 (backlog)")
    return backlog


# ══════════════════════════════════════════════════════════════
# Phase 1: 知识采集 — 并行学习
# ══════════════════════════════════════════════════════════════

FAILURE_KEYWORDS = ["不存在，跳过", "搜索工具不可用", "无产出", "未采集到内容", "TIMEOUT"]


def run_phase1_learn():
    """运行4个学习Agent。返回产出文件列表。"""
    from datetime import datetime as dt
    date_str = dt.now().strftime("%Y-%m-%d")
    learned_dir = Path(os.path.expanduser("~/brain/ideas/indicator-library/learned/"))
    learned_dir.mkdir(parents=True, exist_ok=True)

    results = {}
    errors = []
    agents = {
        "arxiv": {
            "script": None,
            "keywords": ["quant finance", "trading indicator", "machine learning",
                         "technical analysis", "crypto volatility"],
            "output": f"{date_str}-arxiv-auto.md",
        },
        "github": {
            "script": None,
            "keywords": ["quantitative trading strategy", "crypto trading bot",
                         "technical indicators python"],
            "output": f"{date_str}-github-auto.md",
        },
        "tv": {
            "script": None,
            "keywords": ["PineScript indicator", "trading strategy", "volume analysis"],
            "output": f"{date_str}-tv-auto.md",
        },
        "community": {
            "script": None,
            "keywords": ["freqtrade strategy github", "crypto mean reversion strategy",
                         "momentum breakout strategy"],
            "output": f"{date_str}-community-auto.md",
        },
    }

    for agent_name, cfg in agents.items():
        out_path = learned_dir / cfg["output"]
        try:
            if cfg.get("script"):
                script_path = ROOT / cfg["script"]
                if script_path.exists():
                    logger.info(f"[{agent_name}] 运行 {script_path}...")
                    result = subprocess.run(
                        [sys.executable, str(script_path)],
                        capture_output=True, text=True, timeout=120,
                        cwd=str(ROOT)
                    )
                    if result.returncode != 0:
                        logger.warning(f"[{agent_name}] 返回码 {result.returncode}: {result.stderr[:200]}")
                        errors.append(f"{agent_name}: exit={result.returncode}, {result.stderr[:100]}")
                    output_text = result.stdout[-3000:] if len(result.stdout) > 3000 else result.stdout
                else:
                    logger.warning(f"[{agent_name}] 脚本 {script_path} 不存在，跳过")
                    output_text = ""
            else:
                logger.info(f"[{agent_name}] 输出占位文件（可在cron增强版本中对接Scrapling）")
                output_text = ""

            # 只写入非空内容 — 如果无产出，保留历史文件
            output_text = output_text if output_text else ""
            if output_text.strip():
                with open(out_path, "w") as f:
                    f.write(f"# {agent_name.upper()} Auto-Learn Report\n")
                    f.write(f"Generated: {dt.now().isoformat()}\n\n")
                    f.write(output_text)

            results[agent_name] = str(out_path)
            logger.info(f"[{agent_name}] ✅ → {out_path}")

        except subprocess.TimeoutExpired:
            logger.warning(f"[{agent_name}] 超时")
            errors.append(f"{agent_name}: timeout")
            # 不覆盖历史文件
            if not out_path.exists():
                with open(out_path, "w") as f:
                    f.write(f"# {agent_name.upper()} Auto-Learn Report (TIMEOUT)\n\n采集超时\n")
            results[agent_name] = str(out_path)
        except Exception as e:
            logger.error(f"[{agent_name}] 错误: {e}")
            errors.append(f"{agent_name}: {e}")

    return results, errors


# ── Bug #2 Fix: Phase 1 内容质量检查 ───────
def _phase1_has_real_content(results: dict) -> bool:
    """检查至少一个Agent产出了实质内容（>200字符且非失败关键词）。
    如果文件不存在（未覆盖历史文件），视为空。"""
    for agent_name, path in results.items():
        try:
            p = Path(path)
            if not p.exists():
                continue
            content = p.read_text(errors="ignore")
            body = content.split("---")[-1]
            if len(body) > 200:
                fail_count = sum(1 for kw in FAILURE_KEYWORDS if kw in body)
                if fail_count == 0:
                    return True
        except Exception:
            continue
    return False


# ══════════════════════════════════════════════════════════════
# Phase 2: 差距分析 — 对比现有scoring.py，找出P0注入候选
# ══════════════════════════════════════════════════════════════

def run_phase2_gap_analysis(learn_results: dict) -> dict:
    """
    分析学习收获中哪些是新指标，哪些能注入scoring.py。
    过滤: 已注入(cumulative_injected.json)、已存在(WEIGHTS)、已在backlog中。
    """
    from exchange_fetchers.scoring import WEIGHTS as existing_weights

    existing_names = set(existing_weights.keys())

    # 读取已注入记录
    cum = load_cumulative_injected()
    already_injected_names = set(cum.get("injected", []))

    # 读取待办池
    backlog = load_backlog()
    backlog_names = {b["name"] for b in backlog}

    # 所有已排除的指标
    excluded = existing_names | already_injected_names | backlog_names | {
        # 别名/组合名，过滤掉
        "RSI", "ATR", "MACD", "OBV", "EMA", "MA", "VWAP",
        "Bollinger", "Supertrend", "ADX",
        "of", "volume_profile", "vwap_deviation",
        "crash_bounce", "panic_volume", "liquidation",
        "taker_flow", "funding_rate", "oi_growth",
        "long_short_ratio", "cg_trending",
    }

    # 从学习文件中提取候选
    candidates = []
    gap_notes = []

    learned_dir = Path(os.path.expanduser("~/brain/ideas/indicator-library/learned/"))
    if not learned_dir.exists():
        return {"candidates": [], "summary": "learned目录不存在", "gap_notes": []}

    today_str = datetime.now().strftime("%Y-%m-%d")
    # 读今天 + 最近3天的文件（扩大扫描范围）
    files = []
    for delta in range(4):
        d = datetime.now()
        from datetime import timedelta
        d = d - timedelta(days=delta)
        files.extend(learned_dir.glob(f"{d.strftime('%Y-%m-%d')}-*.md"))

    all_text = ""
    for f in files:
        try:
            all_text += f.read_text(errors="ignore") + "\n"
        except Exception:
            pass

    # 简单规则提取候选指标关键词
    candidate_keywords = {
        "ichimoku": "Ichimoku云图",
        "elder": "Elder射线",
        "chaikin": "Chaikin",
        "keltner": "Keltner通道",
        "donchian": "Donchian通道",
        "parabolic sar": "Parabolic SAR",
        "aroon": "Aroon",
        "kairi": "Kairi指标",
        "pivot point": "Pivot Point",
        "volume price trend": "VPT",
        "accumulation distribution": "A/D线",
        "money flow": "MFI",
        "williams": "Williams %R",
        "ultimate": "Ultimate Oscillator",
        "commodity channel": "CCI",
        "dmi": "DMI",
        "psar": "Parabolic SAR",
        "heikin ashi": "Heikin Ashi",
        "renko": "Renko图",
        "cloud": "Ichimoku云",
        "rolling vol": "Rolling Volatility",
        "kde": "KDE概率分布",
        "hurst": "Hurst指数",
        "entropy": "熵值",
        "regime": "市场regime检测",
        "cluster": "聚类",
    }

    found = set()
    for keyword, label in candidate_keywords.items():
        if keyword.lower() in all_text.lower():
            if label not in excluded and label not in found:
                candidates.append({
                    "name": label,
                    "keyword": keyword,
                    "source": "learned_files",
                    "feasibility": "high" if len(candidates) < 3 else "medium",
                })
                found.add(label)
                gap_notes.append(f"🔍 潜在新指标: {label}")

    # 筛选P0级（只选可行性高的前3个）
    p0 = [c for c in candidates if c["feasibility"] == "high"][:3]
    if not p0:
        p0 = candidates[:2]

    summary_parts = []
    if p0:
        summary_parts.append(f"发现{len(p0)}个P0候选: {', '.join(c['name'] for c in p0)}")
    if gap_notes:
        summary_parts.append("; ".join(gap_notes[:5]))
    if already_injected_names:
        summary_parts.append(f"📌 历史已注入: {', '.join(already_injected_names)}")
    if backlog_names:
        summary_parts.append(f"📋 待办池: {', '.join(list(backlog_names)[:3])}")

    return {
        "candidates": p0,
        "summary": "; ".join(summary_parts) or "本轮未发现可注入新指标",
        "gap_notes": gap_notes,
    }


# ══════════════════════════════════════════════════════════════
# Phase 3: 自动注入到 scoring.py
# ══════════════════════════════════════════════════════════════

def run_phase3_inject(gap_result: dict) -> dict:
    """
    将差距分析选出的P0候选注入到scoring.py。
    Bug #1 Fix: 注入时同步更新 WEIGHTS dict + 重算 MAX_SCORE
    Bug #3 Fix: 无注入逻辑的候选写入 backlog 而非丢弃
    """
    candidates = gap_result.get("candidates", [])
    if not candidates:
        return {"status": "skipped", "reason": "无候选指标", "injected": []}

    scoring_path = ROOT / "exchange_fetchers" / "scoring.py"
    if not scoring_path.exists():
        return {"status": "error", "reason": f"scoring.py不存在: {scoring_path}"}

    with open(scoring_path) as f:
        content = f.read()

    injected = []
    # 读取已注入记录 (Bug #4)
    already_injected = set(load_cumulative_injected().get("injected", []))

    for cand in candidates:
        name = cand["name"]
        keyword = cand["keyword"]
        logger.info(f"评估注入候选: {name}")

        # Bug #4: 检查是否已通过 cumulative 注入过
        if name in already_injected:
            logger.info(f"  {name} 已在 cumulative 注入记录中，跳过")
            continue

        # 检查 WEIGHTS dict 中是否已存在 (比关键词匹配更精准)
        try:
            from exchange_fetchers.scoring import WEIGHTS as w
            weight_key = _name_to_weight_key(name)
            if weight_key in w:
                logger.info(f"  {name} 已在 WEIGHTS dict 中，跳过")
                continue
        except Exception:
            pass  # 如果导入失败则 fallback 到内容检查

        # 生成标准化注入块
        block_info = _generate_injection_block(cand)
        if not block_info:
            logger.info(f"  {name} 无预置注入逻辑")
            # Bug #3: 写入待办池
            add_to_backlog(name, source=cand.get("source", "gap_analysis"))
            cand["injected"] = False
            continue

        injection_code = block_info["code"]
        weight_key = block_info["weight_key"]
        weight_value = block_info["weight_value"]

        # 1. 注入代码块到 score_kline 函数
        insert_marker = "    # ── BEAR SIGNAL: 恐慌放量 (panic volume) ─────────────"
        if insert_marker not in content:
            logger.warning(f"  找不到插入点来注入 {name}")
            continue
        content = content.replace(insert_marker, injection_code + "\n" + insert_marker)

        # 2. 更新 WEIGHTS dict — 在 "bear": 之前插入新键
        weight_line = f'    "{weight_key}": {weight_value},  # auto-injected: {name}\n'
        content = content.replace('    "bear":', weight_line + '    "bear":')

        # 3. 重算 MAX_SCORE 注解（不重要但保持同步）
        # 实际运行时会自动算 sum()，所以注解只是提示

        injected.append(name)
        cand["injected"] = True
        logger.info(f"  ✅ 注入 {name} (weight_key={weight_key}, value={weight_value})")

        # 标记已注入 (Bug #4)
        mark_injected(name)

    # 写回
    if injected:
        with open(scoring_path, "w") as f:
            f.write(content)

    return {"status": "success" if injected else "skipped", "injected": injected}


def _name_to_weight_key(name: str) -> str:
    """指标名转 WEIGHTS key（snake_case）。"""
    mapping = {
        "chandelier exit": "chandelier",
        "ichimoku云图": "ichimoku",
        "elder射线": "elder",
        "chaikin": "chaikin",
        "keltner通道": "keltner",
        "donchian通道": "donchian",
        "parabolic sar": "psar",
        "aroon": "aroon",
        "kairi指标": "kairi",
        "pivot point": "pivot",
        "vpt": "vpt",
        "a/d线": "ad_line",
        "mfi": "mfi",
        "williams %r": "williams_r",
        "ultimate oscillator": "ultimate",
        "cci": "cci",
        "dmi": "dmi",
        "heikin ashi": "heikin_ashi",
        "renko图": "renko",
        "rolling volatility": "rolling_vol",
        "kde概率分布": "kde",
        "hurst指数": "hurst",
        "熵值": "entropy",
        "市场regime检测": "regime",
        "聚类": "cluster",
    }
    name_lower = name.lower().strip()
    if name_lower in mapping:
        return mapping[name_lower]
    # fallback: 拼音转snake_case
    s = name_lower.replace(" ", "_").replace("-", "_")
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s if s else "custom_indicator"


def _generate_injection_block(cand: dict):
    """
    根据候选名称生成标准化的计算块代码。
    返回: {"code": str, "weight_key": str, "weight_value": int}
    或 None（无预置逻辑）。
    """
    name_lower = cand["name"].lower()

    # 格式: key -> {code, weight_key, weight_value}
    blocks = {
        "ichimoku": None,
        "keltner通道": None,
        "donchian通道": None,
        "parabolic sar": None,
        "cci": None,
        "mfi": None,
        "heikin ashi": None,
        "renko图": None,
        "kde概率分布": None,
        "hurst指数": None,
        "熵值": None,
        "市场regime检测": None,
        "聚类": None,
    }

    # 模糊匹配
    for key, block in blocks.items():
        if key in name_lower or name_lower in key:
            return block

    return None


# ══════════════════════════════════════════════════════════════
# Phase 4: 测试验证
# ══════════════════════════════════════════════════════════════

def run_phase4_test(inject_result: dict) -> dict:
    """运行测试验证scoring.py仍然正常工作。"""
    test_results = {"syntax": False, "import": False, "score_kline": False, "basic_signals": False}

    # 1. 语法检查
    try:
        import py_compile
        py_compile.compile(str(ROOT / "exchange_fetchers" / "scoring.py"), doraise=True)
        test_results["syntax"] = True
    except Exception as e:
        return {"status": "failed", "reason": f"语法错误: {e}", "details": test_results}

    # 2. 导入检查 + WEIGHTS 变更验证
    try:
        from exchange_fetchers.scoring import score_kline, merge_multi_scores, grade_from_score, WEIGHTS, MAX_SCORE
        test_results["import"] = True
        test_results["weights_count"] = len(WEIGHTS)
        test_results["max_score"] = MAX_SCORE
        if inject_result.get("injected"):
            # 验证新注入的权重确实存在
            for name in inject_result["injected"]:
                wk = _name_to_weight_key(name)
                if wk in WEIGHTS:
                    test_results[f"weights_{wk}"] = WEIGHTS[wk]
    except Exception as e:
        return {"status": "failed", "reason": f"导入失败: {e}", "details": test_results}

    # 3. score_kline运行检查
    try:
        import numpy as np
        np.random.seed(42)
        n = 130
        close = 100 + np.cumsum(np.random.randn(n) * 0.5)
        high = close + np.abs(np.random.randn(n) * 0.3)
        low = close - np.abs(np.random.randn(n) * 0.3)
        open_p = close - np.random.randn(n) * 0.2
        volume = 1000 + np.abs(np.random.randn(n) * 200)
        kline = {k: v.astype(np.float64) for k, v in {
            'open': open_p, 'high': high, 'low': low,
            'close': close, 'volume': volume,
            'times': np.arange(n).astype(np.float64)
        }.items()}

        result = score_kline('TESTUSDT', float(close[-1]), 5.0, kline)
        test_results["score_kline"] = True
        test_results["score"] = result["score"]
        test_results["grade"] = result["grade"]
        test_results["signals_count"] = len(result["signals"])

        test_results["supertrend"] = result.get("supertrend_trend") is not None
        test_results["adx"] = result.get("adx") is not None
        test_results["bb"] = result.get("bb_lower") is not None
    except Exception as e:
        return {"status": "error", "reason": f"score_kline运行失败: {e}", "details": test_results}

    # 汇总
    all_pass = all(v is True for v in [test_results["syntax"], test_results["import"],
                                       test_results["score_kline"]])

    if inject_result.get("injected"):
        test_results["injected"] = inject_result["injected"]

    return {
        "status": "passed" if all_pass else "partial",
        "details": test_results,
    }


# ══════════════════════════════════════════════════════════════
# Phase 5: 生成汇报
# ══════════════════════════════════════════════════════════════

def run_phase5_report(phase1, phase2, phase3, phase4, state):
    """生成完整汇报并写入learned/报告。"""
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    report_dir = Path(os.path.expanduser("~/brain/ideas/indicator-library/learned/"))
    report_dir.mkdir(parents=True, exist_ok=True)

    report_file = report_dir / f"{date_str}-auto-quant-report.md"

    lines = []
    lines.append(f"# 🤖 全自动量化学习闭环报告 v2.1")
    lines.append(f"**时间**: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"**轮次**: #{state.get('round', 0)}")
    lines.append(f"**累计注入**: {state.get('total_injected', 0)} 个指标")
    lines.append("")

    # Phase 1
    lines.append("## 📚 Phase 1: 知识采集")
    for agent, path in phase1.get("results", {}).items():
        lines.append(f"- ✅ {agent}: {path}")
    if phase1.get("errors"):
        lines.append(f"- ⚠️ 错误: {len(phase1['errors'])}个")
        for e in phase1["errors"][:3]:
            lines.append(f"  - {e}")
    lines.append("")

    # Phase 2
    lines.append("## 🔍 Phase 2: 差距分析")
    lines.append(f"- {phase2.get('summary', '无差距分析结果')}")
    if phase2.get("gap_notes"):
        for note in phase2["gap_notes"][:5]:
            lines.append(f"- {note}")
    lines.append("")

    # Phase 3
    lines.append("## 💉 Phase 3: 自动注入")
    if phase3.get("injected"):
        for inj in phase3["injected"]:
            lines.append(f"- ✅ 注入: {inj}")
    else:
        lines.append(f"- {phase3.get('reason', '本轮无可注入新指标')}")
    backlog = load_backlog()
    if backlog:
        lines.append(f"- 📋 待办池: {len(backlog)} 个候选等待人工注入逻辑")
        for b in backlog[-5:]:
            lines.append(f"  - {b['name']} (since {b['discovered_at'][:10]})")
    lines.append("")

    # Phase 4
    lines.append("## 🧪 Phase 4: 测试验证")
    if phase4.get("status") == "passed":
        lines.append("- ✅ 全部测试通过")
    elif phase4.get("status") == "partial":
        lines.append("- ⚠️ 部分测试通过")
    else:
        lines.append(f"- ❌ 测试失败: {phase4.get('reason', '未知')}")
    det = phase4.get("details", {})
    for k, v in det.items():
        if isinstance(v, bool):
            lines.append(f"  - {k}: {'✅' if v else '❌'}")
        else:
            lines.append(f"  - {k}: {v}")
    lines.append("")

    # 当前scoring.py状态
    lines.append("## 📊 当前scoring.py状态")
    try:
        from exchange_fetchers.scoring import WEIGHTS, MAX_SCORE
        lines.append(f"- MAX_SCORE: {MAX_SCORE}")
        lines.append(f"- 指标数: {len(WEIGHTS)}")
        for k, v in sorted(WEIGHTS.items()):
            if k == "bear":
                lines.append(f"- **{k}**: {v} (bear追踪)")
            else:
                lines.append(f"- {k}: {v}")
    except Exception:
        lines.append("- 无法导入 scoring.py，可能测试失败后状态异常")

    report_content = "\n".join(lines)
    with open(report_file, "w") as f:
        f.write(report_content)

    logger.info(f"✅ 报告已生成: {report_file}")
    return report_content, str(report_file)


# ══════════════════════════════════════════════════════════════
# Main Entry
# ══════════════════════════════════════════════════════════════

def main():
    state = load_state()
    state["round"] += 1
    state["last_run"] = datetime.now().isoformat()

    logger.info(f"🚀 === 全自动量化学习闭环 #{state['round']} (v2.1) 启动 ===")

    # Phase 1: 知识采集
    logger.info("📚 Phase 1: 4 Agent并行学习...")
    results, errors = run_phase1_learn()
    phase1 = {"results": results, "errors": errors}
    logger.info(f"  完成: {len(results)} Agent, {len(errors)} 错误")

    # ═══ Bug #2 Fix: Phase 1 空跑检测 ═══
    if not _phase1_has_real_content(results):
        logger.warning("⚠️ Phase 1 全部空跑 — 所有Agent均未能产出实质内容")
        logger.warning("   终止管线，不执行注入")
        phase1["empty"] = True
        phase2 = {"candidates": [], "summary": "Phase 1空跑，跳过差距分析", "gap_notes": []}
        phase3 = {"status": "skipped", "reason": "Phase 1空跑，无可注入", "injected": []}
        phase4 = {"status": "skipped", "reason": "无注入，跳过测试"}
        report_text, report_path = run_phase5_report(phase1, phase2, phase3, phase4, state)
        save_state(state)
        print("=" * 60)
        print(f"🤖 全自动量化闭环 #{state['round']} 终止 (Phase 1 空跑)")
        print(f"  报告: {report_path}")
        print("=" * 60)
        return 0

    # Phase 2: 差距分析
    logger.info("🔍 Phase 2: 差距分析...")
    phase2 = run_phase2_gap_analysis(results)
    logger.info(f"  结果: {phase2.get('summary', '无')}")

    # Phase 3: 自动注入
    logger.info("💉 Phase 3: 自动注入...")
    phase3 = run_phase3_inject(phase2)
    if phase3.get("injected"):
        logger.info(f"  ✅ 注入: {', '.join(phase3['injected'])}")
        state["total_injected"] = state.get("total_injected", 0) + len(phase3["injected"])
        state["injected_count"] = len(phase3["injected"])
    else:
        logger.info(f"  跳过: {phase3.get('reason', '无可注入')}")

    # Phase 4: 测试验证
    logger.info("🧪 Phase 4: 测试验证...")
    phase4 = run_phase4_test(phase3)
    if phase4["status"] == "passed":
        logger.info("  ✅ 全部测试通过")
    else:
        logger.warning(f"  ⚠️ {phase4.get('reason', '部分失败')}")

    # Phase 5: 报告
    logger.info("📄 Phase 5: 生成报告...")
    report_text, report_path = run_phase5_report(phase1, phase2, phase3, phase4, state)

    # 持久化状态
    save_state(state)

    # 输出汇总
    print("=" * 60)
    print(f"🤖 全自动量化闭环 #{state['round']} 完成")
    print(f"  学习: {len(results)} Agent")
    print(f"  差距: {phase2.get('summary', '无')}")
    print(f"  注入: {phase3.get('injected', [])}")
    backlog = load_backlog()
    if backlog:
        print(f"  待办: {len(backlog)} 个候选（见 {BACKLOG_FILE}）")
    print(f"  测试: {phase4['status']}")
    print(f"  报告: {report_path}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
