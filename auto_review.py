#!/usr/bin/env python3
"""
auto_review.py — 自主量化交易复盘引擎

Two-phase autonomous review:
  1. performance_tracker() — 每日信号质量统计
  2. miss_detector() — 识别哪些暴涨/暴跌币被系统漏了
  3. parameter_adjuster() — 基于复盘自动生成调参建议

Run:  python3 auto_review.py            # 完整复盘
       python3 auto_review.py --quick    # 只检查漏报（最快）
       python3 auto_review.py --report   # 输出日志到复盘报告

Schedule: every 4h via hermes cron
"""

import os, sys, json, logging, time
from datetime import datetime, timezone
from collections import defaultdict, Counter
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from exchange_fetchers import (
    fetch_okx_tickers, fetch_gate_tickers, fetch_bitget_tickers,
    fetch_okx_kline, fetch_gate_kline, fetch_bitget_kline,
)
from exchange_fetchers.scoring import score_kline, merge_multi_scores

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("auto_review")

DATA_DIR = "/root/binance-square-agent/data"
HISTORICAL_DIR = f"{DATA_DIR}/historical"
REVIEW_DB = f"{DATA_DIR}/review_db.json"
PARAM_DB = f"{DATA_DIR}/param_evolution.json"
SIGNAL_CACHE = f"{DATA_DIR}/surge_signals_cache.json"

os.makedirs(HISTORICAL_DIR, exist_ok=True)

# =========================================================
# 1. PERFORMANCE TRACKER — 统计信号质量
# =========================================================

def load_signal_cache() -> list:
    """Load last surge_scanner signal cache."""
    try:
        with open(SIGNAL_CACHE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def performance_tracker() -> dict:
    """
    Track current scan quality metrics:
    - A/B/C/D grade counts
    - top 10 symbols by composite score
    - signal types triggered
    """
    signals = load_signal_cache()
    if not signals:
        return {"status": "no_signals", "ts": datetime.now(timezone.utc).isoformat()}

    grades = Counter(r.get("grade", "?") for r in signals)
    signal_counts = Counter()
    pattern_counts = Counter()
    for r in signals:
        for s in r.get("signals", []):
            signal_counts[s] += 1
        for p in r.get("patterns", []):
            pattern_counts[p] += 1

    bear_signals = sum(1 for r in signals if r.get("has_bear"))
    
    # Top 10 by score
    sorted_sigs = sorted(signals, key=lambda x: x.get("score", 0), reverse=True)[:10]

    result = {
        "status": "ok",
        "ts": datetime.now(timezone.utc).isoformat(),
        "total_signals": len(signals),
        "grades": dict(grades),
        "bear_signals": bear_signals,
        "top_signals": [
            {"sym": s["sym"], "score": s.get("score", 0), "grade": s.get("grade", "?"), 
             "chg24h": s.get("chg24h", 0), "vol_24h": s.get("vol_24h", 0)}
            for s in sorted_sigs[:5]
        ],
        "top_signal_types": signal_counts.most_common(10),
        "top_patterns": pattern_counts.most_common(5),
    }
    return result


# =========================================================
# 2. MISS DETECTOR — 识别漏报币种
# =========================================================

def fetch_current_tickers() -> dict:
    """Gather big-movers across all 3 exchanges with a relaxed threshold."""
    candidates = {}
    
    for exchange_name, fetcher_fn in [
        ("okx", fetch_okx_tickers), 
        ("gate", fetch_gate_tickers), 
        ("bitget", fetch_bitget_tickers)
    ]:
        try:
            tickers = fetcher_fn()
            if not isinstance(tickers, dict):
                continue
            for sym, info in tickers.items():
                chg = abs(info.get("chg24h", 0) or 0)
                vol = info.get("vol_usd", 0) or 0
                
                # 宽松阈值: 涨跌幅>=5% 且 成交额>=10万
                if chg >= 5.0 and vol >= 100_000:
                    if sym not in candidates:
                        candidates[sym] = {"exchanges": [], "max_chg": 0, "total_vol": 0}
                    candidates[sym]["exchanges"].append(exchange_name)
                    candidates[sym]["max_chg"] = max(candidates[sym]["max_chg"], chg)
                    candidates[sym]["total_vol"] += vol
                    candidates[sym][f"chg_{exchange_name}"] = info.get("chg24h", 0)
                    candidates[sym][f"vol_{exchange_name}"] = vol
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", exchange_name, e)
    
    return candidates


def miss_detector() -> list:
    """
    Compare big-movers against surge signal cache.
    Returns list of missed coins with metadata.
    """
    logger.info("🔍 Running miss detector...")
    
    # Big movers from all exchanges
    candidates = fetch_current_tickers()
    logger.info("  Found %d big-mover candidates (|chg|>=5%%, vol>=100K)", len(candidates))
    
    # Currently signaled coins
    signaled = {r["sym"] for r in load_signal_cache()}
    
    missed = []
    for sym, info in sorted(candidates.items(), key=lambda x: x[1]["max_chg"], reverse=True):
        if sym not in signaled:
            missed.append({
                "sym": sym,
                "max_chg": info["max_chg"],
                "total_vol": info["total_vol"],
                "exchanges": info["exchanges"],
                "ex_detail": {k: v for k, v in info.items() if k.startswith("chg_") or k.startswith("vol_")},
            })
    
    # Sort by max_chg descending
    missed.sort(key=lambda x: x["max_chg"], reverse=True)
    
    if missed:
        logger.info("  ⚠️ %d coins MISSED by surge scanner:", len(missed))
        for m in missed[:10]:
            logger.info("    %s | chg=%+.1f%% | vol=$%.0f | ex=%s", 
                        m["sym"], m["max_chg"] * (1 if any(v>0 for k,v in m["ex_detail"].items() if k.startswith("chg_")) else -1),
                        m["total_vol"], "/".join(m["exchanges"]))
    else:
        logger.info("  ✅ All big movers captured")
    
    return missed


# =========================================================
# 3. PARAMETER ADJUSTER — 自动调参建议
# =========================================================

def load_review_history() -> list:
    try:
        with open(REVIEW_DB) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def save_review_history(history: list):
    with open(REVIEW_DB, "w") as f:
        json.dump(history, f, indent=2)

def load_param_history() -> dict:
    try:
        with open(PARAM_DB) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            "version": 1,
            "params": {
                "min_vol": 1000000,
                "min_vol_rapid": 200000,
                "rapid_chg": 15.0,
                "a_grade": 72,
                "b_grade": 52,
                "c_grade": 32,
                "vol_ratio_threshold": 2.0,
                "atr_mult_threshold": 1.2,
                "vol_penalty_threshold": 0.01,
                "vol_penalty_score": -6,
            },
            "adjustments": [],
            "miss_history": [],
        }

def save_param_history(param_data: dict):
    with open(PARAM_DB, "w") as f:
        json.dump(param_data, f, indent=2)


def analyze_miss_patterns(missed: list) -> dict:
    """Analyze what type of coins are being missed."""
    if not missed:
        return {"has_misses": False}
    
    # Why were they missed? Categorize
    low_vol = [m for m in missed if m["total_vol"] < 1_000_000]
    single_ex = [m for m in missed if len(m["exchanges"]) == 1]
    only_gate = [m for m in missed if m["exchanges"] == ["gate"]]
    extreme_chg = [m for m in missed if abs(m["max_chg"]) > 30]
    
    return {
        "has_misses": True,
        "total_missed": len(missed),
        "low_vol_<1M": len(low_vol),
        "single_exchange": len(single_ex),
        "only_gate": len(only_gate),
        "extreme_chg_>30pct": len(extreme_chg),
        "missed_syms": [m["sym"] for m in missed[:10]],
    }


def parameter_adjuster(missed: list) -> dict:
    """
    Generate parameter adjustment recommendations based on miss patterns.
    Rules:
      - If consistently missing low-vol (<1M) coins: consider lowering MIN_VOL_USDT
      - If consistently missing single-exchange coins: boost cross_exchange resonance
      - If consistently missing Gate-only coins: add Gate weight
      - If misses are extreme vol coins (30%+): add rapid breakout bonus
    """
    param = load_param_history()
    analysis = analyze_miss_patterns(missed)
    recommendations = []
    
    if not analysis.get("has_misses"):
        recommendations.append("✅ No misses — no adjustment needed")
        return {"recommendations": recommendations, "analysis": analysis}
    
    # Check miss patterns over last N reviews
    param["miss_history"].append({
        "ts": datetime.now(timezone.utc).isoformat(),
        "count": analysis["total_missed"],
        "syms": analysis.get("missed_syms", []),
    })
    # Keep last 10
    if len(param["miss_history"]) > 10:
        param["miss_history"] = param["miss_history"][-10:]
    
    # Persistence analysis: check for repeat offenders
    history = load_review_history()
    repeated = {}
    for sym in analysis.get("missed_syms", []):
        streak = sum(1 for h in history if sym in h.get("missed_syms", []))
        if streak >= 2:
            repeated[sym] = streak
    
    if repeated:
        logger.info("  🔴 %d coins with 2+ consecutive misses (structural blind spots):", len(repeated))
        for sym, streak in sorted(repeated.items(), key=lambda x: -x[1]):
            logger.info("    %s: missed %dx", sym, streak)
        recommendations.append(f"⚠️ {len(repeated)} coins missed 2+ times — requires scoring engine patch")
    
    # Pattern: repeated low-volume misses
    low_vol_count = sum(
        1 for h in param["miss_history"][-5:]
        if h["count"] > 0
    )
    
    if low_vol_count >= 3:
        # 连续漏报 — 建议放宽参数
        suggestions = []
        if analysis.get("low_vol_<1M", 0) >= analysis.get("total_missed", 0) * 0.5:
            suggestions.append(f"🔧 建议降低 MIN_VOL_USDT: {param['params']['min_vol']} → {int(param['params']['min_vol'] * 0.8)}")
        
        if analysis.get("single_exchange", 0) >= 2:
            suggestions.append("🔧 单所币种持续漏报，建议增加 Gate 单平台币独立检测通道（vol>=200K, chg>=15%）")
        
        if analysis.get("only_gate", 0) >= 2:
            suggestions.append("🔧 Gate 独占币持续漏报，建议加 Gate 独立评分通道避免被跨所共振机制忽视")
        
        recommendations.extend(suggestions)
        
        if suggestions:
            param["adjustments"].append({
                "ts": datetime.now(timezone.utc).isoformat(),
                "miss_count": analysis["total_missed"],
                "suggested": suggestions,
                "applied": False,
            })
    
    save_param_history(param)
    return {
        "recommendations": recommendations or ["无自动参数调优建议"],
        "analysis": analysis,
    }


# =========================================================
# 4. KLINE-BASED DEEP REVIEW — 对特定币种做K线级复盘
# =========================================================

def deep_review(sym: str, exchange: str = "bitget", tf: str = "4H") -> dict:
    """
    Deep K-line review for a specific coin.
    Score each bar, check if any pattern was missed.
    """
    logger.info("  🔬 Deep review: %s on %s %s", sym, exchange, tf)
    
    try:
        if exchange == "okx":
            inst_id = sym.replace("USDT", "-USDT-SWAP")
            kline = fetch_okx_kline(sym, inst_id, tf)
        elif exchange == "gate":
            contract = sym.replace("USDT", "_USDT")
            kline = fetch_gate_kline(contract, tf.lower())
        else:
            kline = fetch_bitget_kline(sym, tf)
        
        if not kline or len(kline.get("close", [])) < 50:
            return {"sym": sym, "error": "insufficient kline data"}
        
        closes = kline["close"]
        prices = [float(c) if hasattr(c, 'item') else c for c in closes[-50:]]
        
        # Simple pattern detection
        surges = []
        for i in range(2, len(prices)):
            prev = prices[i-2]
            curr = prices[i]
            chg = (curr - prev) / prev * 100
            if abs(chg) > 10:
                surges.append({"bar": i, "price": curr, "chg_pct": round(chg, 2)})
        
        return {
            "sym": sym,
            "exchange": exchange,
            "tf": tf,
            "bars_analyzed": len(closes),
            "recent_prices": prices[-10:],
            "surges_detected": surges,
            "volatility": round(np.std(prices) / np.mean(prices) * 100, 2) if prices else 0,
        }
    except Exception as e:
        return {"sym": sym, "error": str(e)}


# =========================================================
# MAIN
# =========================================================

def main():
    if "--quick" in sys.argv:
        # 快速模式：只跑漏报检测
        logger.info("=" * 60)
        logger.info("🚀 AUTO-REVIEW (quick mode) — Miss Detection Only")
        logger.info("=" * 60)
        
        missed = miss_detector()
        analysis = analyze_miss_patterns(missed)
        
        # Also save to review_db.json for persistence tracking
        hist = load_review_history()
        hist.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "missed_count": len(missed),
            "missed_syms": [m["sym"] for m in missed[:15]],
        })
        if len(hist) > 100:
            hist = hist[-100:]
        save_review_history(hist)
        
        report = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "mode": "quick",
            "miss_detection": {
                "missed_count": len(missed),
                "missed": [{"sym": m["sym"], "max_chg": m["max_chg"], "total_vol": m["total_vol"], "exchanges": m["exchanges"]} for m in missed[:15]],
            },
            "adjustments": {"analysis": analysis},
            "recommendations": [],
        }
        
        with open(f"{HISTORICAL_DIR}/review_latest.json", "w") as f:
            json.dump(report, f, indent=2)
        # Also save to top-level data/ for easy cron access
        with open(f"{DATA_DIR}/review_latest.json", "w") as f:
            json.dump(report, f, indent=2)
        
        logger.info("✅ Quick review saved")
        return report
    
    if "--report" in sys.argv:
        mode = "report"
    else:
        mode = "full"
    
    logger.info("=" * 60)
    logger.info("🚀 AUTO-REVIEW (%s mode)", mode)
    logger.info("=" * 60)
    
    # 1. Performance tracking
    perf = performance_tracker()
    logger.info("\n📊 Performance: %d total signals | Grades: %s", 
                perf.get("total_signals", 0), perf.get("grades", {}))
    
    # 2. Miss detection
    missed = miss_detector()
    
    # 3. Parameter adjustment
    adj = parameter_adjuster(missed)
    
    # 4. Deep review for any extreme misses
    deep_reviews = []
    for m in missed[:3]:
        if abs(m["max_chg"]) > 20:
            # Deep dive
            primary_ex = m["exchanges"][0] if m["exchanges"] else "bitget"
            dr = deep_review(m["sym"], primary_ex)
            deep_reviews.append(dr)
    
    # 5. Save report
    report = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "performance": {
            "total_signals": perf.get("total_signals", 0),
            "grades": perf.get("grades", {}),
            "top5": perf.get("top_signals", []),
        },
        "miss_detection": {
            "total_candidates": len(missed),  # approximated
            "missed_count": len(missed),
            "missed": missed[:10],
        },
        "adjustments": adj,
        "recommendations": adj.get("recommendations", []),
    }
    
    os.makedirs(HISTORICAL_DIR, exist_ok=True)
    with open(f"{HISTORICAL_DIR}/review_latest.json", "w") as f:
        json.dump(report, f, indent=2)
    with open(f"{DATA_DIR}/review_latest.json", "w") as f:
        json.dump(report, f, indent=2)
    
    # 累积复盘历史
    hist = load_review_history()
    hist.append({
        "ts": report["ts"],
        "missed_count": len(missed),
        "total_signals": perf.get("total_signals", 0),
        "grades": perf.get("grades", {}),
        "missed_syms": [m["sym"] for m in missed[:10]],
    })
    if len(hist) > 100:
        hist = hist[-100:]
    save_review_history(hist)
    
    logger.info("\n" + "=" * 60)
    logger.info("✅ REVIEW COMPLETE — %d missed, %d signals", len(missed), perf.get("total_signals", 0))
    if adj["recommendations"]:
        for r in adj["recommendations"]:
            logger.info("  %s", r)
    logger.info("=" * 60)
    
    return report


if __name__ == "__main__":
    report = main()
    
    # Output key findings for terminal (cron-compatible JSON)
    result = {
        "ts": report["ts"],
        "missed_count": report.get("miss_detection", {}).get("missed_count", 0),
        "missed_syms": [m["sym"] for m in report.get("miss_detection", {}).get("missed", [])],
        "recommendations": report.get("recommendations", []),
    }
    print("\n\n")
    print(json.dumps(result, indent=2))
