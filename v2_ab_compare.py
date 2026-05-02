#!/usr/bin/env python3
"""
AB对比：baseline（去掉v2优化）vs optimized（当前版本）
完全自包含，用 OKX 4H 数据回测 top volume 币种
"""
import os, sys, json, re, logging, time
import numpy as np
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from exchange_fetchers.scoring import score_kline as score_kline_opt
from exchange_fetchers.scoring import grade_from_score
from exchange_fetchers import fetch_okx_kline, fetch_okx_tickers

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("ab-test")

LOOKAHEAD = 4
TRAIN_BARS = 100
TRAIN_SLACK = 20

def slice_kline(kline, end):
    return {k: v[:end+1] if isinstance(v, np.ndarray) else v for k, v in kline.items()}

def future_return(closes, i, n):
    target = min(i + n, len(closes) - 1)
    return 0.0 if target <= i else float((closes[target] - closes[i]) / closes[i]) * 100

def max_future_return(closes, i, n):
    target = min(i + n, len(closes) - 1)
    if target <= i: return 0.0
    return float((max(closes[i:target+1]) - closes[i]) / closes[i]) * 100

def min_future_return(closes, i, n):
    target = min(i + n, len(closes) - 1)
    if target <= i: return 0.0
    return float((min(closes[i:target+1]) - closes[i]) / closes[i]) * 100


def score_kline_baseline(symbol, close, change_24h, kline, vol_24h=0, hours_since_listing=9999, extras=None):
    """Baseline: optimized minus v2 additions."""
    result = score_kline_opt(symbol, close, change_24h, kline, vol_24h, hours_since_listing=hours_since_listing, extras=extras)
    
    score = result["score"]
    
    # 1) Undo Bonus 4/5/6
    for d in result.get("details", []):
        if "Supertrend+ADX趋势共振" in d:
            score = max(0, score - 4)
        elif "SuperTrend+EMA双金叉共振" in d:
            score = max(0, score - 6)
        elif "OFI买压+突破共振" in d:
            score = max(0, score - 4)
        elif "OFI+BB/RSI共振" in d:
            m = re.search(r'\+(\d+)', d)
            if m: score = max(0, score - int(m.group(1)))
        elif "震荡市趋势减益" in d:
            m = re.search(r'-(\d+)', d)
            if m: score += int(m.group(1))
        elif "MA88偏离" in d and "不计分" in d:
            score = min(1000, score + 8)
    
    result["score"] = score
    result["details"] = [d for d in result.get("details", [])
                        if all(x not in d for x in [
                            "Supertrend+ADX趋势共振",
                            "SuperTrend+EMA双金叉共振",
                            "OFI买压+突破共振",
                            "OFI+BB/RSI共振",
                            "震荡市趋势减益",
                            "MA88偏离"])]
    result["grade"] = grade_from_score(score)
    return result


def run_backtest(scorer_func, label):
    logger.info(f"\n{'='*50}")
    logger.info(f"FULL BACKTEST — {label}")
    logger.info(f"{'='*50}")
    
    tickers = fetch_okx_tickers(min_vol=10_000_000)
    if not tickers:
        logger.error("Failed to fetch tickers!")
        return [], []
    
    top = sorted(tickers.items(), key=lambda x: -x[1].get("vol_usd", 0))[:15]
    logger.info(f"Top {len(top)} symbols: {', '.join(s for s,_ in top)}")
    
    all_results = []
    sym_stats = []
    
    for sym, info in top:
        base = sym.replace("USDT", "")
        iid = f"{base}-USDT-SWAP"
        
        logger.info(f"\n  {sym}...")
        kline = fetch_okx_kline(sym, iid, "4H")
        
        if not kline or kline.get("close") is None or len(kline["close"]) < TRAIN_BARS + LOOKAHEAD + TRAIN_SLACK:
            logger.warning(f"    SKIP: insufficient data ({len(kline.get('close',[])) if kline else 0} bars)")
            continue
        
        closes = kline["close"]
        volumes = kline["volume"]
        n = len(closes)
        
        results = []
        for i in range(TRAIN_BARS, n - LOOKAHEAD):
            curr_kline = slice_kline(kline, i)
            sc = scorer_func(sym, closes[i], 0, curr_kline, volumes[i], hours_since_listing=9999)
            
            if sc.get("grade") not in ("A", "B", "C"):
                continue
            
            results.append({
                "bar": i,
                "score": sc["score"],
                "grade": sc["grade"],
                "signals": sc.get("signals", []),
                "bonus": sc.get("bonus", 0),
                "ret_4": future_return(closes, i, 4),
                "ret_8": future_return(closes, i, 8),
                "max_gain_4": max_future_return(closes, i, 4),
                "max_loss_4": -min_future_return(closes, i, 4),
            })
        
        wins_8 = sum(1 for r in results if r["ret_8"] > 0)
        n_sigs = len(results)
        avg_ret = round(sum(r["ret_8"] for r in results)/n_sigs, 2) if n_sigs else 0
        
        sym_stats.append({
            "sym": sym,
            "signals": n_sigs,
            "wins_8": wins_8,
            "wr_8": round(wins_8/n_sigs*100, 1) if n_sigs else 0,
            "avg_ret_8": avg_ret,
        })
        all_results.extend(results)
        
        logger.info(f"    {n_sigs} signals, WR={wins_8/n_sigs*100:.1f}% (8bar), avg_ret={avg_ret:.2f}%" if n_sigs else "    0 signals")
    
    return all_results, sym_stats


if __name__ == "__main__":
    print(f"Auto-fetching top-15 OKX symbols (4H)...")
    
    baseline_results, baseline_sym = run_backtest(score_kline_baseline, "BASELINE (优化前)")
    print(f"\n--- 等待 2s 保证API速率 ---")
    time.sleep(2)
    opt_results, opt_sym = run_backtest(score_kline_opt, "OPTIMIZED (v2)")

    # ── Report ──
    print("\n\n" + "=" * 70)
    print("  📊 AB 对比报告 — Baseline vs Optimized v2")
    print("=" * 70)
    
    def print_stats(results, sym_stats, label):
        if not results:
            print(f"\n  ❌ {label}: 无信号数据")
            return
        
        wins = sum(1 for r in results if r["ret_8"] > 0)
        total = len(results)
        wr = wins/total*100
        avg_ret = sum(r["ret_8"] for r in results)/total
        avg_bonus = sum(r["bonus"] for r in results)/total
        
        a_sigs = [r for r in results if r["grade"] == "A"]
        b_sigs = [r for r in results if r["grade"] == "B"]
        c_sigs = [r for r in results if r["grade"] == "C"]
        
        def grade_line(sigs, label):
            if sigs:
                wr = sum(1 for r in sigs if r['ret_8']>0)/len(sigs)*100
                ret = sum(r['ret_8'] for r in sigs)/len(sigs)
                return f"  │ {label}: {len(sigs):>4d}  WR={wr:.1f}% avg_ret={ret:+.2f}%{' ' * (20-len(sigs))}│"
            return f"  │ {label}:    0  {'N/A':>20}                 │"
        
        print(f"\n  {label}:")
        print(f"  ┌──────────────────────────────────────────┐")
        print(f"  │ 总信号:   {total:>4d}                          │")
        print(grade_line(a_sigs, "A级"))
        print(grade_line(b_sigs, "B级"))
        print(grade_line(c_sigs, "C级"))
        print(f"  │ 胜率(8b): {wr:>5.1f}%                        │")
        print(f"  │ 均收益:   {avg_ret:>+5.2f}%                        │")
        print(f"  │ 均值bonus:{avg_bonus:>5.1f}                        │")
        print(f"  │ 币种数:   {len(sym_stats):>4d}                        │")
        print(f"  └──────────────────────────────────────────┘")
    
    print_stats(baseline_results, baseline_sym, "BASELINE (去v2)")
    print_stats(opt_results, opt_sym, "OPTIMIZED v2")
    
    if baseline_results and opt_results:
        b_wr = sum(1 for r in baseline_results if r["ret_8"] > 0)/len(baseline_results)*100
        o_wr = sum(1 for r in opt_results if r["ret_8"] > 0)/len(opt_results)*100
        b_ret = sum(r["ret_8"] for r in baseline_results)/len(baseline_results)
        o_ret = sum(r["ret_8"] for r in opt_results)/len(opt_results)
        
        print("\n  ── CORE METRICS ──")
        print(f"  信号量:     {len(baseline_results):>4d}  →  {len(opt_results):>4d}  ({len(opt_results)-len(baseline_results):+d})")
        print(f"  胜率(8b):   {b_wr:>5.1f}%  →  {o_wr:>5.1f}%  ({o_wr-b_wr:+.1f}%)")
        print(f"  胜率(4b):   {sum(1 for r in baseline_results if r['ret_4']>0)/len(baseline_results)*100:.1f}%  →  {sum(1 for r in opt_results if r['ret_4']>0)/len(opt_results)*100:.1f}%")
        print(f"  平均收益:   {b_ret:>+5.2f}%  →  {o_ret:>+5.2f}%  ({o_ret-b_ret:+.2f}%)")
        print(f"  平均bonus:  {sum(r['bonus'] for r in baseline_results)/len(baseline_results):.1f}  →  {sum(r['bonus'] for r in opt_results)/len(opt_results):.1f}")
        
        # A-level comparison
        b_a = [r for r in baseline_results if r["grade"] == "A"]
        o_a = [r for r in opt_results if r["grade"] == "A"]
        
        if b_a and o_a:
            b_a_wr = sum(1 for r in b_a if r["ret_8"]>0)/len(b_a)*100
            o_a_wr = sum(1 for r in o_a if r["ret_8"]>0)/len(o_a)*100
            print(f"  A级Δ:      {b_a_wr:>5.1f}%  →  {o_a_wr:>5.1f}%  ({o_a_wr-b_a_wr:+.1f}%)")
        
        # Per-symbol table
        print("\n  ── BY SYMBOL (8-bar win rate) ──")
        base_map = {s["sym"]: s for s in baseline_sym}
        opt_map = {s["sym"]: s for s in opt_sym}
        
        header = f"  {'Symbol':<10} {'Base WR':>8} {'Opt WR':>8} {'ΔWR':>8} {'BaseRet':>8} {'OptRet':>8} {'ΔRet':>8}"
        print(header)
        print(f"  {'-'*len(header.strip())}")
        
        for sym in sorted(set(list(base_map.keys()) + list(opt_map.keys()))):
            b = base_map.get(sym)
            o = opt_map.get(sym)
            if b and o:
                print(f"  {sym:<10} {b['wr_8']:>6.1f}%({b['signals']:>2d}) {o['wr_8']:>6.1f}%({o['signals']:>2d}) {o['wr_8']-b['wr_8']:>+7.1f}% {b['avg_ret_8']:>+6.2f}% {o['avg_ret_8']:>+6.2f}% {o['avg_ret_8']-b['avg_ret_8']:>+7.2f}%")
            elif b:
                print(f"  {sym:<10} {b['wr_8']:>6.1f}%({b['signals']:>2d}) {'N/A':>8} {'N/A':>8} {b['avg_ret_8']:>+6.2f}%")
        
        # Bonus distribution
        print("\n  ── BONUS 分布变化 ──")
        # Count original bonuses (1-3) vs new bonuses (4-6)
        opt_bonus_count = {"1(动量+量能)": 0, "2(金叉+突破)": 0, "3(放量+动量)": 0, 
                          "4(SuperTrend+ADX)": 0, "5(SuperTrend+EMA)": 0, "6(OFI+突破)": 0}
        
        for r in opt_results:
            for d in r.get("details", []):
                if "动量+量能共振" in d: opt_bonus_count["1(动量+量能)"] += 1
                if "金叉+突破共振" in d: opt_bonus_count["2(金叉+突破)"] += 1
                if "放量+动量加速" in d: opt_bonus_count["3(放量+动量)"] += 1
                if "Supertrend+ADX趋势共振" in d: opt_bonus_count["4(SuperTrend+ADX)"] += 1
                if "SuperTrend+EMA双金叉共振" in d: opt_bonus_count["5(SuperTrend+EMA)"] += 1
                if "OFI买压+突破共振" in d: opt_bonus_count["6(OFI+突破)"] += 1
        
        for k, v in opt_bonus_count.items():
            print(f"    Bonus {k}: {v:>3d} 次 ({v/len(opt_results)*100:.1f}%)" if opt_results else "")
    
    # Save
    os.makedirs("data", exist_ok=True)
    result_data = {
        "baseline": {"total": len(baseline_results), "wins": sum(1 for r in baseline_results if r["ret_8"]>0), "wr": round(sum(1 for r in baseline_results if r["ret_8"]>0)/len(baseline_results)*100, 1) if baseline_results else 0},
        "optimized": {"total": len(opt_results), "wins": sum(1 for r in opt_results if r["ret_8"]>0), "wr": round(sum(1 for r in opt_results if r["ret_8"]>0)/len(opt_results)*100, 1) if opt_results else 0},
        "baseline_sym": baseline_sym,
        "opt_sym": opt_sym,
    }
    with open("data/v2_ab_compare.json", "w") as f:
        json.dump(result_data, f, indent=2, default=str)
    print(f"\n  详细数据 → data/v2_ab_compare.json")
