#!/usr/bin/env python3
"""
backtest_surge_v2.py — Multi-exchange surge scanner backtest.
Tests across OKX + Gate + Bitget, for top volume symbols.
Simulates the real merge_multi_scores pipeline.
"""

import os, sys, json, logging, time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from exchange_fetchers import (
    fetch_okx_tickers, fetch_gate_tickers, fetch_bitget_tickers,
    fetch_okx_kline, fetch_gate_kline, fetch_bitget_kline,
)
from exchange_fetchers.scoring import score_kline, grade_from_score, merge_multi_scores

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("bt_v2")

LOOKAHEAD = 4
TRAIN_BARS = 100
EXCHANGES = ["okx", "gate", "bitget"]
TIMEFRAMES = ["1H", "2H", "4H"]

# Instrument ID mapping
INST_MAP = {
    "okx": lambda s: s.replace("USDT", "-USDT-SWAP"),
    "gate": lambda s: s.replace("USDT", "_USDT"),
    "bitget": lambda s: s,  # direct
}

def fetch_single(sym: str, ex: str, tf: str):
    """Fetch kline for one (sym, exchange, timeframe)."""
    inst = INST_MAP[ex](sym)
    if ex == "okx":
        return fetch_okx_kline(sym, inst, tf)
    elif ex == "gate":
        return fetch_gate_kline(sym, inst, tf)
    return fetch_bitget_kline(sym, tf)

def backtest_symbol(sym: str) -> dict:
    """Run full backtest for one symbol across all (ex × tf) pairs.
    
    Returns dict with merged results + per-pair results.
    """
    # Fetch all kline data concurrently
    kline_cache = {}
    tasks = []
    
    with ThreadPoolExecutor(max_workers=12) as exe:
        for ex in EXCHANGES:
            for tf in TIMEFRAMES:
                future = exe.submit(fetch_single, sym, ex, tf)
                tasks.append((ex, tf, future))
        
        for ex, tf, future in tasks:
            try:
                kline = future.result(timeout=30)
                if kline and len(kline.get("close", [])) >= TRAIN_BARS + LOOKAHEAD:
                    kline_cache[(ex, tf)] = kline
            except Exception:
                pass

    if not kline_cache:
        return {"sym": sym, "pairs": 0, "signal_bars": [], "merged": []}

    # Per-pair walk-forward scoring
    pair_results = {}  # (ex,tf) -> list of dicts (one per bar)
    min_bars = min(len(k["close"]) for k in kline_cache.values())
    end_bar = min_bars - LOOKAHEAD
    
    # We'll score every bar from TRAIN_BARS to end_bar
    scored_bars = end_bar - TRAIN_BARS
    if scored_bars <= 0:
        return {"sym": sym, "pairs": len(kline_cache), "signal_bars": [], "merged": []}

    # For each bar index, build list of score dicts from all pairs
    merged_results = []
    
    for i in range(TRAIN_BARS, end_bar):
        # Score each pair at this bar
        individual_scores = []
        for (ex, tf), kline in kline_cache.items():
            c = kline["close"]
            bar_kline = {
                k: v[:i+1] if isinstance(v, np.ndarray) else v
                for k, v in kline.items()
            }
            try:
                sc = score_kline(
                    sym=sym,
                    price=float(c[i]),
                    chg24h=0.0,
                    kline=bar_kline,
                    vol_24h=0,
                )
                sc["exchange"] = ex
                sc["timeframe"] = tf
                individual_scores.append(sc)
            except Exception:
                continue
        
        if not individual_scores:
            continue
        
        # Merge
        merged = merge_multi_scores(individual_scores)
        
        # Forward returns
        # Use the OKX close for returns (or first available)
        primary_close = None
        for ex in EXCHANGES:
            for tf in TIMEFRAMES:
                if (ex, tf) in kline_cache:
                    primary_close = kline_cache[(ex, tf)]["close"]
                    break
            if primary_close:
                break
        
        if primary_close is None or i + LOOKAHEAD >= len(primary_close):
            continue
        
        def fr(n):
            t = min(i + n, len(primary_close) - 1)
            return round(float((primary_close[t] - primary_close[i]) / primary_close[i]) * 100, 2) if t > i else 0.0
        
        merged.update({
            "i": i,
            "ts": str(kline_cache[list(kline_cache.keys())[0]]["times"][i]),
            "price": float(primary_close[i]),
            "ret_1": fr(1),
            "ret_2": fr(2),
            "ret_4": fr(4),
            "ret_8": fr(LOOKAHEAD),
            "max_gain": round(float(np.max(primary_close[i+1:min(i+LOOKAHEAD+1, len(primary_close))]) - primary_close[i]) / primary_close[i] * 100, 2) if i+1 < len(primary_close) else 0.0,
            "max_loss": round(float(np.min(primary_close[i+1:min(i+LOOKAHEAD+1, len(primary_close))]) - primary_close[i]) / primary_close[i] * 100, 2) if i+1 < len(primary_close) else 0.0,
        })
        
        merged_results.append(merged)

    return {"sym": sym, "pairs": len(kline_cache), "signal_bars": merged_results}


def analyze(results_dict: dict):
    """Print comprehensive analysis."""
    all_sigs = []
    sym_summaries = []
    
    for sym, data in results_dict.items():
        merged = data["signal_bars"]
        if not merged:
            continue
        
        sigs = [r for r in merged if r.get("grade") in ("A", "B", "C")]
        if not sigs:
            continue
        
        wins = sum(1 for r in sigs if r["ret_8"] > 0)
        wr = wins / len(sigs) * 100
        avg_r = sum(r["ret_8"] for r in sigs) / len(sigs)
        
        sym_summaries.append((sym, len(sigs), wr, avg_r, data["pairs"]))
        all_sigs.extend(sigs)
    
    # Sort by sig count desc
    sym_summaries.sort(key=lambda x: -x[1])
    
    print(f"\n{'='*90}")
    print("📊  MULTI-EXCHANGE BACKTEST RESULTS  (OKX+Gate+Bitget × 1H/2H/4H)")
    print(f"      {len(sym_summaries)} symbols | {len(all_sigs)} total A/B/C signals")
    print('='*90)
    
    print(f"\n{'Symbol':>12} {'Pairs':>6} {'Signals':>8} {'Wins':>6} {'Win%':>7} {'AvgR+8':>9} {'MaxG':>7} {'MaxL':>7}")
    print("-" * 64)
    
    all_wins = 0
    all_sig_count = 0
    all_r_sum = 0.0
    
    for sym, cnt, wr, avg_r, pairs in sym_summaries:
        wins = int(cnt * wr / 100)
        sym_sigs = [r for r in results_dict[sym]["signal_bars"] if r.get("grade") in ("A", "B", "C")]
        max_g = max(r.get("max_gain", 0) for r in sym_sigs) if sym_sigs else 0
        max_l = min(r.get("max_loss", 0) for r in sym_sigs) if sym_sigs else 0
        print(f"{sym:>12} {pairs:>4d}    {cnt:>4d}    {wins:>4d}  {wr:>5.1f}%  {avg_r:>7.2f}%  {max_g:>6.2f}%  {max_l:>6.2f}%")
        all_wins += int(cnt * wr / 100)
        all_sig_count += cnt
        all_r_sum += avg_r * cnt
    
    if all_sig_count > 0:
        overall_wr = all_wins / all_sig_count * 100
        overall_r = all_r_sum / all_sig_count
        print("-" * 64)
        print(f"{'TOTAL':>12}        {all_sig_count:>4d}    {all_wins:>4d}  {overall_wr:>5.1f}%  {overall_r:>7.2f}%")
    
    # Signal quality by grade
    print(f"\n{'─'*60}")
    print("Signal Quality by Grade:")
    gs = {"A": [], "B": [], "C": []}
    for r in all_sigs:
        g = r.get("grade", "D")
        if g in gs:
            gs[g].append(r)
    
    print(f"{'Grade':>6} {'Count':>7} {'Win%':>7} {'AvgR+8':>9}")
    print("-" * 32)
    for g in ["A", "B", "C"]:
        sub = gs[g]
        if not sub:
            continue
        w = sum(1 for r in sub if r["ret_8"] > 0)
        ar = sum(r["ret_8"] for r in sub) / len(sub)
        print(f"{g:>6} {len(sub):>6d}  {w/len(sub)*100:>5.1f}%  {ar:>7.2f}%")
    
    # Signal type frequency
    print(f"\n{'─'*60}")
    print("Signal Type Frequency:")
    sc = Counter()
    for r in all_sigs:
        for s in r.get("signals", []):
            sc[s] += 1
    for s, c in sc.most_common(15):
        print(f"  {s:30s}: {c:3d}")
    
    # Best / worst symbols
    if sym_summaries:
        print(f"\n{'─'*60}")
        print(f"🏆 Top 3 by Win Rate (min 5 signals):")
        top3 = sorted([s for s in sym_summaries if s[1] >= 5], key=lambda x: -x[2])[:3]
        for sym, cnt, wr, ar, p in top3:
            print(f"  {sym:12s}: {cnt:2d} sigs  wr={wr:.1f}%  avg_r={ar:.2f}%")
        
        print(f"\n💀 Bottom 3 by Win Rate (min 5 signals):")
        btm3 = sorted([s for s in sym_summaries if s[1] >= 5], key=lambda x: x[2])[:3]
        for sym, cnt, wr, ar, p in btm3:
            print(f"  {sym:12s}: {cnt:2d} sigs  wr={wr:.1f}%  avg_r={ar:.2f}%")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=10, help="Top N volume symbols to test")
    parser.add_argument("--sym", help="Single symbol mode")
    args = parser.parse_args()
    
    t0 = time.monotonic()
    
    # Get top volume symbols from all 3 exchanges
    all_tickers = {}
    for ex_name, fetcher in [("okx", fetch_okx_tickers), ("gate", fetch_gate_tickers), ("bitget", fetch_bitget_tickers)]:
        try:
            t = fetcher(min_vol=0)
            for sym, info in t.items():
                if sym not in all_tickers:
                    all_tickers[sym] = 0
                all_tickers[sym] += info.get("vol_usd", 0)
        except Exception as e:
            print(f"  {ex_name} ticker fetch failed: {e}")
    
    # Sort by cross-exchange total volume
    top_syms = sorted(all_tickers.items(), key=lambda x: -x[1])
    
    if args.sym:
        syms_to_test = [args.sym]
    else:
        syms_to_test = [s for s, _ in top_syms[:args.top]]
    
    print(f"Testing {len(syms_to_test)} symbols...")
    print(f"Top 5 by volume: {', '.join(syms_to_test[:5])}")
    print()
    
    results = {}
    completed = 0
    
    for sym in syms_to_test:
        print(f"  [{completed+1}/{len(syms_to_test)}] {sym}...", end=" ", flush=True)
        try:
            data = backtest_symbol(sym)
            results[sym] = data
            sigs = len([r for r in data.get("signal_bars", []) if r.get("grade") in ("A", "B", "C")])
            if sigs:
                wins = sum(1 for r in data["signal_bars"] if r.get("grade") in ("A","B","C") and r["ret_8"] > 0)
                print(f"✅ {sigs} sigs, {wins/sigs*100:.0f}% wr  ({data['pairs']} pairs)", flush=True)
            else:
                print(f"✅ (no signals, {data['pairs']} pairs)", flush=True)
            completed += 1
        except Exception as e:
            print(f"❌ {e}", flush=True)
    
    cost = time.monotonic() - t0
    print(f"\n⏱️  Total: {cost:.0f}s ({cost/60:.1f}min)")
    
    analyze(results)


if __name__ == "__main__":
    main()
