#!/usr/bin/env python3
"""分析回测结果，按信号类型统计胜率和回报"""
import json, os
from collections import Counter

data_dir = "/root/binance-square-agent/data"
bt_files = [f for f in os.listdir(data_dir) if f.startswith("bt_") and f.endswith(".json")]

all_results = []
sym_summary = []

for fname in sorted(bt_files):
    with open(os.path.join(data_dir, fname)) as f:
        data = json.load(f)
    sym = data["sym"]
    results = data["results"]
    
    sigs = [r for r in results if r["grade"] in ("A", "B", "C")]
    wins = sum(1 for r in sigs if r["ret_8"] > 0)
    
    sym_summary.append({
        "sym": sym,
        "total": len(results),
        "sigs": len(sigs),
        "wins": wins,
        "wr": round(wins/len(sigs)*100,1) if sigs else 0,
        "avg_r": round(sum(r["ret_8"] for r in sigs)/len(sigs),2) if sigs else 0,
    })
    all_results.extend(results)

# Signal-level analysis
sig_counter = Counter()
sig_wins = Counter()
sig_returns = {}

for r in all_results:
    for s in r["sigs"]:
        sig_counter[s] += 1
        if r["ret_8"] > 0:
            sig_wins[s] += 1
        if s not in sig_returns:
            sig_returns[s] = []
        sig_returns[s].append(r["ret_8"])

print("="*80)
print("📊  SIGNAL-LEVEL BACKTEST ANALYSIS")
print("="*80)
print(f"\nTotal symbols: {len(bt_files)}")
print(f"Total bars scored: {len(all_results)}")
print(f"Total signals (A/B/C): {sum(1 for r in all_results if r['grade'] in ('A','B','C'))}")
print(f"Overall win rate: {sum(1 for r in all_results if r['grade'] in ('A','B','C') and r['ret_8'] > 0) / max(1, sum(1 for r in all_results if r['grade'] in ('A','B','C'))) * 100:.1f}%")

print(f"\n{'='*80}")
print("Signal Quality Ranking (by win rate, min 3 appearances):")
print(f"{'Signal':>30} {'Count':>6} {'Wins':>6} {'Win%':>7} {'AvgR+8':>9} {'Min':>9} {'Max':>9}")
print("-" * 80)

sig_stats = []
for s, cnt in sig_counter.most_common(50):
    if cnt < 3:
        continue
    w = sig_wins[s]
    r = sig_returns[s]
    avg_r = sum(r) / len(r)
    min_r = min(r)
    max_r = max(r)
    sig_stats.append((s, cnt, w, w/cnt*100, avg_r, min_r, max_r))

sig_stats.sort(key=lambda x: -x[3])  # sort by win rate descending

for s, cnt, w, wr, avg_r, min_r, max_r in sig_stats:
    bar = "█" * int(wr / 5)
    print(f"{s:>30}: {cnt:4d} {w:4d} {wr:>5.1f}% {avg_r:>7.2f}% {min_r:>7.2f}% {max_r:>7.2f}%  {bar}")

print(f"\n{'='*80}")
print("Symbol Summary:")
print(f"{'Symbol':>12} {'Sigs':>6} {'Win%':>7} {'AvgR8':>9} {'Bear':>6}")
print("-" * 42)
total_sigs = 0
total_wins = 0
for s in sorted(sym_summary, key=lambda x: -x["sigs"]):
    print(f"{s['sym']:>12}: {s['sigs']:4d} {s['wr']:>6.1f}% {s['avg_r']:>8.2f}%")
    total_sigs += s["sigs"]
    total_wins += s["wins"]

print(f"\n{'TOTAL':>12}: {total_sigs:4d} {total_wins/total_sigs*100:>5.1f}%")
