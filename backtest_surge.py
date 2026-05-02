#!/usr/bin/env python3
"""
backtest_surge.py — 策略信号回测系统

Walk back through historical kline data, scoring each bar as if it were "current",
then track how accurate signals are over the NEXT N bars.

Usage:
  python3 backtest_surge.py                    # Default: BTCUSDT OKX 4H
  python3 backtest_surge.py --sym ETHUSDT --exchange okx --tf 4H
  python3 backtest_surge.py --full             # Run top 10 volume symbols
  python3 backtest_surge.py --list             # List top volume symbols with data availability
"""

import os, sys, json, time, logging
from datetime import datetime, timezone
from collections import defaultdict, Counter
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from exchange_fetchers import (
    fetch_okx_kline, fetch_gate_kline, fetch_bitget_kline,
    fetch_okx_tickers,
)
from exchange_fetchers.scoring import score_kline, grade_from_score, merge_multi_scores

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("backtest")

# ── Config ──────────────────────────────────────────────────
LOOKAHEAD = 4          # Check price N bars after signal
TRAIN_BARS = 100        # Warm-up bars before scoring starts
TRAIN_SLACK = 20        # Extra bars at end for lookahead headroom


def slice_kline(kline: dict, end: int) -> dict:
    """Return a kline dict sliced up to index `end` (inclusive)."""
    return {
        k: v[:end+1] if isinstance(v, np.ndarray) else v
        for k, v in kline.items()
    }

def future_return(closes: np.ndarray, i: int, n: int) -> float:
    target = min(i + n, len(closes) - 1)
    if target <= i:
        return 0.0
    return float((closes[target] - closes[i]) / closes[i]) * 100

def max_future_return(closes: np.ndarray, i: int, n: int) -> float:
    target = min(i + n, len(closes) - 1)
    if target <= i:
        return 0.0
    return float((np.max(closes[i+1:target+1]) - closes[i]) / closes[i]) * 100

def min_future_return(closes: np.ndarray, i: int, n: int) -> float:
    target = min(i + n, len(closes) - 1)
    if target <= i:
        return 0.0
    return float((np.min(closes[i+1:target+1]) - closes[i]) / closes[i]) * 100


def backtest_one_kline(kline: dict) -> list[dict]:
    """
    Walk through kline data bar by bar.
    For each bar after TRAIN_BARS, score it and track forward returns.
    """
    n = len(kline["close"])
    min_bars = TRAIN_BARS + LOOKAHEAD
    if n < min_bars:
        logger.warning("Not enough bars: %d (need %d)", n, min_bars)
        return []

    results = []
    closes = kline["close"]

    for i in range(TRAIN_BARS, n - LOOKAHEAD):
        bar_kline = slice_kline(kline, i)

        try:
            result = score_kline(
                sym="TEST",
                price=float(closes[i]),
                chg24h=0.0,
                kline=bar_kline,
                vol_24h=0,
            )
        except Exception as e:
            logger.debug("Score failed at bar %d: %s", i, e)
            continue

        entry = {
            "i": i,
            "ts": str(kline["times"][i]),
            "price": float(closes[i]),
            "score": result["score"],
            "grade": result["grade"],
            "n_sig": len(result["signals"]),
            "sigs": result["signals"],
            "bear_sigs": result["signals_bear"],
            "has_bear": result["has_bear"],
            "bear_score": result.get("bear_score", 0),
            "chg24_sim": round(float((closes[i] - closes[max(0,i-24)]) / closes[max(0,i-24)]) * 100, 2) if i >= 24 else 0,
            "vol_ratio": result.get("vol_ratio", 0),
            "rsi": result.get("rsi", 50),
            "trend": result.get("trend", ""),
        }

        for lb in [1, 2, 4]:
            entry[f"ret_{lb}"] = round(future_return(closes, i, lb), 2)
        entry["ret_8"] = round(future_return(closes, i, LOOKAHEAD), 2)
        entry["max_gain"] = round(max_future_return(closes, i, LOOKAHEAD), 2)
        entry["max_loss"] = round(min_future_return(closes, i, LOOKAHEAD), 2)

        results.append(entry)

    return results


def analyze_results(results: list[dict]):
    """Print comprehensive backtest analysis."""
    if not results:
        print("No results.")
        return

    n = len(results)
    print(f"\n{'='*62}")
    print(f"📊  BACKTEST  —  {n} bars scored  (lookahead={LOOKAHEAD})")
    print(f"{'='*62}")

    # ── Grade distribution ──
    gc = Counter(r["grade"] for r in results)
    print(f"\n📈 Grade Distribution:")
    total_sig = 0
    for g in ["A", "B", "C", "D"]:
        cnt = gc.get(g, 0)
        pct = cnt / n * 100
        if g in ("A", "B", "C"):
            total_sig += cnt
        print(f"  {g}: {cnt:4d} ({pct:5.1f}%)")
    print(f"  → {total_sig} non-D signals ({total_sig/n*100:.1f}%)")

    bear_n = sum(1 for r in results if r["has_bear"])
    print(f"\n🐻 Bear Signals: {bear_n} ({bear_n/n*100:.1f}%)")

    # ── Forward return by grade ──
    print(f"\n📉 Avg Forward Returns (lookahead={LOOKAHEAD} bars):")
    print(f"{'Grade':>6} {'N':>6} {'R+1':>7} {'R+2':>7} {'R+4':>7} {'R+8':>7} {'MaxG':>7} {'MaxL':>7} {'Win%':>6}")
    print("-" * 66)
    for g in ["A", "B", "C", "D"]:
        sub = [r for r in results if r["grade"] == g]
        if not sub:
            continue
        def avg(k):
            return sum(r[k] for r in sub) / len(sub)
        win = sum(1 for r in sub if r["ret_8"] > 0)
        print(f"{g:>6} {len(sub):>6} {avg('ret_1'):>6.2f}% {avg('ret_2'):>6.2f}% {avg('ret_4'):>6.2f}% {avg('ret_8'):>6.2f}% {avg('max_gain'):>6.2f}% {avg('max_loss'):>6.2f}% {win/len(sub)*100:>5.1f}%")

    # Bear grade specifically
    bear_sub = [r for r in results if r["has_bear"]]
    if bear_sub:
        avg_r8 = sum(r["ret_8"] for r in bear_sub) / len(bear_sub)
        avg_ml = sum(r["max_loss"] for r in bear_sub) / len(bear_sub)
        down = sum(1 for r in bear_sub if r["ret_8"] < 0)
        print(f"{'BEAR':>6} {len(bear_sub):>6} {'':>7} {'':>7} {'':>7} {avg_r8:>6.2f}% {'':>7} {avg_ml:>6.2f}% {down/len(bear_sub)*100:>5.1f}%")

    # ── Signal frequency ──
    print(f"\n🔍 Signal Frequency:")
    sc = Counter()
    for r in results:
        for s in r["sigs"]:
            sc[s] += 1
    for s, c in sc.most_common(15):
        print(f"  {s:22s}: {c:3d} ({c/n*100:4.1f}%)")

    bsc = Counter()
    for r in results:
        for s in r["bear_sigs"]:
            bsc[s] += 1
    if bsc:
        print(f"\n🐻 Bear Signal Frequency:")
        for s, c in bsc.most_common(10):
            print(f"  {s:22s}: {c:3d} ({c/n*100:4.1f}%)")

    # ── Signal win rate ──
    print(f"\n🎯 Signal Win Rate (ret_8 > 0):")
    print(f"{'Signal':>24} {'N':>5} {'Win%':>6} {'AvgR8':>8}")
    print("-" * 46)
    for s in sc:
        sr = [r for r in results if s in r["sigs"]]
        w = sum(1 for r in sr if r["ret_8"] > 0)
        ar = sum(r["ret_8"] for r in sr) / len(sr)
        print(f"{s:>24}: {len(sr):>4} {w/len(sr)*100:>5.1f}% {ar:>7.2f}%")

    # Summary
    print(f"\n📋 Summary:")
    sigs = [r for r in results if r["grade"] in ("A", "B", "C")]
    if sigs:
        wr = sum(1 for r in sigs if r["ret_8"] > 0) / len(sigs) * 100
        ar = sum(r["ret_8"] for r in sigs) / len(sigs)
        print(f"  A/B/C signals: {len(sigs)}, win rate={wr:.1f}%, avg ret={ar:.2f}%")
    else:
        print("  No A/B/C signals.")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Surge Scanner Backtest")
    parser.add_argument("--sym", default="BTCUSDT")
    parser.add_argument("--exchange", default="okx", choices=["okx", "gate", "bitget"])
    parser.add_argument("--tf", default="4H", choices=["1H", "2H", "4H", "1h", "2h", "4h"])
    parser.add_argument("--full", action="store_true", help="Run top volume symbols")
    parser.add_argument("--list", action="store_true", help="List available top-15 symbols")
    args = parser.parse_args()

    def inst(sym, ex):
        base = sym.replace("USDT", "")
        if ex == "okx":
            return f"{base}-USDT-SWAP"
        elif ex == "gate":
            return f"{base}_USDT"
        return sym

    def fetch(sym, ex, tf):
        iid = inst(sym, ex)
        if ex == "okx":
            return fetch_okx_kline(sym, iid, tf)
        elif ex == "gate":
            return fetch_gate_kline(sym, iid, tf)
        return fetch_bitget_kline(sym, iid, tf)

    if args.list:
        print("Fetching top tickers...")
        tickers = fetch_okx_tickers(min_vol=5_000_000)
        top = sorted(tickers.items(), key=lambda x: -x[1].get("vol_usd", 0))[:15]
        print(f"{'Sym':12s} {'Vol':>10s} {'Price':>10s}")
        print("-" * 34)
        for sym, info in top:
            print(f"{sym:12s} {info.get('vol_usd',0)/1e6:>8.1f}M {info.get('price',0):>10.4f}")
        return

    if args.full:
        tickers = fetch_okx_tickers(min_vol=5_000_000)
        top = sorted(tickers.items(), key=lambda x: -x[1].get("vol_usd", 0))[:15]
        syms = [s for s, _ in top]

        all_results = {}
        for sym in syms:
            print(f"\n{'─'*50}")
            print(f"Backtesting {sym} on {args.exchange} {args.tf}...", end=" ", flush=True)
            try:
                kline = fetch(sym, args.exchange, args.tf)
                if kline is None or len(kline.get("close", [])) < TRAIN_BARS + LOOKAHEAD:
                    print("⚠️  short data")
                    continue
                results = backtest_one_kline(kline)
                all_results[sym] = results
                sigs = [r for r in results if r["grade"] in ("A", "B", "C")]
                if sigs:
                    wr = sum(1 for r in sigs if r["ret_8"] > 0) / len(sigs) * 100
                    ar = sum(r["ret_8"] for r in sigs) / len(sigs)
                    print(f"✅ {len(sigs)} sigs, wr={wr:.0f}%, avg_r={ar:.2f}%")
                else:
                    print("✅ (no signals)")
            except Exception as e:
                print(f"❌ {e}")

        # Cross-symbol summary
        print(f"\n\n{'='*60}")
        print("📊  CROSS-SYMBOL SUMMARY")
        print('='*60)
        total_sigs = 0
        total_wins = 0
        for sym, res in all_results.items():
            sigs = [r for r in res if r["grade"] in ("A", "B", "C")]
            wins = sum(1 for r in sigs if r["ret_8"] > 0)
            total_sigs += len(sigs)
            total_wins += wins
            if sigs:
                wr = wins / len(sigs) * 100
                ar = sum(r["ret_8"] for r in sigs) / len(sigs)
                print(f"  {sym:12s}: {len(sigs):3d} sigs, wr={wr:5.1f}%, avg_r={ar:7.2f}%")
            else:
                print(f"  {sym:12s}: no signals")
        if total_sigs:
            print(f"\n  {'TOTAL':12s}: {total_sigs:3d} sigs, wr={total_wins/total_sigs*100:.1f}%")
    else:
        sym, ex, tf = args.sym, args.exchange, args.tf
        kline = fetch(sym, ex, tf)
        if kline is None:
            print("❌ Failed to fetch kline")
            return
        bars = len(kline.get("close", []))
        if bars < TRAIN_BARS + LOOKAHEAD:
            print(f"❌ Short data: {bars} bars (need {TRAIN_BARS + LOOKAHEAD})")
            return
        print(f"✅ {bars} bars")
        results = backtest_one_kline(kline)
        analyze_results(results)

        out = f"data/bt_{sym}_{ex}_{tf}.json"
        os.makedirs("data", exist_ok=True)
        with open(out, "w") as f:
            json.dump({"sym": sym, "ex": ex, "tf": tf, "n": len(results), "results": results}, f, indent=1)
        print(f"\n📁 Saved to {out}")


if __name__ == "__main__":
    main()
