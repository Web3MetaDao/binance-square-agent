#!/usr/bin/env python3
"""
OKX Auto Trader — Main entry point.

全天候全自动交易系统 | 100U → 2000U
Signal → Decision → Execution pipeline.

Usage:
    python main.py scan          # Scan market for opportunities (dry-run)
    python main.py cycle         # Run one full decision cycle
    python main.py monitor       # Check and update open positions
    python main.py status        # Print current system status
    python main.py close-all     # Emergency: close all positions
    python main.py history       # Show trade history
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

from okx_wrapper import OKXWrapper
from signal_fusion import SignalFusionEngine
from risk_manager import RiskController
from strategy_engine import DecisionEngine

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)


def cmd_scan(args):
    """Dry-run scan: show fused signals + breakout signals."""
    okx = OKXWrapper()
    engine = SignalFusionEngine(okx)

    print(f"[{datetime.now(timezone.utc).isoformat()}] 📊 Full Market Scan")
    print("=" * 70)
    
    # ── 1. OKX评分信号 ──
    ticker_signals = engine.scan_top_setups(limit=20, min_vol_usd=50_000)
    
    if ticker_signals:
        print(f"\n▸ OKX技术评分信号 (Top {len(ticker_signals)}):")
        print("-" * 70)
        for s in ticker_signals[:args.limit]:
            score = s["score"]
            direction = "🐂" if score > 0 else "🐻" if score < 0 else "⚪"
            action = s["action"].replace("_", " ").upper()
            print(f"  {direction} {s['symbol']:20s} | score={score:+.1f} | "
                  f"conf={s['confidence']:.2f} | {action:15s} | "
                  f"{'✨RESONANCE' if s.get('resonance') else ''}"
                  f"{'⚠️CONFLICT' if s.get('conflict') else ''}")
            for comp in s.get("components", []):
                det = comp.get("details", {})
                det_str = " | ".join(
                    f"{k}={v}" for k, v in det.items()
                    if isinstance(v, (int, float))
                )
                print(f"    {comp['name']:15s}: {comp['score']:+.1f} ({comp['weighted_score']:+.1f}) {det_str}")
    else:
        print("  (no ticker signals)")
    
    # ── 2. 起涨点快速扫描 ──
    try:
        from breakout_rapid import run_scan as run_breakout
        print(f"\n▸ 起涨点快速扫描 v1.6 (双通道并行):")
        print("-" * 70)
        bsignals = run_breakout(top_n=15)

        if bsignals:
            print(f"  {'币种':<20s} {'条件':8s} {'品质':>4s} {'通道':>4s} {'涨幅%':>7s} {'量比':>6s} {'24h%':>7s} {'评分':>5s} {'量级':>5s} {'舆情':>5s}")
            print(f"  {'-'*75}")
            for s in bsignals:
                cond_str = "+".join(s["conditions"])
                qual = s.get("h_quality", 0)
                qual_str = f"H{qual}" if qual > 0 else "-"
                cls = s.get("signal_class", "-")
                vt = s.get("vol_tier", "-")
                sh = s.get("sentiment_heat", 0)
                sh_str = f"{sh:.0f}" if sh > 0 else "-"
                print(f"  {s['symbol']:<20s} {cond_str:8s} {qual_str:>4s} {cls:>4s} {s['chg_pct']:>+6.2f}% {s['vol_ratio']:>5.1f}x {s['chg24h']:>+6.2f}% {s['score']:>5.1f} {vt:>5s} {sh_str:>5s}")
        else:
            print("  (暂无起涨信号)")
    except ImportError:
        print("\n  (breakout_rapid模块未安装 — 跳过起涨点扫描)")
    except Exception as e:
        print(f"\n  (起涨点扫描出错: {e})")
    
    # ── 3. 信号交叉验证 ──
    print(f"\n▸ 信号合并验证:")
    print("-" * 70)
    if ticker_signals and bsignals:
        # 找交集
        ticker_coins = {s["symbol"] for s in ticker_signals}
        breakout_coins = {s["symbol"] for s in bsignals}
        overlap = ticker_coins & breakout_coins
        if overlap:
            print(f"  🔥 共振信号 ({len(overlap)}个): ", ", ".join(sorted(overlap)[:10]))
        else:
            print("  无交叉共振信号（两套评分独立运作中）")
    
    print(f"\n{'=' * 70}")
    
    # Save combined results
    out_path = os.path.join(DATA_DIR, "last_scan.json")
    with open(out_path, "w") as f:
        json.dump({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "ticker_signals": ticker_signals,
            "breakout_signals": [dict(s) if hasattr(s, '__dict__') else s for s in (bsignals or [])],
        }, f, indent=2, default=str)
    print(f"Scan results saved to {out_path}")


def cmd_cycle(args):
    """Run one full trading cycle: scan → decide → (optionally) execute."""
    engine = DecisionEngine()
    print(f"[{datetime.now(timezone.utc).isoformat()}] 🚀 Running trading cycle...")

    # Check system status
    status = engine.risk.get_state_summary()
    print(f"  System: {status['equity']:.1f}U | Peak: {status['peak_equity']:.1f}U | "
          f"Drawdown: {status['drawdown_pct']:.1f}% | "
          f"Trades: {status['total_trades']} (WR: {status['win_rate']:.0%})")

    # Run cycle
    actions = engine.run_cycle()

    # Check positions
    pos_updates = engine.check_positions()

    if actions:
        print(f"\n  📋 Actions Proposed:")
        for a in actions:
            if a.get("type") == "blocked":
                print(f"    ⛔ {a['reason']}")
            else:
                print(f"    {'📈' if a.get('direction') == 'long' else '📉'} "
                      f"{a['symbol']:15s} | "
                      f"size={a['size_usd']:.1f}U | "
                      f"SL={a['stop_loss_pct']:.1f}% | "
                      f"TP={a['take_profit_pct']:.1f}% | "
                      f"conf={a['confidence']:.2f} | "
                      f"strat={a['strategy']} | "
                      f"reason: {a.get('reason', '')}")
    else:
        print("  No actions proposed.")

    if pos_updates:
        print(f"\n  📊 Position Updates ({len(pos_updates)}):")
        for u in pos_updates:
            t = u.get("type", "")
            if t == "position_closed":
                print(f"    🔒 {u['symbol']}: CLOSED ({u.get('reason')}) PnL={u.get('pnl_usd', 0):+.2f}U")
            elif t == "trailing_stop_updated":
                print(f"    🔼 {u['symbol']}: trailing stop moved to {u.get('new_sl_pct', 0):.2f}%")

    # Final status
    final_status = engine.risk.get_state_summary()
    print(f"\n  📊 After Cycle: {final_status['equity']:.1f}U | "
          f"Open: {final_status['open_positions']} | "
          f"Daily: {final_status['daily_pnl']:+.1f}U")

    # Save cycle result
    cycle_path = os.path.join(DATA_DIR, "last_cycle.json")
    with open(cycle_path, "w") as f:
        json.dump({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": final_status,
            "actions": actions,
            "position_updates": pos_updates,
        }, f, indent=2, default=str)

    # If --execute flag, actually send trades
    if args.execute:
        _execute_actions(engine, actions)


def _execute_actions(engine: DecisionEngine, actions: list[dict]) -> None:
    """
    Execute approved actions via OKX CLI.
    Requires API credentials.
    """
    okx = engine.okx
    risk = engine.risk

    for a in actions:
        if a.get("type") != "enter":
            continue

        try:
            if a.get("direction") == "long":
                # Set leverage first
                okx.set_leverage(symbol, args.leverage, "cross", "long")
                # Place market order
                result = okx.place_swap_order(
                    a["symbol"],
                    side="buy",
                    sz=str(calculate_contracts(okx, a["symbol"], a["size_usd"], a.get("entry_price", 0))),
                    pos_side="long",
                    tgt_ccy="quote_ccy",
                    lever=args.leverage,
                )
                # Set stop loss
                sl_price = a.get("entry_price", 0) * (1 - a["stop_loss_pct"] / 100)
                tp_price = a.get("entry_price", 0) * (1 + a["take_profit_pct"] / 100)
                okx.set_tp_sl(
                    a["symbol"],
                    tp_trigger_px=str(tp_price),
                    sl_trigger_px=str(sl_price),
                    pos_side="long",
                )
                print(f"    ✅ Executed LONG {a['symbol']} ({a['size_usd']:.1f}U)")
            elif a.get("direction") == "short":
                okx.set_leverage(symbol, args.leverage, "cross", "short")
                result = okx.place_swap_order(
                    a["symbol"],
                    side="sell",
                    sz=str(calculate_contracts(okx, a["symbol"], a["size_usd"], a.get("entry_price", 0))),
                    pos_side="short",
                    tgt_ccy="quote_ccy",
                    lever=args.leverage,
                )
                sl_price = a.get("entry_price", 0) * (1 + a["stop_loss_pct"] / 100)
                tp_price = a.get("entry_price", 0) * (1 - a["take_profit_pct"] / 100)
                okx.set_tp_sl(
                    a["symbol"],
                    tp_trigger_px=str(tp_price),
                    sl_trigger_px=str(sl_price),
                    pos_side="short",
                )
                print(f"    ✅ Executed SHORT {a['symbol']} ({a['size_usd']:.1f}U)")

            # Log executed order to risk manager
            risk.open_position(
                symbol=a["symbol"],
                direction=a["direction"],
                size_usd=a["size_usd"],
                entry_price=a.get("entry_price", 0),
                leverage=args.leverage,
                stop_loss_pct=a["stop_loss_pct"],
                take_profit_pct=a["take_profit_pct"],
                strategy_source=a["strategy"],
            )

        except Exception as e:
            print(f"    ❌ Failed to execute {a['symbol']}: {e}")


def calculate_contracts(okx: OKXWrapper, symbol: str, usd_amount: float,
                        price: float) -> int:
    """Calculate number of contracts for a USD amount."""
    size = usd_amount
    # For quote_ccy mode, sz = USD amount
    return int(size)


def cmd_monitor(args):
    """Monitor and update open positions."""
    engine = DecisionEngine()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Monitoring positions...")

    updates = engine.check_positions()
    status = engine.risk.get_state_summary()

    open_positions = engine.risk.get_open_positions()

    if open_positions:
        print(f"\n  📊 Open Positions ({len(open_positions)}):")
        for p in open_positions:
            pnl_str = f"{p['pnl_usd']:+.2f}U ({p['pnl_pct']:+.1f}%)"
            print(f"    {'📈' if p['direction'] == 'long' else '📉'} "
                  f"{p['symbol']:15s} | entry={p['entry_price']:.4f} | "
                  f"now={p['current_price']:.4f} | {pnl_str} | "
                  f"SL={p['stop_loss_pct']:.1f}% | "
                  f"TP={p['take_profit_pct']:.1f}% | "
                  f"strat={p['strategy_source']}")
    else:
        print("  No open positions.")

    if updates:
        print(f"\n  📋 Updates ({len(updates)}):")
        for u in updates:
            if u["type"] == "trailing_stop_updated":
                print(f"    🔼 {u['symbol']}: trailing stop → {u.get('new_sl_pct', 0):.1f}%")
            elif u["type"] == "position_closed":
                print(f"    🔒 {u['symbol']}: CLOSED ({u.get('reason')}) PnL={u.get('pnl_usd', 0):+.2f}U")

    print(f"\n  📊 Equity: {status['equity']:.1f}U | "
          f"Daily: {status['daily_pnl']:+.1f}U | "
          f"Progress: {status['progress_pct']:.1f}% → {2000}U")


def cmd_status(args):
    """Show full system status."""
    engine = DecisionEngine()
    status = engine.get_status()
    print(json.dumps(status, indent=2, default=str))


def cmd_close_all(args):
    """Emergency: close all open positions."""
    print(f"[{datetime.now(timezone.utc).isoformat()}] ⚠️ EMERGENCY: Closing all positions...")
    risk = RiskController()
    closed = risk.close_all_positions(reason="manual_emergency")
    print(f"  Closed {len(closed)} positions.")
    for p in closed:
        print(f"    {p['symbol']} | PnL={p.get('pnl_usd', 0):+.2f}U")
    print(f"  Equity: {risk.get_state_summary()['equity']:.1f}U")


def cmd_history(args):
    """Show trade history."""
    risk = RiskController()
    history = risk.get_trade_history(limit=args.limit)

    if not history:
        print("No trades yet.")
        return

    print(f"\nTrade History (last {len(history)}):")
    print("-" * 90)
    print(f"{'Date':20s} {'Symbol':15s} {'Dir':6s} {'Size':8s} {'Entry':10s} {'Exit':10s} {'PnL':10s} {'Reason':12s}")
    print("-" * 90)

    total_pnl = 0
    wins = 0
    for t in history:
        date = datetime.fromtimestamp(t.get("opened_at", 0)).strftime("%Y-%m-%d %H:%M")
        pnl = t.get("pnl_usd", 0)
        total_pnl += pnl
        if pnl > 0:
            wins += 1
        dir_icon = "📈" if t.get("direction") == "long" else "📉"
        print(f"{date:20s} {t.get('symbol', ''):15s} {dir_icon+' '+t.get('direction',''):6s} "
              f"{t.get('size_usd',0):<8.1f} {t.get('entry_price',0):<10.4f} "
              f"{t.get('exit_price',0):<10.4f} {pnl:<+9.2f}U {t.get('exit_reason',''):12s}")

    print("-" * 90)
    print(f"Total: {len(history)} trades | Wins: {wins} | "
          f"Win Rate: {wins/max(1,len(history)):.0%} | "
          f"Net PnL: {total_pnl:+.2f}U")


def main():
    parser = argparse.ArgumentParser(
        description="OKX Auto Trader — 100U → 2000U System"
    )
    parser.add_argument(
        "--leverage", type=int, default=5,
        help="Leverage for trades (default: 5)"
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # scan
    p_scan = sub.add_parser("scan", help="Dry-run: scan market for signals")
    p_scan.add_argument("--limit", type=int, default=20)

    # cycle
    p_cycle = sub.add_parser("cycle", help="Run one full decision cycle")
    p_cycle.add_argument("--execute", action="store_true",
                         help="⚠️ Actually execute trades (requires API key)")

    # monitor
    sub.add_parser("monitor", help="Monitor open positions")

    # status
    sub.add_parser("status", help="Show full system status")

    # close-all
    sub.add_parser("close-all", help="⚠️ Emergency close all positions")

    # history
    p_hist = sub.add_parser("history", help="Show trade history")
    p_hist.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()

    if args.command == "scan":
        cmd_scan(args)
    elif args.command == "cycle":
        cmd_cycle(args)
    elif args.command == "monitor":
        cmd_monitor(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "close-all":
        cmd_close_all(args)
    elif args.command == "history":
        cmd_history(args)


if __name__ == "__main__":
    main()
