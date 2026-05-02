#!/usr/bin/env python3
"""
surge_scanner_v2.py — Multi-exchange surge scanner (v3 engine).
5-phase pipeline: ticker fetch -> batch kline -> scoring -> merge -> push.

Phases:
  1. Parallel fetch tickers from OKX, Gate, Bitget (full scan, no top-N).
  2. Per-exchange batch kline fetch for 3 timeframes (1H, 2H, 4H).
  3. score_kline() for each (symbol, exchange, timeframe) pair.
  4. merge_multi_scores() for each symbol across all pairs.
  5. Sort by merged score, push A/B signals to Telegram, send summary.

Usage:
  python3 surge_scanner_v2.py              # Full scan + push
  python3 surge_scanner_v2.py --dry        # Preview without sending
  python3 surge_scanner_v2.py --max 3      # Max signals (default 5)
"""

import os, sys, json, time as _time, logging
from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

# ── Exchange fetchers ─────────────────────────────────────
from exchange_fetchers import (
    fetch_okx_tickers,
    fetch_gate_tickers,
    fetch_bitget_tickers,
    batch_fetch_okx_klines,
    batch_fetch_gate_klines,
    batch_fetch_bitget_klines,
    ExtrasFetcher,
    LargeTakerDetector,
    LiquidationDetector,
    KlineDB,
)
from exchange_fetchers.scoring import score_kline, merge_multi_scores

# ── Hardcoded config ──────────────────────────────────────
SIGNAL_BOT_TOKEN="8757823940:AAEZMfxUBa0-dgxem_XSrbiYR_A1qDsb7V4"
SIGNAL_CHAT_ID = "1077054086"
DATA_DIR = "/root/binance-square-agent/data"

# ── Scan parameters ───────────────────────────────────────
MIN_VOL_USDT = 1_000_000       # 主池：稳定币最低成交量
MIN_VOL_RAPID = 200_000        # 快速启动通道：低市值爆拉币阈值
RAPID_CHG_THRESHOLD = 15.0     # 快速启动：24h涨跌幅必须≥此值
TIMEFRAMES = ["1H", "2H", "4H"]
# Gate.io uses lowercase timeframes
GATE_TIMEFRAMES = ["1h", "2h", "4h"]

# ── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("surge_v3")


# ═══════════════════════════════════════════════════════════
# Phase 1: Fetch tickers from all 3 exchanges in parallel
# ═══════════════════════════════════════════════════════════

def fetch_all_tickers(min_vol: float = MIN_VOL_USDT):
    """
    Fetch tickers from OKX, Gate, Bitget concurrently — dual channel.

    Primary channel: vol >= min_vol (default 1M) — all candidates.
    Rapid channel: vol >= MIN_VOL_RAPID AND |chg24h| >= RAPID_CHG_THRESHOLD
    — low-cap breakout coins that would be missed by the primary pool.

    Returns:
        candidates: dict {sym: {price, chg24h, vol_usd, exchanges: [list], rapid: bool}}
        exchange_listings: dict {exchange_name: [list of {sym, ...required kline input keys}]}
    """
    candidates: dict[str, dict] = {}
    exchange_listings: dict[str, list] = {
        "okx": [],
        "gate": [],
        "bitget": [],
    }

    def fetch_one(exchange_name: str, fetcher_fn):
        """Fetch one exchange and return (name, tickers_dict, symbol_items)."""
        try:
            tickers = fetcher_fn(MIN_VOL_RAPID)  # fetch at lower threshold for rapid channel
            return exchange_name, tickers
        except Exception as e:
            logger.error("fetch_one %s failed: %s", exchange_name, e)
            return exchange_name, {}

    with ThreadPoolExecutor(max_workers=3) as exe:
        futures = {
            exe.submit(fetch_one, "okx", fetch_okx_tickers): "okx",
            exe.submit(fetch_one, "gate", fetch_gate_tickers): "gate",
            exe.submit(fetch_one, "bitget", fetch_bitget_tickers): "bitget",
        }
        for future in as_completed(futures):
            name, tickers = future.result()
            # Build exchange_listings only for primary pool (vol >= min_vol)
            # to avoid pulling klines for too many low-vol coins
            primary_items = []
            for sym, info in tickers.items():
                if info.get("vol_usd", 0) >= min_vol:
                    primary_items.append(info)
            exchange_listings[name] = primary_items

            for sym, info in tickers.items():
                vol = info.get("vol_usd", 0)
                chg = abs(info.get("chg24h", 0))
                # Primary pool: standard threshold
                if vol >= min_vol:
                    pass  # always include
                # Rapid channel: low vol but big move
                elif vol >= MIN_VOL_RAPID and chg >= RAPID_CHG_THRESHOLD:
                    pass  # include as rapid
                else:
                    continue

                if sym not in candidates:
                    candidates[sym] = {
                        "sym": sym,
                        "price": info.get("price", 0),
                        "chg24h": info.get("chg24h", 0),
                        "vol_usd": vol,
                        "exchanges": [],
                        "rapid": vol < min_vol,
                    }
                else:
                    old = candidates[sym]
                    if info.get("price", 0) > 0:
                        old["price"] = info.get("price", 0)
                    if vol > old.get("vol_usd", 0):
                        old["vol_usd"] = vol
                    if info.get("chg24h", 0) != 0:
                        old["chg24h"] = info.get("chg24h", 0)
                    # If any exchange qualifies for primary, upgrade from rapid
                    if vol >= min_vol:
                        old["rapid"] = False
                candidates[sym]["exchanges"].append(name)

    rapid_count = sum(1 for c in candidates.values() if c.get("rapid"))
    logger.info(
        "Phase 1 total: %d unique candidates (%d rapid) across %d exchanges",
        len(candidates), rapid_count, len([e for e in exchange_listings.values() if e]),
    )
    return candidates, exchange_listings


# ═══════════════════════════════════════════════════════════
# Phase 2: Batch kline fetch per exchange for all timeframes
# ═══════════════════════════════════════════════════════════

def _make_kline_fetch_fn(exchange_name: str, tf: str):
    """Return a zero-arg callable suitable for KlineDB.ensure_gap_klines.

    The returned factory takes a symbol and returns a fetch_fn callable
    that captures the exchange-specific convention (inst_id, contract, etc).
    """
    tf_map = tf.lower() if exchange_name == "gate" else tf

    if exchange_name == "okx":
        from exchange_fetchers.okx_fetcher import fetch_okx_kline as fn
        def _fetch(sym: str):
            inst_id = sym.replace("USDT", "-USDT-SWAP")
            return fn(sym, inst_id, tf)
        return _fetch
    elif exchange_name == "gate":
        from exchange_fetchers.gate_fetcher import fetch_gate_kline as fn
        def _fetch(sym: str):
            contract = sym.replace("USDT", "_USDT")
            return fn(contract, tf_map)
        return _fetch
    elif exchange_name == "bitget":
        from exchange_fetchers.bitget_fetcher import fetch_bitget_kline as fn
        def _fetch(sym: str):
            return fn(sym, tf)
        return _fetch
    return None


def fetch_all_klines(exchange_listings: dict, candidates: dict | None = None) -> list[dict]:
    """
    Fetch klines using KlineDB cache layer.
    Falls back to direct API via ensure_gap_klines when cache is stale/missing.

    If candidates is provided, also fetches klines for rapid coins (vol < 1M but big moves).

    Returns:
        raw_scores_input: list of dicts with keys:
            {sym, price, chg24h, vol_24h, exchange, timeframe, kline}
    """
    timeframe_map = {
        "okx": TIMEFRAMES,
        "gate": GATE_TIMEFRAMES,
        "bitget": TIMEFRAMES,
    }

    raw_scores_input: list[dict] = []
    db = KlineDB()
    total_fetches = 0

    for exchange_name, items in exchange_listings.items():
        if not items:
            continue

        tfs = timeframe_map[exchange_name]

        for tf in tfs:
            klines: dict[str, dict] = {}

            # Build a fetch factory for this (exchange, timeframe) combo
            fetch_sym_fn = _make_kline_fetch_fn(exchange_name, tf)
            if fetch_sym_fn is None:
                logger.warning("No fetcher for %s %s, skipping", exchange_name, tf)
                continue

            for item in items:
                sym = item.get("sym", "")
                if not sym:
                    continue

                try:
                    # Wrap with zero-arg closure for KlineDB
                    result = db.ensure_gap_klines(
                        sym, exchange_name, tf,
                        lambda s=sym, fn=fetch_sym_fn: fn(s),
                    )
                    if result is not None:
                        klines[sym] = result
                        total_fetches += 1
                except Exception as exc:
                    logger.debug(
                        "KlineDB fetch failed %s %s %s: %s",
                        sym, exchange_name, tf, exc,
                    )
                    continue

            logger.info(
                "Phase 2 %s %s: %d klines fetched (cached)",
                exchange_name, tf, len(klines),
            )

            # Build lookup for ticker data (price, chg24h, vol_usd)
            ticker_lookup = {item.get("sym", ""): item for item in items}

            for sym, kline in klines.items():
                info = ticker_lookup.get(sym, {})
                raw_scores_input.append({
                    "sym": sym,
                    "price": info.get("price", 0),
                    "chg24h": info.get("chg24h", 0),
                    "vol_24h": info.get("vol_usd", 0),
                    "exchange": exchange_name,
                    "timeframe": tf,
                    "kline": kline,
                })

    # Fetch klines for rapid coins (vol < 1M but |chg| >= RAPID_CHG_THRESHOLD)
    rapid_fetched = 0
    if candidates:
        for sym, rc in candidates.items():
            if not rc.get("rapid"):
                continue
            for exch_name in rc.get("exchanges", []):
                tfs = timeframe_map.get(exch_name, TIMEFRAMES)
                for tf in tfs:
                    fetch_fn = _make_kline_fetch_fn(exch_name, tf)
                    if fetch_fn is None:
                        continue
                    try:
                        kline = db.ensure_gap_klines(sym, exch_name, tf, lambda s=sym, fn=fetch_fn: fn(s))
                        if kline is not None:
                            raw_scores_input.append({
                                "sym": sym,
                                "price": rc.get("price", 0),
                                "chg24h": rc.get("chg24h", 0),
                                "vol_24h": rc.get("vol_usd", 0),
                                "exchange": exch_name,
                                "timeframe": tf,
                                "kline": kline,
                            })
                            rapid_fetched += 1
                    except Exception:
                        pass
        if rapid_fetched:
            logger.info("Phase 2 rapid: %d kline groups for rapid coins", rapid_fetched)

    db.close()

    logger.info(
        "Phase 2 total: %d (symbol, exchange, timeframe) kline groups",
        len(raw_scores_input),
    )
    return raw_scores_input


# ═══════════════════════════════════════════════════════════
# Phase 3: Score each (symbol, exchange, timeframe) pair
# ═══════════════════════════════════════════════════════════

def score_all_klines(raw_inputs: list[dict], extras_data: dict[str, dict] | None = None) -> list[dict]:
    """
    Score every kline group with score_kline().

    If extras_data is provided, it will be passed to score_kline() as the
    'extras' parameter for each symbol (merged across exchange/timeframe).

    Args:
        raw_inputs: list of dicts from fetch_all_klines()
        extras_data: optional {sym: {funding_rate, oi_growth_pct, long_short_ratio}}

    Returns list of individual score dicts with exchange/timeframe metadata.
    """
    individual_scores: list[dict] = []

    for inp in raw_inputs:
        try:
            score = score_kline(
                sym=inp["sym"],
                price=inp["price"],
                chg24h=inp["chg24h"],
                kline=inp["kline"],
                vol_24h=inp["vol_24h"],
                extras=extras_data.get(inp["sym"]) if extras_data else None,
            )
            # Attach exchange and timeframe for later merging
            score["exchange"] = inp["exchange"]
            score["timeframe"] = inp["timeframe"]
            individual_scores.append(score)
        except Exception as e:
            logger.debug(
                "score_kline failed %s %s %s: %s",
                inp["sym"], inp["exchange"], inp["timeframe"], e,
            )

    logger.info(
        "Phase 3: %d individual scores computed", len(individual_scores)
    )
    return individual_scores


# ═══════════════════════════════════════════════════════════
# Phase 4: Merge multi-scores per symbol
# ═══════════════════════════════════════════════════════════

def merge_scores_by_symbol(individual_scores: list[dict]) -> list[dict]:
    """
    Group individual scores by symbol and call merge_multi_scores() per group.
    Returns list of merged score dicts, sorted by score descending.
    """
    by_sym: dict[str, list[dict]] = defaultdict(list)
    for sc in individual_scores:
        by_sym[sc["sym"]].append(sc)

    merged_results = []
    for sym, scores_list in by_sym.items():
        try:
            merged = merge_multi_scores(scores_list)
            merged_results.append(merged)
        except Exception as e:
            logger.debug("merge_multi_scores failed %s: %s", sym, e)

    # Add volatility_score to each merged result (for composite ordering)
    for r in merged_results:
        chg = abs(r.get("chg24h", 0))
        r["volatility_score"] = chg / 10.0  # 10% daily move = 1.0

    merged_results.sort(key=lambda r: -r.get("score", 0))

    # Grade distribution
    grade_counts = defaultdict(int)
    for r in merged_results:
        grade_counts[r.get("grade", "D")] += 1

    logger.info(
        "Phase 4 merged: %d symbols | A=%d B=%d C=%d D=%d",
        len(merged_results),
        grade_counts.get("A", 0),
        grade_counts.get("B", 0),
        grade_counts.get("C", 0),
        grade_counts.get("D", 0),
    )
    return merged_results


# ═══════════════════════════════════════════════════════════
# Phase 5: Format and push signals + summary to Telegram
# ═══════════════════════════════════════════════════════════

def format_signal(r: dict, bear: bool = False) -> str:
    """Format a single signal message for Telegram — structured card."""
    sym_raw = r.get("sym", "UNKNOWN")
    sym_short = sym_raw.replace("USDT", "")
    price = r.get("price", 0)
    chg = r.get("chg24h", 0)
    grade = r.get("grade", "D")
    score = r.get("score", 0)
    ts = datetime.now(timezone.utc).strftime("%m-%d %H:%M UTC")

    vol = r.get("vol_24h", 0)
    if vol >= 1_000_000:
        vol_str = f"{vol/1_000_000:.1f}M"
    elif vol >= 1000:
        vol_str = f"{vol/1000:.0f}K"
    else:
        vol_str = f"{vol:.0f}"
    cx = r.get("cross_exchange", 0)
    ctf = r.get("cross_timeframe", 0)

    # ── Header ──
    icon = {"A": "🟢", "B": "🟡", "C": "🟠", "D": "⚪"}.get(grade, "⚪")
    rapid_flag = r.get("rapid", False)
    if bear:
        header = f"🔴 *${sym_short}* — Bearish Signal"
    elif rapid_flag:
        header = f"🚀 *${sym_short}* — Rapid Breakout"
    else:
        header = f"{icon} *${sym_short}* — {grade} Surge Signal"
    lines = [header, ""]

    # ── Price & Change ──
    chg_icon = "🟢" if chg > 0 else "🔴"
    lines.append(f"💵 Price: `${price:.4f}`  {chg_icon}`{chg:+.2f}%`")
    lines.append(f"📊 24h Vol: `{vol_str}`")
    if bear:
        bear_score = r.get("bear_score", 0)
        lines.append(f"🔴 Score: `{bear_score}`")
    else:
        lines.append(f"📈 Score: `{score}/100`  Grade: `{grade}`")
    lines.append(f"🔄 Cross: `{cx}` exchanges / `{ctf}` timeframes")
    lines.append("")

    # ── Signals ──
    if bear:
        sigs_raw = r.get("signals_bear", [])
    else:
        sigs_raw = r.get("signals", [])
    if sigs_raw:
        lines.append("🎯 *Signals*")
        for s in sigs_raw:
            s_esc = s.replace("*", "\\*")
            lines.append(f"• {s_esc}")
        lines.append("")

    # ── Warnings ──
    if not bear:
        fail_raw = r.get("signals_fail", [])
        if fail_raw:
            lines.append("⚠️ *Warnings*")
            for s in fail_raw[:3]:
                s_esc = s.replace("*", "\\*")
                lines.append(f"• {s_esc}")
            if len(fail_raw) > 3:
                lines.append(f"• +{len(fail_raw)-3} more")
            lines.append("")

    # ── Patterns ──
    if not bear:
        patterns_raw = r.get("patterns", [])
        if patterns_raw:
            lines.append("📐 *Patterns*")
            for p in patterns_raw:
                p_esc = p.replace("*", "\\*")
                lines.append(f"• `{p_esc}`")
            lines.append("")

    # ── Details ──
    details = r.get("details", [])
    if details:
        lines.append("📋 *Details*")
        for d in details[:3]:
            d_esc = d.replace("*", "\\*")
            lines.append(f"• {d_esc}")
        if len(details) > 3:
            lines.append(f"• +{len(details)-3} more")
        lines.append("")

    # ── Advice ──
    if not bear:
        entry = r.get("entry_advice", "")
        if entry:
            entry_esc = entry.replace("*", "\\*")
            lines.append(f"✅ *Entry*\n{entry_esc}")
    exit_a = r.get("exit_advice", "")
    if exit_a:
        exit_esc = exit_a.replace("*", "\\*")
        lines.append(f"❌ *Exit*\n{exit_esc}")

    lines.append("")
    lines.append(f"🤖 Surge Scanner v3 · {ts}")
    return "\n".join(lines)


def format_summary(
    results: list[dict],
    candidates_count: int,
    kline_groups_count: int,
    ticker_counts: dict,
    cost: float,
) -> str:
    """Format a summary report for Telegram — structured card layout."""
    grade_counts = defaultdict(int)
    bear_count = 0
    for r in results:
        grade_counts[r.get("grade", "D")] += 1
        if r.get("has_bear") and r.get("bear_score", 0) >= 10:
            bear_count += 1

    ts = datetime.now(timezone.utc).strftime("%m-%d %H:%M UTC")

    lines = ["📊 *Surge Scanner Report*"]
    lines.append("")

    # ── Stats line ──
    exch_parts = []
    for name, cnt in sorted(ticker_counts.items()):
        exch_parts.append(f"{name}={cnt}")
    lines.append(f"• `{'  '.join(exch_parts)}`")
    lines.append(f"• Candidates: {candidates_count}  |  Klines: {kline_groups_count}  |  ⏱{cost:.0f}s")
    rapid_total = sum(1 for r in results if r.get("rapid"))
    if rapid_total:
        lines.append(f"• 🚀 Rapid breakout: {rapid_total}")
    sigs = []
    if grade_counts.get("A", 0):
        sigs.append(f"🟢A={grade_counts['A']}")
    if grade_counts.get("B", 0):
        sigs.append(f"🟡B={grade_counts['B']}")
    if grade_counts.get("C", 0):
        sigs.append(f"🟠C={grade_counts['C']}")
    if bear_count:
        sigs.append(f"🔴Bear={bear_count}")
    if grade_counts.get("D", 0):
        sigs.append(f"⚪D={grade_counts['D']}")
    if sigs:
        lines.append(f"• Signals: {'  '.join(sigs)}")
    lines.append("")
    lines.append("─── *Top 10* ───")
    lines.append("")

    # ── Each result as a compact card ──
    for r in results[:10]:
        sym = r.get("sym", "?").replace("USDT", "")
        g = r.get("grade", "D")
        sc = r.get("score", 0)
        chg = r.get("chg24h", 0)
        vol = r.get("vol_24h", 0)
        vol_str = f"{vol/1e6:.1f}M" if vol >= 1e6 else f"{vol/1000:.0f}K" if vol >= 1000 else f"{vol:.0f}"
        sig_cnt = len(r.get("signals", []))
        cx = r.get("cross_exchange", 0)
        icon = {"A": "🟢", "B": "🟡", "C": "🟠", "D": "⚪"}.get(g, "⚪")
        chg_icon = "🟢" if chg > 0 else "🔴"
        rapid_tag = " 🚀" if r.get("rapid") else ""
        pr = r.get("price", 0)
        lines.append(
            f"{icon} *${sym}*{rapid_tag}  {chg_icon}`{chg:+.2f}%`  ${pr:.4f}"
        )
        lines.append(
            f"    Grade {g}({sc})  Vol {vol_str}  Xchg×{cx}  Sig×{sig_cnt}"
        )
    lines.append("")
    lines.append(f"🤖 Surge Scanner v3 · {ts}")
    return "\n".join(lines)


def _tg_escape_md(text: str) -> str:
    """Escape Markdown special characters for Telegram parse_mode=Markdown."""
    # Must escape _, *, `, [ before any other processing
    text = text.replace("_", r"\_")
    text = text.replace("*", r"\*")
    text = text.replace("`", r"\`")
    text = text.replace("[", r"\[")
    return text


def send_tg(text: str, dry: bool = False, summary: bool = False):
    """Send message to Telegram. If dry, print to stdout instead."""
    if dry:
        tag = "[SUMMARY]" if summary else "[SIGNAL]"
        print(f"\n{tag}\n{'='*40}\n{text}\n{'='*40}")
        return

    token = SIGNAL_BOT_TOKEN
    chat = SIGNAL_CHAT_ID
    if not token or not chat:
        logger.warning("SIGNAL_BOT_TOKEN or SIGNAL_CHAT_ID not set")
        return

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning("TG push failed: %d %s", resp.status_code, resp.text[:200])
    except Exception as e:
        logger.warning("TG exception: %s", e)


# ═══════════════════════════════════════════════════════════
# Main pipeline orchestrator
# ═══════════════════════════════════════════════════════════

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Multi-exchange surge scanner with 5-indicator scoring"
    )
    parser.add_argument("--dry", action="store_true", help="Preview only, no TG push")
    parser.add_argument(
        "--max", type=int, default=8, help="Max bull signals to push (default 8)"
    )
    parser.add_argument(
        "--max-bear", type=int, default=3, help="Max bear signals to push (default 3)"
    )
    args = parser.parse_args()

    t0 = _time.monotonic()
    logger.info("🚀 Surge Scanner v3 started")

    # ── Phase 1: Fetch all tickers ────────────────────────
    candidates, exchange_listings = fetch_all_tickers(MIN_VOL_USDT)

    # ── Phase 2: Batch kline fetch ────────────────────────
    kline_groups = fetch_all_klines(exchange_listings, candidates)

    # ── Phase 2b: Detect large taker trades (OKX) ──────────
    large_taker_data: dict[str, bool] = {}
    try:
        symbols_for_lt = list(exchange_listings.get("okx", []))
        lt_syms = [item.get("sym", "") for item in symbols_for_lt if item.get("sym")]
        if lt_syms:
            lt_detector = LargeTakerDetector()
            large_taker_data = lt_detector.detect_large_trades(lt_syms)
            lt_count = sum(1 for v in large_taker_data.values() if v)
            if lt_count:
                logger.info("Phase 2b large taker: %d/%d symbols", lt_count, len(lt_syms))
    except Exception as e:
        logger.warning("Phase 2b large taker detection failed (non-fatal): %s", e)

    # ── Phase 2c: Detect liquidation cascades from kline data ──
    liquidation_data: dict[str, dict] = {}
    try:
        # Build kline lookup: sym -> first available kline dict
        kline_lookup: dict[str, dict] = {}
        for inp in kline_groups:
            sym = inp.get("sym", "")
            kln = inp.get("kline")
            if sym and kln and sym not in kline_lookup:
                kline_lookup[sym] = kln
        if kline_lookup:
            liq_detector = LiquidationDetector()
            raw_liq = liq_detector.detect_liquidation_cascade(kline_lookup)
            liquidation_data = liq_detector.mark_liquidation_signal(raw_liq)
            liq_count = sum(1 for v in liquidation_data.values() if v.get("liquidation_cascade"))
            if liq_count:
                logger.info("Phase 2c liquidation: %d/%d symbols flagged", liq_count, len(liquidation_data))
    except Exception as e:
        logger.warning("Phase 2c liquidation detection failed (non-fatal): %s", e)

    # ── Phase 2d: Fetch extra data (funding rate, L/S ratio) ───
    extras_data: dict[str, dict] = {}
    try:
        extras_data = ExtrasFetcher().fetch_all_extras(list(candidates.keys()))
        if extras_data:
            logger.info("Phase 2d extras: %d symbols with extra data", len(extras_data))
    except Exception as e:
        logger.warning("Phase 2d extras fetch failed (non-fatal): %s", e)

    # ── Merge large taker and liquidation into extras_data ────
    try:
        for sym in list(candidates.keys()):
            if sym not in extras_data:
                extras_data[sym] = {}
            # large_taker: bool or None
            extras_data[sym]["large_taker"] = large_taker_data.get(sym, False)
            # liquidation signals
            liq_info = liquidation_data.get(sym, {})
            extras_data[sym]["liquidation_cascade"] = liq_info.get("liquidation_cascade")
            extras_data[sym]["liquidation_score"] = liq_info.get("liquidation_score", 0)
        logger.debug(
            "Merged large_taker (%d flagged) and liquidation (%d flagged) into extras",
            sum(1 for v in large_taker_data.values() if v),
            sum(1 for v in liquidation_data.values() if v.get("liquidation_cascade")),
        )
    except Exception as e:
        logger.warning("Merge of large_taker/liquidation into extras failed (non-fatal): %s", e)

    # ── Phase 3: Score each kline group ───────────────────
    individual_scores = score_all_klines(kline_groups, extras_data)

    # ── Phase 4: Merge scores by symbol ───────────────────
    merged_results = merge_scores_by_symbol(individual_scores)

    # Inject rapid flag from Phase 1 candidates
    for r in merged_results:
        sym = r.get("sym", "")
        if sym in candidates:
            r["rapid"] = candidates[sym].get("rapid", False)

    cost = _time.monotonic() - t0

    # ── Save merged results to cache (for Square reposter) ──
    _cache_path = Path(DATA_DIR) / "surge_signals_cache.json"
    try:
        _cache_path.parent.mkdir(parents=True, exist_ok=True)
        _cache_path.write_text(
            json.dumps(merged_results, ensure_ascii=False, indent=2, default=str)
        )
    except Exception as e:
        logger.debug("Failed to cache signals: %s", e)

    # ── Phase 5: Push signals + summary ───────────────────
    # Filter out \"dead signals\"
    def is_dead_signal(r: dict) -> bool:
        score = r.get("score", 0)
        chg = abs(r.get("chg24h", 0))
        vol = r.get("vol_24h", 0)
        rapid = r.get("rapid", False)
        if rapid:
            # Rapid channel: accept if score≥20 OR chg≥10% (low-cap breakouts)
            return score < 20 and chg < 10.0
        return score < 60 and chg < 2.0 and vol < 2_000_000

    pre_filter_count = len(merged_results)
    merged_results = [r for r in merged_results if not is_dead_signal(r)]
    dead_count = pre_filter_count - len(merged_results)
    if dead_count:
        logger.info("Filtered %d dead signals (score<60, chg<2%%, vol<2M)", dead_count)

    # Count tickers per exchange for summary
    ticker_counts = {}
    for name, items in exchange_listings.items():
        ticker_counts[name] = len(items)

    # Count A/B/C distribution
    a_list = [r for r in merged_results if r.get("grade") == "A"]
    b_list = [r for r in merged_results if r.get("grade") == "B"]
    c_list = [r for r in merged_results if r.get("grade") == "C"]
    # Bear signals: any result with has_bear=True AND bear_score >= 10
    bear_list = [r for r in merged_results if r.get("has_bear") and r.get("bear_score", 0) >= 10]

    # Composite score: weighted combination of indicator score and volatility
    def composite(r: dict) -> float:
        score = r.get("score", 0)
        chg = abs(r.get("chg24h", 0))
        vol_component = min(100, chg * 3)
        base = score * 0.7 + vol_component * 0.3
        # Boost rapid coins (low-cap breakout) — they have score disadvantage
        # but high volatility makes them interesting
        if r.get("rapid"):
            base *= 1.5
        return base

    # Build ordered list: A signals first (by composite), then B, then C
    ordered = []
    for r in sorted(a_list, key=lambda x: -composite(x)):
        ordered.append((r, False))
    for r in sorted(b_list, key=lambda x: -composite(x)):
        ordered.append((r, False))
    for r in sorted(c_list, key=lambda x: -composite(x)):
        ordered.append((r, False))
    # Bears sorted by bear_score descending
    for r in sorted(bear_list, key=lambda x: -x.get("bear_score", 0)):
        ordered.append((r, True))

    pushed = 0
    bear_pushed = 0
    for r, is_bear in ordered:
        if is_bear and bear_pushed < args.max_bear:
            msg = format_signal(r, bear=True)
            send_tg(msg, args.dry)
            chg = r.get("chg24h", 0)
            logger.info(
                "   Push bear: %s (score=%d, chg=%+.2f%%)",
                r.get("sym", "?"), r.get("bear_score", 0), chg,
            )
            bear_pushed += 1
        elif not is_bear and pushed < args.max:
            msg = format_signal(r)
            send_tg(msg, args.dry)
            grade = r.get("grade", "?")
            score = r.get("score", 0)
            chg = r.get("chg24h", 0)
            if grade == "C" and abs(chg) >= 5.0:
                logger.info(
                    "   Push: %s %s(%d) [vol:%+.2f%%]",
                    r.get("sym", "?"), grade, score, chg,
                )
            else:
                logger.info(
                    "   Push: %s %s(%d/100)",
                    r.get("sym", "?"), grade, score,
                )
            pushed += 1

    if pushed == 0 and bear_pushed == 0:
        logger.info("No signals found")

    # Push summary report
    summary = format_summary(
        merged_results,
        len(candidates),
        len(kline_groups),
        ticker_counts,
        cost,
    )
    send_tg(summary, args.dry, summary=True)

    a_cnt = sum(1 for r in merged_results if r.get("grade") == "A")
    b_cnt = sum(1 for r in merged_results if r.get("grade") == "B")
    c_cnt = sum(1 for r in merged_results if r.get("grade") == "C")
    bear_cnt = sum(1 for r in merged_results if r.get("has_bear") and r.get("bear_score", 0) >= 10)
    logger.info(
        "✅ Done | A=%d B=%d C=%d Bear=%d Pushed=%d Time=%.0fs",
        a_cnt, b_cnt, c_cnt, bear_cnt, pushed, cost,
    )


if __name__ == "__main__":
    main()
