#!/usr/bin/env python3
"""
backtest/cron_daily.py — Daily research pipeline CLI entry point.

Runs the DailyResearchPipeline and sends a Telegram summary report.

Modes:
  --mode full           Run everything (default)
  --mode harvest-only   Only harvest + parse + store
  --mode backtest-only  Only run backtest + deploy gate (requires existing data)
  --mode report-only    Only generate and send a dashboard report

Usage:
  python3 -m backtest.cron_daily --mode full
  python3 -m backtest.cron_daily --mode report-only
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from research.pipeline import DailyResearchPipeline
from research.store import StrategyStore
from backtest.deploy_gate import DeployGate

logger = logging.getLogger("cron_daily")

# ── Telegram config ──────────────────────────────────────────────────

def _load_tg_config() -> tuple[str, str]:
    """Load Telegram bot token and chat ID from environment or .env.

    Checks (in order):
      1. ``TG_BOT_TOKEN`` / ``TG_CHAT_ID`` (local project convention)
      2. ``TELEGRAM_BOT_TOKEN`` / ``TELEGRAM_CHAT_ID`` (standard)
      3. ``SIGNAL_BOT_TOKEN`` / ``<same chat>`` (surge_scanner convention)

    Returns
    -------
    tuple[str, str]
        ``(bot_token, chat_id)`` — may be empty strings.
    """
    token = (
        os.environ.get("TG_BOT_TOKEN")
        or os.environ.get("TELEGRAM_BOT_TOKEN")
        or os.environ.get("SIGNAL_BOT_TOKEN")
        or ""
    )
    chat_id = (
        os.environ.get("TG_CHAT_ID")
        or os.environ.get("TELEGRAM_CHAT_ID")
        or os.environ.get("TELEGRAM_ALLOWED_USERS")
        or ""
    )

    # Fallback: try reading .env manually
    if not token or not chat_id:
        env_path = Path(__file__).resolve().parent.parent / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                s = line.strip()
                if s.startswith("TG_BOT_TOKEN=") or s.startswith("SIGNAL_BOT_TOKEN="):
                    token = s.split("=", 1)[1].strip().strip("\"'")
                elif s.startswith("TG_CHAT_ID="):
                    chat_id = s.split("=", 1)[1].strip().strip("\"'")

    return token, chat_id


def send_telegram_message(
    text: str,
    token: Optional[str] = None,
    chat_id: Optional[str] = None,
) -> bool:
    """Send a message via Telegram Bot API.

    Parameters
    ----------
    text : str
        Message text (Telegram markdown supported).
    token : str, optional
        Bot API token.  Auto-loaded if omitted.
    chat_id : str, optional
        Chat ID.  Auto-loaded if omitted.

    Returns
    -------
    bool
        True if sent successfully.
    """
    if token is None or chat_id is None:
        loaded_token, loaded_chat = _load_tg_config()
        token = token or loaded_token
        chat_id = chat_id or loaded_chat

    if not token or not chat_id:
        logger.warning("Telegram not configured — skipping message")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": "true",
    }).encode()

    try:
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode()
            resp_json = json.loads(body)
            if resp_json.get("ok"):
                logger.info("Telegram message sent successfully")
                return True
            else:
                logger.warning("Telegram API error: %s", resp_json)
                return False
    except Exception as exc:
        logger.error("Failed to send Telegram message: %s", exc)
        return False


# ── Report formatter ─────────────────────────────────────────────────

def format_daily_summary(summary: dict[str, Any]) -> str:
    """Format a pipeline summary dict into a Telegram-ready message.

    Parameters
    ----------
    summary : dict
        The full summary dict from ``DailyResearchPipeline.run_full_cycle()``.

    Returns
    -------
    str
        Telegram-markdown formatted report.
    """
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    steps = summary.get("steps", {})
    errors = summary.get("errors", [])

    lines = [
        f"📡 *Research Pipeline Report — {date_str}*",
        "",
    ]

    # ── Harvest ──
    harvest = steps.get("harvest", {})
    harvest_status = _status_emoji(harvest.get("status", "error"))
    total = harvest.get("total_items", 0)
    by_source = harvest.get("by_source", {})
    source_parts = " | ".join(
        f"{k}: {v}" for k, v in by_source.items()
    )
    lines.append(f"{harvest_status} *Harvest:* {total} items ({source_parts})")

    # ── Parse ──
    parse = steps.get("parse", {})
    parse_status = _status_emoji(parse.get("status", "error"))
    parsed = parse.get("total_parsed", 0)
    errs = len(parse.get("parse_errors", []))
    lines.append(f"{parse_status} *Parse:* {parsed} strategies ({errs} errors)")

    # ── Store ──
    store = steps.get("store", {})
    store_status = _status_emoji(store.get("status", "error"))
    stored = store.get("stored_count", 0)
    lines.append(f"{store_status} *Store:* {stored} saved to DB")

    # ── Fusion ──
    fusion = steps.get("fusion", {})
    fusion_status = _status_emoji(fusion.get("status", "error"))
    fusion_id = fusion.get("fusion_id")
    candidates = fusion.get("candidate_count", 0)
    scheme = fusion.get("best_scheme", "")
    if fusion_id:
        meta = fusion.get("predicted_metrics", {})
        sharpe = meta.get("sharpe_ratio", "?")
        lines.append(
            f"{fusion_status} *Fusion:* id={fusion_id} | candidates={candidates} "
            f"| sharpe={sharpe}"
        )
        if scheme:
            lines.append(f"  └ Best: _{scheme}_")
    else:
        lines.append(f"{fusion_status} *Fusion:* (none)")

    # ── Review ──
    review = steps.get("review", {})
    if review.get("status") != "skipped":
        review_status = _status_emoji(review.get("status", "error"))
        verdict = review.get("verdict", "unknown")
        score = review.get("risk_score", "?")
        emoji = {"pass": "🟢", "warning": "🟡", "fail": "🔴"}.get(verdict, "⚪")
        lines.append(f"{review_status} *Review:* {emoji} {verdict} (risk={score})")

    # ── Backtest ──
    backtest = steps.get("backtest", {})
    if backtest.get("status") != "skipped" and backtest.get("status") != "error":
        bt_status = _status_emoji(backtest.get("status", "error"))
        levels = backtest.get("levels", {})
        lines.append(f"{bt_status} *Backtest:*")
        for level_name in ("insample", "outsample", "pressure", "slippage", "monte_carlo"):
            level = levels.get(level_name, {})
            if not level:
                continue
            if "error" in level:
                lines.append(f"  └ {level_name}: ❌ {level['error']}")
            elif level_name == "monte_carlo":
                pos = level.get("positive_paths_ratio", 0)
                lines.append(f"  └ MC: positive={pos:.1%}")
            elif level_name == "pressure":
                meta = level.get("_meta", {})
                survived = meta.get("scenarios_survived", 0)
                total_sc = meta.get("scenarios_run", 0)
                lines.append(f"  └ pressure: {survived}/{total_sc} survived")
            else:
                sharpe = level.get("sharpe_ratio", 0)
                dd = level.get("max_drawdown", 0)
                wr = level.get("win_rate", 0)
                lines.append(
                    f"  └ {level_name}: sharpe={sharpe:.2f} dd={dd:.1%} wr={wr:.1%}"
                )

    # ── Deploy Gate ──
    gate = steps.get("deploy_gate", {})
    if gate.get("status") != "skipped":
        approved = gate.get("approved", False)
        reasons = gate.get("reasons", [])
        if approved:
            lines.append("")
            lines.append("✅ *Deploy Gate: APPROVED*")
        else:
            lines.append("")
            lines.append(f"❌ *Deploy Gate: REJECTED*")
            for r in reasons:
                lines.append(f"  └ {r}")

    # ── Errors ──
    if errors:
        lines.append("")
        lines.append("⚠️ *Errors:*")
        for e in errors:
            lines.append(f"  └ {e}")

    # ── Step-level errors ──
    failed_steps = [
        name
        for name, step in steps.items()
        if step.get("status") == "error"
    ]
    if failed_steps:
        lines.append("")
        lines.append("⚠️ *Failed Steps:* " + ", ".join(failed_steps))

    # ── Footer ──
    lines.append("")
    lines.append(f"`{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC`")

    return "\n".join(lines)


def _status_emoji(status: str) -> str:
    """Map a step status to an emoji."""
    return {"ok": "✅", "error": "❌", "skipped": "⏭️"}.get(status, "❓")


def format_dashboard_report(store: StrategyStore) -> str:
    """Generate a dashboard-only report from StrategyStore.

    Parameters
    ----------
    store : StrategyStore
        Database store instance.

    Returns
    -------
    str
        Telegram-markdown formatted report.
    """
    dash = store.dashboard()
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    by_status = dash.get("fusion_by_status", {})

    lines = [
        f"📊 *Research Dashboard — {date_str}*",
        "",
        f"📚 Total Strategies: `{dash.get('total_strategies', 0)}`",
        f"🆕 New (24h): `{dash.get('new_last_24h', 0)}`",
        f"🔄 Pending/Backtesting: `{dash.get('pending_backtesting', 0)}`",
        f"✅ Deployed: `{dash.get('deployed', 0)}`",
        f"📋 Events (24h): `{dash.get('events_last_24h', 0)}`",
        "",
        "*Fusion Status:*",
    ]
    for status, count in sorted(by_status.items()):
        lines.append(f"  └ `{status}`: `{count}`")

    lines.append("")
    lines.append(f"`{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC`")
    return "\n".join(lines)


# ── CLI ──────────────────────────────────────────────────────────────

def main() -> None:
    """CLI entry point for the daily research cron."""
    parser = argparse.ArgumentParser(
        description="Daily research pipeline cron — harvest, parse, fuse, backtest, deploy"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "harvest-only", "backtest-only", "report-only"],
        default="full",
        help="Pipeline mode (default: full)",
    )
    parser.add_argument(
        "--no-telegram",
        action="store_true",
        help="Skip sending Telegram report (print to stdout instead)",
    )
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    store = StrategyStore()
    summary: dict[str, Any] = {
        "pipeline_start": datetime.now(timezone.utc).isoformat(),
        "mode": args.mode,
        "steps": {},
        "approved": False,
    }

    try:
        if args.mode == "full":
            pipeline = DailyResearchPipeline(store=store)
            summary = pipeline.run_full_cycle()

        elif args.mode == "harvest-only":
            from research.harvester import UnifiedHarvester

            harvester = UnifiedHarvester(max_items_per_source=10)
            harvest_data = harvester.harvest_all()
            summary["steps"]["harvest"] = {
                "status": "ok",
                "total_items": harvest_data.get("total_count", 0),
                "by_source": {
                    s: len(harvest_data.get(s, []))
                    for s in ("github", "arxiv", "blog")
                },
            }
            logger.info(
                "Harvest-only: %d items collected",
                harvest_data.get("total_count", 0),
            )

        elif args.mode == "backtest-only":
            fusions = store.get_backtesting_fusions()
            if not fusions:
                logger.info("No backtesting fusions found — nothing to do")
                summary["steps"]["backtest"] = {
                    "status": "skipped",
                    "reason": "no backtesting fusions",
                }
            else:
                pipeline = DailyResearchPipeline(store=store)
                for fusion in fusions:
                    fid = fusion["id"]
                    summary["fusion_id"] = fid
                    summary["steps"]["backtest"] = pipeline._step_backtest(fid)
                    summary["steps"]["deploy_gate"] = pipeline._step_deploy_gate(
                        fid, summary["steps"]["backtest"]
                    )
                    summary["approved"] = summary["steps"]["deploy_gate"].get(
                        "approved", False
                    )

        elif args.mode == "report-only":
            # Just send the dashboard report
            report = format_dashboard_report(store)
            print(report)
            if not args.no_telegram:
                send_telegram_message(report)
            return

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user (Ctrl+C)")
        summary["interrupted"] = True
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        summary["fatal_error"] = str(exc)

    # Build and send report
    report = format_daily_summary(summary)
    print(report)

    if not args.no_telegram and not summary.get("interrupted"):
        send_telegram_message(report)

    # Exit with non-zero if fatal error
    if summary.get("fatal_error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
