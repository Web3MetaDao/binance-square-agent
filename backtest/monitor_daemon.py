#!/usr/bin/env python3
"""
backtest/monitor_daemon.py — Circuit breaker monitor daemon.

Loads the latest deploy_control record from StrategyStore, checks circuit
breaker conditions, and sends a Telegram alert if any are triggered.

.. important::

    This daemon is a **no-op** without a real exchange PnL feed.  The
    ``_build_stats`` method returns hardcoded zeros, so circuit breakers
    will never trigger.  To activate monitoring:

    1. Replace ``_build_stats`` with a method that fetches live PnL data
       from your exchange API (e.g. Binance, Bybit).
    2. Ensure ``daily_pnl_pct``, ``consecutive_losses``, and
       ``current_drawdown`` are populated from real data.
    3. The ``active_monitoring`` flag in the result dict will then be True.

Can be run as a cron job every 5 minutes::

    *\\/5 * * * * cd /root/binance-square-agent && \\
        PYTHONPATH=. python3 -m backtest.monitor_daemon 2>>logs/monitor.log
"""

from __future__ import annotations

import json
import logging
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from research.store import StrategyStore
from backtest.deploy_gate import DeployGate

logger = logging.getLogger("monitor_daemon")

# ── Telegram helpers ─────────────────────────────────────────────────

def _load_tg_config() -> tuple[str, str]:
    """Load Telegram bot token and chat ID from environment or .env."""
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

    if not token or not chat_id:
        env_path = Path(__file__).resolve().parent.parent / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                s = line.strip()
                if s.startswith("TG_BOT_TOKEN="):
                    token = s.split("=", 1)[1].strip().strip("\"'")
                elif s.startswith("SIGNAL_BOT_TOKEN="):
                    token = s.split("=", 1)[1].strip().strip("\"'")
                elif s.startswith("TG_CHAT_ID="):
                    chat_id = s.split("=", 1)[1].strip().strip("\"'")

    return token, chat_id


def send_telegram_alert(
    text: str,
    token: Optional[str] = None,
    chat_id: Optional[str] = None,
) -> bool:
    """Send an alert via Telegram Bot API.

    Parameters
    ----------
    text : str
        Alert message text.
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
        logger.warning("Telegram not configured — skipping alert")
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
                logger.info("Telegram alert sent successfully")
                return True
            else:
                logger.warning("Telegram API error: %s", resp_json)
                return False
    except Exception as exc:
        logger.error("Failed to send Telegram alert: %s", exc)
        return False


# ── Monitor logic ────────────────────────────────────────────────────

class CircuitBreakerMonitor:
    """Monitors active deployed strategies for circuit breaker conditions.

    Loads the latest deploy_control records from StrategyStore and checks
    each one against the DeployGate circuit breaker rules.  Alerts are
    sent via Telegram when breakers trigger.

    Parameters
    ----------
    store : StrategyStore, optional
        Database store instance.  Creates a fresh one if omitted.
    gate : DeployGate, optional
        Deployment gate with circuit breaker rules.  Creates one if omitted.
    tg_token : str, optional
        Telegram bot token.  Auto-loaded if omitted.
    tg_chat_id : str, optional
        Telegram chat ID.  Auto-loaded if omitted.
    """

    def __init__(
        self,
        store: Optional[StrategyStore] = None,
        gate: Optional[DeployGate] = None,
        tg_token: Optional[str] = None,
        tg_chat_id: Optional[str] = None,
    ) -> None:
        self.store = store or StrategyStore()
        self.gate = gate or DeployGate()
        self.tg_token, self.tg_chat_id = tg_token, tg_chat_id
        if not self.tg_token or not self.tg_chat_id:
            loaded_token, loaded_chat = _load_tg_config()
            self.tg_token = self.tg_token or loaded_token
            self.tg_chat_id = self.tg_chat_id or loaded_chat

    def run_check(self) -> dict[str, Any]:
        """Check all deployed strategies for circuit breaker triggers.

        Returns
        -------
        dict
            Monitor results with keys:
            - ``checked_at``: ISO-8601 timestamp
            - ``deployed_count``: number of deployed strategies checked
            - ``triggered``: list of dicts with ``fusion_id``, ``breakers``
            - ``alerts_sent``: number of Telegram alerts sent
            - ``active_monitoring``: bool — True only when real PnL data flows
        """
        result: dict[str, Any] = {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "deployed_count": 0,
            "triggered": [],
            "alerts_sent": 0,
            "active_monitoring": False,
            "data_source": "no_data",
        }

        # Find all approved (deployed) strategies
        try:
            dash = self.store.dashboard()
            result["deployed_count"] = dash.get("deployed", 0)
        except Exception as exc:
            logger.error("Failed to get dashboard data: %s", exc)
            result["error"] = str(exc)
            return result

        # Query deploy_control for approved strategies
        try:
            deployed = self._get_deployed_strategies()
        except Exception as exc:
            logger.error("Failed to query deployed strategies: %s", exc)
            result["error"] = str(exc)
            return result

        for entry in deployed:
            fusion_id = entry["strategy_fusion_id"]
            cb_count = entry.get("circuit_break_count", 0)
            stopped = entry.get("stopped", 0)

            if stopped:
                logger.info("Fusion %d already stopped — skipping", fusion_id)
                continue

            # Build a synthetic stats dict from whatever data we have
            stats = self._build_stats(entry)

            # Check if we have real data or just hardcoded zeros
            all_zero = (
                stats.get("daily_pnl_pct", 0.0) == 0.0
                and stats.get("consecutive_losses", 0) == 0
                and stats.get("current_drawdown", 0.0) == 0.0
            )
            if all_zero:
                logger.warning(
                    "Monitor daemon has NO live PnL data for fusion %d. "
                    "Stats are all zeros — circuit breakers will NOT trigger. "
                    "This daemon requires a real exchange PnL feed to function. "
                    "See docstring for integration instructions.",
                    fusion_id,
                )
                # Mark monitoring as inactive; still run breakers in case
                # future deployments provide real data
                result["active_monitoring"] = False
                result["data_source"] = "no_data"
            else:
                result["active_monitoring"] = True
                result["data_source"] = "real"

            triggered = self.gate.check_circuit_breakers(stats)

            if triggered:
                logger.warning(
                    "Circuit breaker triggered for fusion %d: %s",
                    fusion_id,
                    triggered,
                )
                alert_data = {
                    "fusion_id": fusion_id,
                    "breakers": triggered,
                    "stats": stats,
                }
                result["triggered"].append(alert_data)

                # Increment circuit_break_count
                try:
                    self._increment_cb_count(fusion_id)
                except Exception as exc:
                    logger.error(
                        "Failed to increment CB count for fusion %d: %s",
                        fusion_id,
                        exc,
                    )

                # Send Telegram alert
                alert_msg = self._format_alert(alert_data)
                sent = send_telegram_alert(
                    alert_msg, self.tg_token, self.tg_chat_id
                )
                if sent:
                    result["alerts_sent"] += 1

                # Auto-stop if CB count exceeds 3
                new_cb_count = cb_count + 1
                if new_cb_count >= 3:
                    self._stop_strategy(fusion_id)
                    stop_msg = (
                        f"🛑 *Strategy Auto-Stopped*\n"
                        f"Fusion ID `{fusion_id}` has triggered {new_cb_count} "
                        f"circuit breaker events — auto-stopped."
                    )
                    send_telegram_alert(
                        stop_msg, self.tg_token, self.tg_chat_id
                    )
                    result["auto_stopped"] = result.get("auto_stopped", 0) + 1

        # Log summary
        total_triggered = len(result["triggered"])
        if total_triggered > 0:
            logger.info(
                "Monitor check: %d/%d strategies triggered (%d alerts sent)",
                total_triggered,
                result["deployed_count"],
                result["alerts_sent"],
            )
        else:
            logger.info(
                "Monitor check: all %d strategies OK",
                result["deployed_count"],
            )

        return result

    # ── internal helpers ─────────────────────────────────────────────

    def _get_deployed_strategies(self) -> list[dict[str, Any]]:
        """Query the deploy_control table for approved strategies.

        Returns
        -------
        list[dict]
            Rows from deploy_control where approved=1 and stopped=0.
        """
        import sqlite3
        with self.store._conn() as c:
            rows = c.execute(
                """SELECT dc.*, sf.hermes_output, sf.code_extracted,
                          sf.optimized_params
                   FROM deploy_control dc
                   JOIN strategy_fusion sf ON dc.strategy_fusion_id = sf.id
                   WHERE dc.approved = 1
                   ORDER BY dc.updated_at DESC"""
            ).fetchall()
            return [dict(r) for r in rows]

    @staticmethod
    def _build_stats(entry: dict[str, Any]) -> dict[str, Any]:
        """Build a stats dict for circuit breaker checks from a DB row.

        .. note::

            This method currently returns hardcoded zeros because the monitor
            daemon does not have access to a real exchange PnL feed.  In a
            production deployment, replace this method with one that fetches
            live PnL data from the exchange API (e.g. Binance, Bybit).

        Parameters
        ----------
        entry : dict
            Row from deploy_control (joined with strategy_fusion).

        Returns
        -------
        dict
            Stats dict compatible with ``DeployGate.check_circuit_breakers``.
        """
        stats: dict[str, Any] = {
            "daily_pnl_pct": 0.0,
            "consecutive_losses": 0,
            "current_drawdown": 0.0,
            "expected_max_drawdown": 0.15,
        }

        # Try to extract from backtest_cache
        # In a real deployment, this would come from live PnL feed
        params_json = entry.get("optimized_params", "{}")
        try:
            params = json.loads(params_json) if isinstance(params_json, str) else params_json
            if isinstance(params, dict):
                stats["expected_max_drawdown"] = params.get(
                    "max_drawdown", stats["expected_max_drawdown"]
                )
        except (json.JSONDecodeError, TypeError):
            pass

        return stats

    def _increment_cb_count(self, fusion_id: int) -> None:
        """Increment the circuit_break_count for a fusion.

        Parameters
        ----------
        fusion_id : int
            Strategy fusion ID.
        """
        import sqlite3
        with self.store._conn() as c:
            c.execute(
                "UPDATE deploy_control SET circuit_break_count = "
                "circuit_break_count + 1, updated_at = datetime('now') "
                "WHERE strategy_fusion_id = ?",
                (fusion_id,),
            )

    def _stop_strategy(self, fusion_id: int) -> None:
        """Mark a strategy as stopped (permanently halted).

        Parameters
        ----------
        fusion_id : int
            Strategy fusion ID.
        """
        import sqlite3
        with self.store._conn() as c:
            c.execute(
                "UPDATE deploy_control SET stopped = 1, "
                "updated_at = datetime('now') WHERE strategy_fusion_id = ?",
                (fusion_id,),
            )
        self.store.log_event(
            fusion_id,
            "auto_stop",
            f"Auto-stopped after 3+ circuit breaker triggers",
        )
        logger.warning("Strategy fusion %d auto-stopped", fusion_id)

    @staticmethod
    def _format_alert(alert_data: dict[str, Any]) -> str:
        """Format a circuit breaker alert for Telegram.

        Parameters
        ----------
        alert_data : dict
            Dict with ``fusion_id``, ``breakers`` (list of strings),
            and ``stats`` (dict).

        Returns
        -------
        str
            Telegram-markdown formatted alert.
        """
        fusion_id = alert_data.get("fusion_id", "?")
        breakers = alert_data.get("breakers", [])
        stats = alert_data.get("stats", {})

        lines = [
            "⚠️ *Circuit Breaker Alert*",
            "",
            f"Fusion ID: `{fusion_id}`",
            "",
            "*Triggered Rules:*",
        ]
        for b in breakers:
            lines.append(f"  • `{b}`")

        if stats:
            lines.append("")
            lines.append("*Current Stats:*")
            for k, v in stats.items():
                lines.append(f"  • `{k}`: `{v}`")

        lines.append("")
        lines.append(f"`{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC`")
        return "\n".join(lines)


# ── CLI entry point ──────────────────────────────────────────────────

def main() -> None:
    """CLI entry point for the monitor daemon / cron."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    monitor = CircuitBreakerMonitor()
    try:
        result = monitor.run_check()
        # Print condensed summary to stdout for cron log
        triggered = len(result.get("triggered", []))
        alerts = result.get("alerts_sent", 0)
        stopped = result.get("auto_stopped", 0)
        print(
            f"[{result['checked_at']}] "
            f"deployed={result['deployed_count']} "
            f"triggered={triggered} "
            f"alerts={alerts} "
            f"auto_stopped={stopped}"
        )
    except KeyboardInterrupt:
        print("Monitor daemon interrupted")
        sys.exit(0)
    except Exception as exc:
        logger.exception("Monitor daemon failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
