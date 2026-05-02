"""
backtest/deploy_gate.py — DeployGate class.

Production deployment gate for trading strategies. Validates backtest
results against minimum requirements, provides daily health reports,
and monitors live performance via circuit breakers.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable

logger = logging.getLogger(__name__)

# ── default deployment thresholds ──────────────────────────────────

DEPLOY_CONDITIONS: dict[str, float] = {
    "outsample_sharpe": 1.0,
    "max_drawdown": 0.15,
    "win_rate": 0.55,
    "profit_ratio": 1.8,
    "monte_carlo_positive": 0.95,
}


def _default_circuit_breakers() -> list[tuple[str, Callable[[dict[str, Any]], bool]]]:
    """Return the standard list of circuit breaker rules.

    Each breaker is a ``(name, predicate)`` pair where ``predicate(current_stats)``
    returns ``True`` when the breaker is *triggered* (i.e. something is wrong).

    Returns
    -------
    list[tuple[str, Callable]]
        Breaker rules.
    """

    def daily_loss_trigger(stats: dict[str, Any]) -> bool:
        """Trigger if daily loss exceeds 5%."""
        daily_pnl_pct = stats.get("daily_pnl_pct", 0.0)
        return daily_pnl_pct < -5.0

    def consecutive_losses_trigger(stats: dict[str, Any]) -> bool:
        """Trigger if >= 3 consecutive losing trades."""
        consecutive_losses = stats.get("consecutive_losses", 0)
        return consecutive_losses >= 3

    def deviation_trigger(stats: dict[str, Any]) -> bool:
        """Trigger if current drawdown deviates >20% from backtest max_drawdown."""
        current_dd = stats.get("current_drawdown", 0.0)
        expected_dd = stats.get("expected_max_drawdown", 0.15)
        if expected_dd > 0:
            deviation = abs(current_dd - expected_dd) / expected_dd
            return deviation > 0.20
        return False

    return [
        ("daily_loss_exceeds_5pct", daily_loss_trigger),
        ("consecutive_losses_ge_3", consecutive_losses_trigger),
        ("deviation_exceeds_20pct", deviation_trigger),
    ]


# Default circuit breakers list for external access
CIRCUIT_BREAKERS: list[tuple[str, Callable[[dict[str, Any]], bool]]] = (
    _default_circuit_breakers()
)


class DeployGate:
    """Production deployment gate for trading strategies.

    Validates backtest results against minimum thresholds (see
    :data:`DEPLOY_CONDITIONS`), generates Telegram-formatted daily
    reports, and monitors live trading performance via circuit breakers.

    Parameters
    ----------
    conditions : dict, optional
        Override deployment thresholds. Defaults to
        :data:`DEPLOY_CONDITIONS`.

    Examples
    --------
    >>> gate = DeployGate()
    >>> backtest_results = {
    ...     "sharpe_ratio": 3.0,
    ...     "max_drawdown": 0.10,
    ...     "win_rate": 0.60,
    ...     "profit_factor": 2.0,
    ...     "monte_carlo_positive": 0.97,
    ... }
    >>> ok, reasons = gate.can_deploy(backtest_results)
    >>> print(ok, reasons)
    True []
    """

    def __init__(
        self,
        conditions: dict[str, float] | None = None,
    ) -> None:
        self.conditions = conditions or dict(DEPLOY_CONDITIONS)
        self._circuit_breakers: list[tuple[str, Callable[[dict[str, Any]], bool]]] = (
            _default_circuit_breakers()
        )

    # ── deployment check ────────────────────────────────────────────

    def can_deploy(self, backtest_results: dict[str, Any]) -> tuple[bool, list[str]]:
        """Check if a strategy is ready for production deployment.

        Evaluates the following gates:

        - **data_source** — rejects strategies using synthetic data
        - **outsample_sharpe** — out-of-sample Sharpe ratio ≥ threshold
          (checks ``sharpe_ratio`` key)
        - **max_drawdown** — maximum drawdown ≤ threshold (checks
          ``max_drawdown`` key)
        - **win_rate** — win rate ≥ threshold (checks ``win_rate`` key)
        - **profit_ratio** — profit factor ≥ threshold (checks
          ``profit_factor`` key)
        - **monte_carlo_positive** — Monte Carlo positive path ratio ≥
          threshold (checks ``monte_carlo_positive`` or ``positive_paths_ratio``)

        Parameters
        ----------
        backtest_results : dict
            Dict with keys ``sharpe_ratio``, ``max_drawdown``,
            ``win_rate``, ``profit_factor``, and optionally
            ``monte_carlo_positive`` or ``positive_paths_ratio``.

        Returns
        -------
        tuple[bool, list[str]]
            ``(passes, reasons)`` where ``passes`` is True if all gates
            pass, and ``reasons`` lists descriptions of any failures.
        """
        failures: list[str] = []

        # Reject synthetic data — cannot deploy fabricated results
        if backtest_results.get("data_source") == "synthetic":
            failures.append(
                "Cannot deploy: backtest used SYNTHETIC data. "
                "Real market data is required for production deployment."
            )
            return False, failures

        # Map backtest keys to condition keys
        checks = [
            ("sharpe_ratio", "outsample_sharpe", False),
            ("max_drawdown", "max_drawdown", True),
            ("win_rate", "win_rate", False),
            ("profit_factor", "profit_ratio", False),
        ]

        for result_key, condition_key, invert in checks:
            value = backtest_results.get(result_key)
            threshold = self.conditions.get(condition_key, 0.0)
            if value is None:
                failures.append(f"Missing '{result_key}' in backtest results")
                continue
            if invert:
                # For drawdown: value should be ≤ threshold
                if float(value) > float(threshold):
                    failures.append(
                        f"{result_key} ({value}) exceeds max_drawdown threshold ({threshold})"
                    )
            else:
                # For sharpe/win_rate/profit: value should be ≥ threshold
                if float(value) < float(threshold):
                    failures.append(
                        f"{result_key} ({value}) below {condition_key} threshold ({threshold})"
                    )

        # Monte Carlo check
        mc_positive = backtest_results.get(
            "monte_carlo_positive",
            backtest_results.get("positive_paths_ratio", None),
        )
        mc_threshold = self.conditions.get("monte_carlo_positive", 0.95)
        if mc_positive is None:
            failures.append("Missing Monte Carlo positive path ratio")
        elif float(mc_positive) < float(mc_threshold):
            failures.append(
                f"monte_carlo_positive ({mc_positive}) below threshold ({mc_threshold})"
            )

        passed = len(failures) == 0
        return passed, failures

    # ── daily report ────────────────────────────────────────────────

    def get_daily_report(self, stats: dict[str, Any]) -> str:
        """Generate a Telegram-formatted daily performance report.

        Parameters
        ----------
        stats : dict
            Current performance statistics. Expected keys:
            - ``date`` or auto-filled
            - ``daily_pnl_pct`` — daily PnL %
            - ``cumulative_pnl_pct`` — total PnL %
            - ``sharpe_ratio`` — live Sharpe
            - ``max_drawdown`` — current drawdown %
            - ``win_rate`` — win rate %
            - ``total_trades`` — trade count
            - ``open_positions`` — currently open positions
            - Any additional stats will be appended.

        Returns
        -------
        str
            Telegram-formatted report string (supports Telegram markdown).
        """
        date_str = stats.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        daily_pnl = stats.get("daily_pnl_pct", 0.0)
        cum_pnl = stats.get("cumulative_pnl_pct", 0.0)
        sharpe = stats.get("sharpe_ratio", 0.0)
        dd = stats.get("max_drawdown", 0.0)
        win_rate = stats.get("win_rate", 0.0)
        trades = stats.get("total_trades", 0)
        open_pos = stats.get("open_positions", 0)

        # PnL emoji
        daily_emoji = "🟢" if daily_pnl >= 0 else "🔴"
        cum_emoji = "🟢" if cum_pnl >= 0 else "🔴"

        lines = [
            f"📊 *Daily Report — {date_str}*",
            "",
            f"{daily_emoji} Daily PnL: `{daily_pnl:+.2f}%`",
            f"{cum_emoji} Cumulative PnL: `{cum_pnl:+.2f}%`",
            "",
            f"⚡ Sharpe Ratio: `{sharpe:.2f}`",
            f"📉 Max Drawdown: `{dd:.2f}%`",
            f"🎯 Win Rate: `{win_rate:.1f}%`",
            f"🔄 Total Trades: `{trades}`",
            f"📌 Open Positions: `{open_pos}`",
        ]

        # Circuit breaker check
        triggered = self.check_circuit_breakers(stats)
        if triggered:
            lines.append("")
            lines.append("⚠️ *Circuit Breakers Triggered:*")
            for reason in triggered:
                lines.append(f"  • {reason}")

        # Extra stats
        extras = {k: v for k, v in stats.items() if k not in {"date", "daily_pnl_pct", "cumulative_pnl_pct", "sharpe_ratio", "max_drawdown", "win_rate", "total_trades", "open_positions"}}
        if extras:
            lines.append("")
            lines.append("*Additional Stats:*")
            for k, v in extras.items():
                lines.append(f"  • `{k}`: `{v}`")

        return "\n".join(lines)

    # ── circuit breakers ────────────────────────────────────────────

    def check_circuit_breakers(self, current_stats: dict[str, Any]) -> list[str]:
        """Check all circuit breaker rules against current stats.

        Parameters
        ----------
        current_stats : dict
            Current trading statistics (same expected keys as
            :meth:`get_daily_report`).

        Returns
        -------
        list[str]
            Human-readable descriptions of any triggered breakers.
            Empty list means all clear.
        """
        triggered: list[str] = []
        for name, predicate in self._circuit_breakers:
            try:
                if predicate(current_stats):
                    triggered.append(name)
                    logger.warning(f"Circuit breaker triggered: {name}")
            except Exception as exc:
                logger.error(f"Circuit breaker '{name}' check failed: {exc}")
                triggered.append(f"{name} (check error: {exc})")
        return triggered

    @property
    def circuit_breakers(self) -> list[tuple[str, Callable[[dict[str, Any]], bool]]]:
        """Return the current circuit breaker rules.

        Returns
        -------
        list[tuple[str, Callable]]
            Breaker name and predicate pairs.
        """
        return list(self._circuit_breakers)

    def add_circuit_breaker(
        self,
        name: str,
        predicate: Callable[[dict[str, Any]], bool],
    ) -> None:
        """Register a custom circuit breaker rule.

        Parameters
        ----------
        name : str
            Unique identifier for the breaker.
        predicate : Callable
            Function ``(current_stats: dict) -> bool`` that returns
            ``True`` when the breaker should trigger.
        """
        self._circuit_breakers.append((name, predicate))
        logger.info(f"Circuit breaker added: {name}")
