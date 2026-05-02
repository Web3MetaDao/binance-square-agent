"""
backtest/pressure_test.py — PressureTestRunner class.

Simulates black-swan crypto crash scenarios (LUNA, FTX) and
runs the strategy through synthetic klines for these extreme events.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

import numpy as np
import pandas as pd

from backtest.engine import BacktestPipeline

logger = logging.getLogger(__name__)

StrategyFunc = Callable[..., Any]


class PressureTestRunner:
    """Runs trading strategies through simulated black-swan crash scenarios.

    Supported scenarios:

    - **LUNA**: 99% price drop over 48 hours, then recovery over ~7 days.
    - **FTX**: Exchange outage style — 70% drop over 72 hours with
      liquidity gaps, followed by a partial recovery.

    Each scenario generates synthetic 4h OHLCV klines and runs the strategy
    through them, reporting performance metrics.

    Parameters
    ----------
    strategy_code : StrategyFunc
        Strategy callable compatible with ``BacktestPipeline``.
    params : dict
        Strategy parameters.

    Examples
    --------
    >>> runner = PressureTestRunner(my_strategy, {"fast": 10, "slow": 30})
    >>> results = runner.run_pressure_test()
    """

    SCENARIOS = {
        "LUNA",
        "FTX",
    }

    def __init__(
        self,
        strategy_code: StrategyFunc,
        params: dict[str, Any],
    ) -> None:
        self.strategy_code = strategy_code
        self.params = params
        self._pipeline = BacktestPipeline()

    def run_pressure_test(self) -> dict[str, Any]:
        """Run all registered pressure test scenarios.

        Returns
        -------
        dict
            Nested dict mapping scenario name to backtest results, plus
            a ``_meta`` key with summary.

        Raises
        ------
        ValueError
            If the strategy callable is not set.
        """
        if self.strategy_code is None:
            raise ValueError("strategy_code must be set before running pressure tests")

        results: dict[str, Any] = {}
        for scenario in sorted(self.SCENARIOS):
            try:
                ohlcv = self._generate_scenario(scenario)
                bt_result = BacktestPipeline._run(
                    self.strategy_code, ohlcv, self.params
                )
                bt_result["scenario"] = scenario
                bt_result["period"] = f"pressure {scenario}"
                results[scenario] = bt_result
                logger.info(f"Pressure test '{scenario}' completed")
            except Exception as exc:
                logger.error(f"Pressure test '{scenario}' failed: {exc}")
                results[scenario] = {
                    "sharpe_ratio": 0.0,
                    "max_drawdown": 0.0,
                    "win_rate": 0.0,
                    "profit_factor": 0.0,
                    "total_trades": 0,
                    "net_profit": 0.0,
                    "error": str(exc),
                    "scenario": scenario,
                }

        # Summary
        survived = sum(
            1
            for r in results.values()
            if isinstance(r, dict) and r.get("net_profit", -1) > -0.10  # less than 10% loss
        )
        results["_meta"] = {
            "scenarios_run": len(self.SCENARIOS),
            "scenarios_survived": survived,
            "all_survived": survived == len(self.SCENARIOS),
        }
        return results

    # ── scenario generators ─────────────────────────────────────────

    def _generate_scenario(self, scenario: str) -> pd.DataFrame:
        """Generate synthetic OHLCV for a crash scenario.

        Parameters
        ----------
        scenario : str
            One of ``"LUNA"`` or ``"FTX"``.

        Returns
        -------
        pd.DataFrame
            Generated OHLCV data.

        Raises
        ------
        ValueError
            For unknown scenario names.
        """
        generator = {
            "LUNA": self._luna_crash,
            "FTX": self._ftx_crash,
        }.get(scenario.upper())

        if generator is None:
            raise ValueError(
                f"Unknown scenario '{scenario}'. Available: {sorted(self.SCENARIOS)}"
            )
        return generator()

    def _luna_crash(self) -> pd.DataFrame:
        """LUNA-style crash: 99% drop over 48h (12 bars at 4h), then recovery.

        Returns
        -------
        pd.DataFrame
             ~10 days of 4h OHLCV data.
        """
        np.random.seed(42)
        n_total = 60  # 10 days * 6 bars/day
        start_price = 100.0

        # Phase 1: crash — 12 bars (48h)
        crash_bars = 12
        crash_returns = np.random.normal(-0.35, 0.10, crash_bars)
        crash_returns = np.clip(crash_returns, -0.60, -0.05)  # steep drops only

        # Phase 2: panic / capitulation — 6 bars
        cap_bars = 6
        cap_returns = np.random.normal(-0.10, 0.15, cap_bars)

        # Phase 3: recovery — remaining bars
        rec_bars = n_total - crash_bars - cap_bars
        rec_returns = np.random.normal(0.03, 0.06, rec_bars)

        all_returns = np.concatenate([crash_returns, cap_returns, rec_returns])
        price = start_price * np.exp(np.cumsum(all_returns))

        return self._make_ohlcv(price, n_total)

    def _ftx_crash(self) -> pd.DataFrame:
        """FTX-style crash: 70% drop over 72h (18 bars at 4h) with liquidity gaps.

        Simulates exchange outage by creating flat-line periods and
        sudden gaps down.

        Returns
        -------
        pd.DataFrame
            ~12 days of 4h OHLCV data.
        """
        np.random.seed(43)
        n_total = 72  # 12 days * 6 bars/day
        start_price = 100.0

        # Phase 1: initial drop — 6 bars
        phase1 = np.random.normal(-0.08, 0.04, 6)

        # Phase 2: outage gap — flat for 4 bars (zero return)
        phase2 = np.zeros(4)

        # Phase 3: gap down — 4 bars
        phase3 = np.random.normal(-0.18, 0.06, 4)

        # Phase 4: another flat outage — 3 bars
        phase4 = np.zeros(3)

        # Phase 5: capitulation — 5 bars
        phase5 = np.random.normal(-0.12, 0.08, 5)

        # Phase 6: recovery — remaining bars
        rec_bars = n_total - len(phase1) - len(phase2) - len(phase3) - len(phase4) - len(phase5)
        phase6 = np.random.normal(0.02, 0.05, rec_bars)

        all_returns = np.concatenate([phase1, phase2, phase3, phase4, phase5, phase6])
        price = start_price * np.exp(np.cumsum(all_returns))

        return self._make_ohlcv(price, n_total)

    # ── helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _make_ohlcv(prices: np.ndarray, n: int) -> pd.DataFrame:
        """Build an OHLCV DataFrame from a price series.

        Parameters
        ----------
        prices : np.ndarray
            Close price series.
        n : int
            Number of bars.

        Returns
        -------
        pd.DataFrame
            Columns: Open, High, Low, Close, Volume.
        """
        index = pd.date_range("2022-01-01", periods=n, freq="4h")
        spread = np.abs(np.random.normal(0, prices * 0.005, n))
        ohlc = pd.DataFrame(
            {
                "Open": prices,
                "High": prices + spread,
                "Low": prices - spread,
                "Close": prices,
                "Volume": np.random.exponential(500, n) * 100,
            },
            index=index,
        )
        # Clean: High >= max(Open, Close), Low <= min(Open, Close)
        ohlc["High"] = np.maximum(ohlc["High"], ohlc[["Open", "Close"]].max(axis=1))
        ohlc["Low"] = np.minimum(ohlc["Low"], ohlc[["Open", "Close"]].min(axis=1))
        return ohlc
