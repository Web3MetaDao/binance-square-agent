"""
backtest/engine.py — BacktestPipeline class.

Runs vectorbt-based backtests on BTC/USDT 4h data from Yahoo Finance,
with a synthetic data fallback for environments where Yahoo is unreachable (e.g., China).
"""

from __future__ import annotations

import logging
from typing import Any, Callable

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Will attempt vectorbt import; provide fallback if not installed
try:
    import vectorbt as vbt
except ImportError:
    vbt = None  # type: ignore[assignment]
    logger.warning("vectorbt not installed; BacktestPipeline will only use synthetic data")


def _synthetic_ohlcv(
    n: int = 4000,
    start_price: float = 20000.0,
    vol: float = 0.02,
    dt: str = "2020-01-01",
    freq: str = "4h",
) -> pd.DataFrame:
    """Generate synthetic OHLCV data for fallback when Yahoo is unreachable.

    Uses geometric Brownian motion to produce realistic-looking 4h klines.

    Parameters
    ----------
    n : int
        Number of bars to generate.
    start_price : float
        Starting price.
    vol : float
        Annualized volatility factor.
    dt : str
        Start date.
    freq : str
        Pandas frequency string, e.g. '4h'.

    Returns
    -------
    pd.DataFrame
        Columns: Open, High, Low, Close, Volume.
    """
    np.random.seed(42)
    dt64 = np.datetime64(dt, "ns").astype("datetime64[ns]")
    index = pd.date_range(dt64.item(), periods=n, freq=freq)
    # Geometric Brownian Motion
    returns = np.random.normal(0, vol / np.sqrt(365 * 6), n)
    price = start_price * np.exp(np.cumsum(returns))
    spread = price * 0.001  # 0.1% spread for high/low
    ohlc = pd.DataFrame(
        {
            "Open": price,
            "High": price + np.abs(np.random.normal(0, spread, n)),
            "Low": price - np.abs(np.random.normal(0, spread, n)),
            "Close": price * np.exp(np.random.normal(0, vol / np.sqrt(365 * 6), n)),
            "Volume": np.random.exponential(1000, n) * 100,
        },
        index=index,
    )
    # Ensure High >= Open/Close and Low <= Open/Close
    ohlc["High"] = np.maximum(ohlc["High"], ohlc[["Open", "Close"]].max(axis=1))
    ohlc["Low"] = np.minimum(ohlc["Low"], ohlc[["Open", "Close"]].min(axis=1))
    return ohlc


def _fetch_data(
    start: str,
    end: str,
    symbol: str = "BTC-USD",
    interval: str = "4h",
) -> tuple[pd.DataFrame, str]:
    """Fetch OHLCV data, falling back to synthetic data if Yahoo fails.

    Returns ``(ohlcv, data_source)`` where ``data_source`` is
    ``'real'`` or ``'synthetic'``.

    Parameters
    ----------
    start : str
        Start date (e.g. '2020-01-01').
    end : str
        End date (e.g. '2024-12-31').
    symbol : str
        Yahoo Finance symbol.
    interval : str
        Data interval.

    Returns
    -------
    tuple[pd.DataFrame, str]
        OHLCV data with Open, High, Low, Close, Volume columns,
        and a data_source label ('real' or 'synthetic').
    """
    if vbt is not None:
        try:
            data = vbt.YFData.download(symbol, start=start, end=end, interval=interval)
            ohlcv = data.get(["Open", "High", "Low", "Close", "Volume"])
            if ohlcv is not None and not ohlcv.empty:
                logger.info(f"Downloaded {len(ohlcv)} bars from Yahoo Finance")
                return ohlcv, "real"
            logger.warning("Yahoo returned empty data; falling back to synthetic")
        except Exception as exc:
            logger.warning(f"Yahoo download failed ({exc}); using synthetic data")
    else:
        logger.info("vectorbt not available; using synthetic data")

    logger.warning(
        "⚠️  SYNTHETIC DATA — no live market data available. "
        "Backtest results are fabricated via GBM and should NOT be trusted for production decisions."
    )
    return _synthetic_ohlcv(n=4000, start_price=20000.0, dt=start), "synthetic"


# ── convenience helpers ─────────────────────────────────────────────


def _compute_stats(result: vbt.Portfolio) -> dict[str, Any]:
    """Extract key metrics from a vectorbt Portfolio result.

    Parameters
    ----------
    result : vbt.Portfolio
        VectorBT portfolio result.

    Returns
    -------
    dict
        Dictionary of strategy performance metrics.
    """
    stats = result.stats()
    sharpe = stats.get("Sharpe Ratio", np.nan)
    if sharpe is None or (isinstance(sharpe, float) and np.isnan(sharpe)):
        # fallback: compute from daily returns
        daily_ret = result.daily_returns()
        if daily_ret is not None and len(daily_ret) > 1:
            sharpe = float(
                np.sqrt(365) * daily_ret.mean() / daily_ret.std()
                if daily_ret.std() > 0
                else 0.0
            )
        else:
            sharpe = 0.0

    max_dd = stats.get("Max Drawdown", 0.0)
    if max_dd is None or (isinstance(max_dd, float) and np.isnan(max_dd)):
        max_dd = 0.0

    win_rate = stats.get("Win Rate", 0.0)
    if win_rate is None or (isinstance(win_rate, float) and np.isnan(win_rate)):
        win_rate = 0.0

    profit_factor = stats.get("Profit Factor", 0.0)
    if profit_factor is None or (isinstance(profit_factor, float) and np.isnan(profit_factor)):
        profit_factor = 0.0

    total_trades = int(stats.get("Total Trades", 0) or 0)
    net_profit = float(stats.get("Total Return", 0.0) or 0.0)
    if isinstance(net_profit, str):
        net_profit = 0.0

    return {
        "sharpe_ratio": round(float(sharpe), 4),
        "max_drawdown": round(float(max_dd), 4),
        "win_rate": round(float(win_rate), 4),
        "profit_factor": round(float(profit_factor), 4),
        "total_trades": total_trades,
        "net_profit": round(float(net_profit), 4),
    }


# Type alias for strategy callable
StrategyFunc = Callable[[pd.DataFrame, dict[str, Any]], pd.Series]


class BacktestPipeline:
    """Multi-phase backtest pipeline for crypto trading strategies.

    Runs ``strategy_code`` on BTC/USDT 4h data over specified date ranges,
    returning a dict of performance metrics.

    Parameters
    ----------
    symbol : str
        Yahoo Finance symbol (default ``BTC-USD``).
    interval : str
        Kline interval (default ``4h``).

    Examples
    --------
    >>> pipeline = BacktestPipeline()
    >>> def my_strategy(ohlcv, params):
    ...     close = ohlcv["Close"]
    ...     fast = params.get("fast", 10)
    ...     slow = params.get("slow", 30)
    ...     entries = close.rolling(fast).mean() > close.rolling(slow).mean()
    ...     exits = ~entries
    ...     return vbt.Signal(entries, exits)
    >>> result = pipeline.run_insample(my_strategy, {"fast": 10, "slow": 30})
    """

    def __init__(
        self,
        symbol: str = "BTC-USD",
        interval: str = "4h",
    ) -> None:
        self.symbol = symbol
        self.interval = interval

    # ── public API ──────────────────────────────────────────────────

    def run_insample(
        self,
        strategy_code: StrategyFunc,
        params: dict[str, Any],
        start: str = "2020-01-01",
        end: str = "2024-12-31",
    ) -> dict[str, Any]:
        """Run in-sample backtest on historical data.

        Parameters
        ----------
        strategy_code : StrategyFunc
            Callable ``(ohlcv: pd.DataFrame, params: dict) -> pd.Series``
            that returns entry positions.
        params : dict
            Strategy parameters (e.g. ``{"fast": 10, "slow": 30}``).
        start : str
            Start date.
        end : str
            End date.

        Returns
        -------
        dict
            Performance metrics.
        """
        ohlcv, data_source = _fetch_data(start, end, self.symbol, self.interval)
        result = self._run(strategy_code, ohlcv, params)
        result["period"] = f"insample {start} → {end}"
        result["data_source"] = data_source
        return result

    def run_outsample(
        self,
        strategy_code: StrategyFunc,
        params: dict[str, Any],
        start: str = "2025-01-01",
    ) -> dict[str, Any]:
        """Run out-of-sample backtest on forward data.

        Parameters
        ----------
        strategy_code : StrategyFunc
            Strategy callable.
        params : dict
            Strategy parameters.
        start : str
            Start date.

        Returns
        -------
        dict
            Performance metrics.
        """
        end = pd.Timestamp.now().strftime("%Y-%m-%d")
        ohlcv, data_source = _fetch_data(start, end, self.symbol, self.interval)
        result = self._run(strategy_code, ohlcv, params)
        result["period"] = f"outsample {start} → {end}"
        result["data_source"] = data_source
        return result

    # ── internals ───────────────────────────────────────────────────

    @staticmethod
    def _run(
        strategy_code: StrategyFunc,
        ohlcv: pd.DataFrame,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute strategy and compute metrics.

        Parameters
        ----------
        strategy_code : StrategyFunc
            Strategy callable.
        ohlcv : pd.DataFrame
            OHLCV data.
        params : dict
            Strategy parameters.

        Returns
        -------
        dict
            Performance metrics.
        """
        if vbt is None:
            return _mock_backtest(ohlcv, strategy_code, params)

        try:
            signals = strategy_code(ohlcv, params)

            # Gracefully handle any signal return format
            entries, exits = _extract_signals(signals, ohlcv)

            pf = vbt.Portfolio.from_signals(
                ohlcv["Close"],
                entries=entries,
                exits=exits,
                direction="both",
            )
            return _compute_stats(pf)

        except Exception as exc:
            logger.warning(f"vectorbt backtest failed ({exc}); falling back to mock")
            return _mock_backtest(ohlcv, strategy_code, params)


def _mock_backtest(
    ohlcv: pd.DataFrame,
    strategy_code: StrategyFunc,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Fallback mock backtest when vectorbt is unavailable or fails.

    Computes simple metrics from the strategy's entry signals using
    a naive hold-to-exit simulation.

    Parameters
    ----------
    ohlcv : pd.DataFrame
        OHLCV data.
    strategy_code : StrategyFunc
        Strategy callable.
    params : dict
        Strategy parameters.

    Returns
    -------
    dict
        Performance metrics.
    """
    close = ohlcv["Close"].values
    try:
        signals = strategy_code(ohlcv, params)
        if isinstance(signals, pd.DataFrame) and "Entries" in signals.columns:
            entries = signals["Entries"].values
        elif isinstance(signals, pd.Series):
            entries = signals.values
        else:
            entries = signals  # type: ignore[assignment]
    except Exception:
        entries = np.zeros(len(close), dtype=bool)

    if not isinstance(entries, np.ndarray):
        entries = np.array(entries, dtype=bool)

    # Simple mock pnl: each entry holds until next entry, uses next-bar return
    pos = 0.0
    trades = []
    for i in range(len(close) - 1):
        if i < len(entries) and entries[i] and pos == 0:
            pos = close[i]
        elif pos > 0 and (i >= len(entries) or entries[i] or i == len(close) - 2):
            ret = (close[i + 1] - pos) / pos
            trades.append(ret)
            pos = 0.0

    if len(trades) == 0:
        return {
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "total_trades": 0,
            "net_profit": 0.0,
        }

    trades_arr = np.array(trades)
    wins = trades_arr[trades_arr > 0]
    losses = trades_arr[trades_arr <= 0]
    win_rate = len(wins) / len(trades_arr) if len(trades_arr) > 0 else 0.0
    profit_factor = (
        abs(wins.sum() / losses.sum()) if len(losses) > 0 and losses.sum() != 0 else float("inf")
    )
    net_profit = float(trades_arr.sum())
    sharpe = (
        float(np.sqrt(365 * 6) * trades_arr.mean() / trades_arr.std())
        if trades_arr.std() > 0
        else 0.0
    )

    # Estimate max drawdown from cumulative returns
    cum_ret = np.cumprod(1 + trades_arr)
    peak = np.maximum.accumulate(cum_ret)
    dd = (cum_ret - peak) / peak
    max_dd = float(np.min(dd)) if len(dd) > 0 else 0.0

    return {
        "sharpe_ratio": round(sharpe, 4),
        "max_drawdown": round(abs(max_dd), 4),
        "win_rate": round(win_rate, 4),
        "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else 999.0,
        "total_trades": len(trades),
        "net_profit": round(net_profit, 4),
    }


def _extract_signals(
    signals: Any,
    ohlcv: pd.DataFrame,
) -> tuple[pd.Series, pd.Series]:
    """Extract entries/exits from any signal return format.

    Supports:
      - (entries, exits) tuple
      - pd.DataFrame with 'Entries'/'Exits' columns
      - pd.Series (entries only; exits = shift(1))
      - vbt.Signal-like object with .entries / .exits attributes
      - numpy array
    """
    # Case 1: tuple (entries, exits)
    if isinstance(signals, (tuple, list)) and len(signals) == 2:
        entries = pd.Series(signals[0], index=ohlcv.index) if not isinstance(signals[0], pd.Series) else signals[0]
        exits = pd.Series(signals[1], index=ohlcv.index) if not isinstance(signals[1], pd.Series) else signals[1]
    # Case 2: DataFrame with columns
    elif isinstance(signals, pd.DataFrame):
        if "Entries" in signals.columns and "Exits" in signals.columns:
            entries = signals["Entries"]
            exits = signals["Exits"]
        else:
            # Use first column as entries, second as exits if available
            entries = signals.iloc[:, 0]
            exits = signals.iloc[:, 1] if signals.shape[1] > 1 else entries.shift(1).fillna(False)
    # Case 3: pd.Series
    elif isinstance(signals, pd.Series):
        entries = signals.astype(bool)
        exits = entries.shift(1).fillna(False)
    # Case 4: object with .entries / .exits (vbt.Signal in 0.x, or custom)
    elif hasattr(signals, "entries"):
        entries = signals.entries
        exits = getattr(signals, "exits", entries.shift(1).fillna(False))
    # Case 5: numpy array or other iterable
    else:
        arr = np.asarray(signals, dtype=bool)
        entries = pd.Series(arr, index=ohlcv.index)
        exits = entries.shift(1).fillna(False)

    # Ensure alignment and boolean type
    entries = entries.reindex(ohlcv.index, fill_value=False).astype(bool)
    exits = exits.reindex(ohlcv.index, fill_value=False).astype(bool)
    return entries, exits
