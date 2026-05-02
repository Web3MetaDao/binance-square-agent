"""
OKX CLI Wrapper — Pythonic interface for the @okx_ai/okx-trade-cli binary.

|All commands are called via subprocess with --json flag and parsed into Python
|dict/list structures. Public market data requires no API key. Private modules
|(smartmoney, news, trade, swap) require OKX API credentials configured via
|`okx config init`.
|
|Key API modules (all verified working with live key):
|  - market:      Ticker, candles, orderbook (public — no key needed)
|  - smartmoney:  Trader leaderboard, consensus signals (key required)
|  - news:        News by coin, sentiment analysis (key required)
|  - account:     Balance, positions, bills (key required)
|  - swap:        Swap order placement, position management (key required)
|
|Usage:
|    from okx_wrapper import OKXWrapper
|
|    okx = OKXWrapper(profile='live')
|    ticker = okx.get_ticker('BTC-USDT')
|    smartmoney_traders = okx.get_smartmoney_traders(limit=5)
"""

import subprocess
import json
import time
import os
import threading
from datetime import datetime, timedelta
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class OKXAPIError(Exception):
    """Raised when the OKX CLI returns a non-zero exit code or JSON error."""
    def __init__(self, message: str, command: str, exit_code: int, stderr: str = ""):
        self.command = command
        self.exit_code = exit_code
        self.stderr = stderr
        super().__init__(f"[exit={exit_code}] {message} | cmd={command}")


class OKXNotConfiguredError(OKXAPIError):
    """Raised when a private command is called without API credentials."""
    pass


class OKXRateLimitError(OKXAPIError):
    """Raised when OKX rate limit (20 req/2s) is exceeded."""
    pass


# ---------------------------------------------------------------------------
# Cache entry
# ---------------------------------------------------------------------------

class _CacheEntry:
    def __init__(self, data: Any, ttl_seconds: float = 30.0):
        self.data = data
        self.expires_at = time.time() + ttl_seconds

    @property
    def is_expired(self) -> bool:
        return time.time() > self.expires_at


# ---------------------------------------------------------------------------
# OKXWrapper
# ---------------------------------------------------------------------------

class OKXWrapper:
    """
    Pythonic wrapper around the 'okx' CLI binary.

    All public market commands are read-only and require no API credentials.
    SmartMoney, News, and Trade commands require credentials configured via
    `okx config init`.

    Args:
        profile: OKX config profile name (default 'live').
        cache_ttl: Cache TTL in seconds for public market data (default 30).
    """

    OKX_BIN = "okx"

    # Rate limiting: max 20 requests per 2 seconds
    RATE_LIMIT_WINDOW = 2.0  # seconds
    RATE_LIMIT_MAX = 18      # use 18 to leave headroom

    # SmartMoney/News/Trade require API key — this check is done lazily
    __key_checked: bool = False
    __has_api_key: bool = False
    __lock = threading.Lock()

    def __init__(self, profile: str = "hermes-trader", cache_ttl: float = 30.0):
        self.profile = profile
        self.cache_ttl = cache_ttl
        self._cache: dict[str, _CacheEntry] = {}
        self._request_timestamps: list[float] = []

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _wait_for_rate_limit(self) -> None:
        """Ensure we don't exceed 20 requests per 2 seconds."""
        now = time.time()
        self._request_timestamps = [t for t in self._request_timestamps
                                    if t > now - self.RATE_LIMIT_WINDOW]
        if len(self._request_timestamps) >= self.RATE_LIMIT_MAX:
            sleep_time = self._request_timestamps[0] + self.RATE_LIMIT_WINDOW - now
            if sleep_time > 0:
                time.sleep(sleep_time)
        self._request_timestamps.append(time.time())

    # ------------------------------------------------------------------
    # CLI invocation
    # ------------------------------------------------------------------

    def _run(self, *args: str, check_api_key: bool = False) -> Any:
        """
        Execute an okx CLI command and return parsed JSON.

        Args:
            *args: Command arguments (e.g., 'market', 'ticker', 'BTC-USDT').
            check_api_key: If True, verify API credentials before running.

        Returns:
            Parsed JSON (list or dict).

        Raises:
            OKXAPIError: On CLI errors.
            OKXNotConfiguredError: If check_api_key=True and no API key found.
        """
        if check_api_key:
            self._ensure_api_key()

        self._wait_for_rate_limit()

        cmd = [self.OKX_BIN, f"--profile={self.profile}", *args, "--json"]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except subprocess.TimeoutExpired:
            raise OKXAPIError("Command timed out (30s)", " ".join(cmd), -1)

        if result.returncode != 0:
            err_msg = result.stderr.strip() or result.stdout.strip() or "Unknown error"

            # Check for "not available in demo" -> credential issue
            if "not available in demo" in err_msg or "401" in err_msg:
                raise OKXNotConfiguredError(
                    err_msg, " ".join(cmd), result.returncode, result.stderr
                )

            raise OKXAPIError(err_msg, " ".join(cmd), result.returncode, result.stderr)

        # Parse JSON output
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as e:
            raise OKXAPIError(
                f"JSON parse error: {e}. Raw: {result.stdout[:500]}",
                " ".join(cmd), -1, result.stderr
            )

    def _cached(self, cache_key: str, ttl: float, *args: str) -> Any:
        """Run command with caching."""
        if cache_key in self._cache and not self._cache[cache_key].is_expired:
            return self._cache[cache_key].data

        result = self._run(*args)
        self._cache[cache_key] = _CacheEntry(result, ttl)
        return result

    # ------------------------------------------------------------------
    # API key check
    # ------------------------------------------------------------------

    def _ensure_api_key(self) -> None:
        """
        Verify that API credentials are configured.
        This is checked once per process lifetime.
        """
        with self.__lock:
            if self.__key_checked:
                if not self.__has_api_key:
                    raise OKXNotConfiguredError(
                        "API key not configured. Run 'okx config init' first.",
                        "config show", -1
                    )
                return

            try:
                # Simply check if 'okx account balance --json' works
                test_result = subprocess.run(
                    [self.OKX_BIN, "account", "balance", "--json"],
                    capture_output=True, text=True, timeout=10,
                )
                has_key = test_result.returncode == 0
                if not has_key:
                    # Fallback: check if config has api_key field
                    cfg_result = subprocess.run(
                        [self.OKX_BIN, "config", "show", "--json"],
                        capture_output=True, text=True, timeout=10,
                    )
                    if cfg_result.returncode == 0:
                        cfg = json.loads(cfg_result.stdout)
                        profiles = cfg.get("profiles", {})
                        has_key = any(
                            p.get("api_key", "").strip() for p in profiles.values()
                        )
                self.__has_api_key = has_key
            except Exception:
                self.__has_api_key = False

            self.__key_checked = True

            if not self.__has_api_key:
                raise OKXNotConfiguredError(
                    "API key not configured. Run 'okx config init' first.",
                    "config show", -1
                )

    # ------------------------------------------------------------------
    # Public Market Data (no API key required)
    # ------------------------------------------------------------------

    def get_ticker(self, instId: str) -> dict:
        """Get ticker for a single instrument."""
        return self._cached(f"ticker:{instId}", self.cache_ttl, "market", "ticker", instId)

    def get_tickers(self, instType: str = "SWAP") -> list:
        """Get all tickers for an instrument type."""
        return self._cached(f"tickers:{instType}", self.cache_ttl, "market", "tickers", instType)

    def get_candles(self, instId: str, bar: str = "5m", limit: int = 50,
                    after: Optional[int] = None, before: Optional[int] = None) -> list:
        """Get OHLCV candles."""
        args = ["market", "candles", instId, "--bar", bar, "--limit", str(limit)]
        if after:
            args.extend(["--after", str(after)])
        if before:
            args.extend(["--before", str(before)])
        return self._run(*args)

    def get_funding_rate(self, instId: str, history: bool = False,
                         limit: int = 10) -> dict:
        """Get current or historical funding rate (SWAP only)."""
        args = ["market", "funding-rate", instId, "--limit", str(limit)]
        if history:
            args.append("--history")
        return self._run(*args)

    def get_open_interest(self, instType: str = "SWAP",
                          instId: Optional[str] = None) -> list:
        """Get open interest."""
        args = ["market", "open-interest", "--instType", instType]
        if instId:
            args.extend(["--instId", instId])
        return self._cached(f"oi:{instType}:{instId or '*'}", self.cache_ttl, *args)

    def get_market_filter(self, instType: str = "SWAP", **kwargs) -> list:
        """
        Multi-dimensional market screener.
        Supports: sortBy, sortOrder, limit, quoteCcy, minChg24hPct, maxChg24hPct,
                  minMarketCapUsd, maxMarketCapUsd, minVolUsd24h, maxVolUsd24h,
                  minFundingRate, maxFundingRate, minOiUsd, maxOiUsd, ctType, etc.
        """
        args = ["market", "filter", "--instType", instType]
        for key, value in kwargs.items():
            # Convert snake_case to camelCase for CLI params
            cli_key = key.replace("_", "")
            args.extend([f"--{cli_key}", str(value)])

        # Build cache key from all args
        cache_key = f"filter:{instType}:{hash(frozenset(kwargs.items()))}"
        result = self._cached(cache_key, 15.0, *args)
        # Filter returns [{rows: [...]}] — unpack to flat list of tickers
        if isinstance(result, list) and len(result) > 0:
            flattened = []
            for entry in result:
                rows = entry.get("rows", [])
                if isinstance(rows, list):
                    flattened.extend(rows)
            if len(flattened) > 0:
                return flattened
        return result

    def get_oi_history(self, instId: str, bar: str = "1H",
                       limit: int = 50) -> list:
        """OI history time series."""
        args = [
            "market", "oi-history", instId,
            "--bar", bar, "--limit", str(limit)
        ]
        return self._run(*args)

    def get_oi_change(self, instType: str = "SWAP",
                      sort_by: str = "oiDeltaPct",
                      sort_order: str = "desc",
                      limit: int = 20,
                      min_oi_usd: Optional[float] = None,
                      min_vol_usd_24h: Optional[float] = None,
                      min_abs_oi_delta_pct: Optional[float] = None) -> list:
        """Find instruments with largest OI changes."""
        args = [
            "market", "oi-change", "--instType", instType,
            "--sortBy", sort_by, "--sortOrder", sort_order,
            "--limit", str(limit)
        ]
        if min_oi_usd is not None:
            args.extend(["--minOiUsd", str(min_oi_usd)])
        if min_vol_usd_24h is not None:
            args.extend(["--minVolUsd24h", str(min_vol_usd_24h)])
        if min_abs_oi_delta_pct is not None:
            args.extend(["--minAbsOiDeltaPct", str(min_abs_oi_delta_pct)])
        return self._cached(f"oi_change:{instType}:{sort_by}:{limit}", 15.0, *args)

    def get_indicator(self, indicator: str, instId: str,
                      bar: str = "1H", params: Optional[str] = None,
                      limit: int = 10) -> Any:
        """Get technical indicator values (70+ indicators supported)."""
        args = ["market", "indicator", indicator, instId, "--bar", bar,
                "--limit", str(limit)]
        if params:
            args.extend(["--params", params])
        return self._run(*args)

    # ------------------------------------------------------------------
    # Smart Money (requires API key)
    # ------------------------------------------------------------------

    def get_smartmoney_traders(self, limit: int = 20) -> list:
        """List/filter traders from leaderboard."""
        return self._run(
            "smartmoney", "traders", "--limit", str(limit),
            check_api_key=True
        )

    def get_smartmoney_trader_detail(self, author_id: str) -> dict:
        """Full trader portrait: profile + positions + trades."""
        return self._run(
            "smartmoney", "trader", "--authorId", author_id,
            check_api_key=True
        )

    def get_smartmoney_overview(self, limit: int = 10) -> dict:
        """Multi-currency smart money overview. (Always uses current-hour data)"""
        return self._run(
            "smartmoney", "overview", "--lmtNum", str(limit),
            check_api_key=True
        )

    def get_smartmoney_signal(self, instId: str) -> dict:
        """Single-currency consensus via trader positions.
        NOTE: Uses traders filtered by coin as proxy for signal (signal endpoint has CLI bug).
        Returns dict with {coin, traders_count, top_traders, avg_win_rate}."""
        try:
            # Use traders with coin filter as signal proxy
            ccy = instId.split("-")[0]
            traders = self._run(
                "smartmoney", "traders", "--instCcy", ccy, "--limit", "5",
                check_api_key=True
            )
            if isinstance(traders, list) and len(traders) > 0:
                avg_wr = sum(float(t.get("winRate", 0) or 0) for t in traders) / len(traders)
                return {
                    "coin": ccy,
                    "trader_count": len(traders),
                    "top_traders": [
                        {"name": t.get("nickName"), "winRate": t.get("winRate"), "pnl": t.get("pnl")}
                        for t in traders[:3]
                    ],
                    "avg_win_rate": round(avg_wr, 4),
                    "bullish_score": min(1.0, avg_wr),
                    "source": "traders_proxy",
                }
            return {"coin": ccy, "trader_count": 0, "bullish_score": 0.0, "source": "traders_proxy"}
        except Exception:
            return {"coin": instId.split("-")[0], "trader_count": 0, "bullish_score": 0.0, "source": "error"}

    def get_smartmoney_signal_history(self, instId: str,
                                      ts: Optional[int] = None) -> list:
        """Signal history timeline for trend analysis."""
        args = ["smartmoney", "signal-history", "--instId", instId]
        if ts is None:
            ts = int(time.time() * 1000)
        args.extend(["--ts", str(ts)])
        return self._run(*args, check_api_key=True)

    # ------------------------------------------------------------------
    # News & Sentiment (requires API key)
    # ------------------------------------------------------------------

    def get_news_latest(self, limit: int = 10) -> list:
        """Latest news."""
        return self._run("news", "latest", "--limit", str(limit),
                         check_api_key=True)

    def get_news_by_coin(self, coins: list[str] = ("BTC",),
                         limit: int = 10) -> list:
        """Coin-specific news."""
        return self._run(
            "news", "by-coin", "--coins", ",".join(coins),
            "--limit", str(limit), check_api_key=True
        )

    def get_news_sentiment_rank(self, sort_by: str = "bullish",
                                limit: int = 20) -> list:
        """Sentiment ranking (hottest coins right now)."""
        return self._run(
            "news", "sentiment-rank", "--sort-by", sort_by,
            "--limit", str(limit), check_api_key=True
        )

    def get_news_coin_sentiment(self, coins: list[str] = ("BTC",)) -> list:
        """Coin sentiment snapshot."""
        return self._run(
            "news", "coin-sentiment", "--coins", ",".join(coins),
            check_api_key=True
        )

    def get_news_coin_trend(self, coin: str = "BTC",
                            period: str = "1h", points: int = 24) -> list:
        """Coin sentiment trend over time."""
        return self._run(
            "news", "coin-trend", coin,
            "--period", period, "--points", str(points),
            check_api_key=True
        )

    # ------------------------------------------------------------------
    # Trading (requires API key) — for the execution layer
    # ------------------------------------------------------------------

    def place_spot_order(self, instId: str, side: str, sz: str,
                         ord_type: str = "market",
                         tgt_ccy: Optional[str] = None) -> dict:
        """Place a spot order."""
        args = ["trade", "spot", "place", "--instId", instId,
                "--side", side, "--sz", sz, "--ordType", ord_type]
        if tgt_ccy:
            args.extend(["--tgtCcy", tgt_ccy])
        return self._run(*args, check_api_key=True)

    def place_swap_order(self, instId: str, side: str, sz: str,
                         ord_type: str = "market",
                         pos_side: Optional[str] = None,
                         tgt_ccy: Optional[str] = None,
                         lever: Optional[int] = None) -> dict:
        """Place a swap order (long/short)."""
        args = ["trade", "swap", "place", "--instId", instId,
                "--side", side, "--sz", sz, "--ordType", ord_type]
        if pos_side:
            args.extend(["--posSide", pos_side])
        if tgt_ccy:
            args.extend(["--tgtCcy", tgt_ccy])
        if lever:
            args.extend(["--lever", str(lever)])
        return self._run(*args, check_api_key=True)

    def set_tp_sl(self, instId: str, tp_trigger_px: Optional[str] = None,
                  sl_trigger_px: Optional[str] = None,
                  tp_ord_px: Optional[str] = None,
                  sl_ord_px: Optional[str] = None,
                  sz: Optional[str] = None,
                  pos_side: str = "long") -> dict:
        """Set take-profit and/or stop-loss for an existing position."""
        args = ["trade", "algo", "place", "--instId", instId,
                "--posSide", pos_side]
        if tp_trigger_px:
            args.extend(["--tpTriggerPx", tp_trigger_px])
            args.append("--tpOrdPx")
            args.append(tp_ord_px if tp_ord_px else tp_trigger_px)
        if sl_trigger_px:
            args.extend(["--slTriggerPx", sl_trigger_px])
            args.append("--slOrdPx")
            args.append(sl_ord_px if sl_ord_px else sl_trigger_px)
        if sz:
            args.extend(["--sz", sz])
        return self._run(*args, check_api_key=True)

    def close_position(self, instId: str, pos_side: str = "long",
                       mgn_mode: str = "cross",
                       auto_close: bool = True) -> dict:
        """Close a position (full close)."""
        args = ["trade", "swap", "close-position", "--instId", instId,
                "--posSide", pos_side, "--mgnMode", mgn_mode]
        if auto_close:
            args.append("--autoClose")
            args.append("true")
        args.append("--json")
        return self._run(*args, check_api_key=True)

    def get_positions(self, instType: str = "SWAP") -> list:
        """Get current positions."""
        return self._run("trade", "position", "--instType", instType,
                         check_api_key=True)

    def get_account_balance(self) -> dict:
        """Get account balance."""
        return self._run("trade", "account", "balance",
                         check_api_key=True)

    def set_leverage(self, instId: str, lever: int,
                     mgn_mode: str = "cross",
                     pos_side: Optional[str] = None) -> dict:
        """Set leverage for an instrument."""
        args = ["trade", "account", "set-leverage", "--instId", instId,
                "--lever", str(lever), "--mgnMode", mgn_mode]
        if pos_side:
            args.extend(["--posSide", pos_side])
        return self._run(*args, check_api_key=True)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def check_connection(self) -> bool:
        """Quick connectivity check using a public endpoint."""
        try:
            self.get_ticker("BTC-USDT")
            return True
        except OKXAPIError:
            return False

    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._cache.clear()

    def reset_key_check(self) -> None:
        """Force re-check of API key on next private call."""
        with self.__lock:
            self.__key_checked = False
