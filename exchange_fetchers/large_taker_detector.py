#!/usr/bin/env python3
"""
large_taker_detector.py — Detects large taker (whale) transactions on OKX futures.

Monitors recent trades for large single-print executions that indicate
whale activity. Uses OKX market trades API exclusively.

Resilient design: all API calls wrapped in try/except, never crashes the caller.

Exports:
    LargeTakerDetector — class with detect_large_trades(symbols) and detect_bulk_trades(symbols)
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

logger = logging.getLogger("large_taker")

REQUEST_TIMEOUT = 15

# ── Thresholds in USDT ────────────────────────────────────
THRESHOLD_BTC_ETH = 500_000   # 50万 USDT for BTC/ETH
THRESHOLD_OTHER = 100_000     # 10万 USDT for other symbols

BIG_TICKERS = {"BTCUSDT", "ETHUSDT"}


def _symbol_to_okx_inst_id(sym: str) -> str:
    """BTCUSDT -> BTC-USDT-SWAP"""
    s = sym.upper()
    if s.endswith("USDT"):
        return f"{s[:-4]}-USDT-SWAP"
    return f"{s}-USDT-SWAP"


class LargeTakerDetector:
    """
    Detects large taker (whale) transactions from OKX market trades.

    Methods:
        detect_large_trades(symbols) -> dict[str, bool]
            Quick check: does this symbol have any whale-sized taker trade?

        detect_bulk_trades(symbols) -> dict[str, dict]
            Detailed check: buy/sell breakdown with total USD values.

        fetch_okx_trades(symbol) -> list[dict]
            Raw trades from OKX API for one symbol.

        fetch_all(symbols) -> dict[str, dict]
            Parallel fetch for all symbols using ThreadPoolExecutor.
    """

    def detect_large_trades(self, symbols: list[str]) -> dict[str, bool]:
        """
        Check for any whale-sized taker trade in the last ~50 trades per symbol.

        Returns:
            {symbol: True if any taker trade >= threshold, else False}
        """
        if not symbols:
            return {}

        bulk = self.detect_bulk_trades(symbols)
        result: dict[str, bool] = {}
        for sym in symbols:
            data = bulk.get(sym, {})
            result[sym] = data.get("large_buy", False) or data.get("large_sell", False)
        return result

    def detect_bulk_trades(self, symbols: list[str]) -> dict[str, dict]:
        """
        Detailed detection returning per-symbol stats.

        Returns:
            {symbol: {
                "large_buy": bool,
                "large_sell": bool,
                "total_taker_buy_usd": float,
                "total_taker_sell_usd": float,
                "buy_sell_ratio": float or None,
            }}
        """
        if not symbols:
            return {}

        all_data = self.fetch_all(symbols)
        result: dict[str, dict] = {}

        for sym in symbols:
            raw = all_data.get(sym, [])
            default = {
                "large_buy": False,
                "large_sell": False,
                "total_taker_buy_usd": 0.0,
                "total_taker_sell_usd": 0.0,
                "buy_sell_ratio": None,
            }

            if not raw:
                result[sym] = default
                continue

            threshold = THRESHOLD_BTC_ETH if sym.upper() in BIG_TICKERS else THRESHOLD_OTHER
            total_buy = 0.0
            total_sell = 0.0
            large_buy = False
            large_sell = False

            for trade in raw:
                try:
                    side = trade.get("side", "").lower()
                    sz = float(trade.get("sz", 0))
                    px = float(trade.get("px", 0))
                    trade_usd = sz * px
                except (ValueError, TypeError):
                    continue

                if side == "buy":
                    total_buy += trade_usd
                    if trade_usd >= threshold:
                        large_buy = True
                elif side == "sell":
                    total_sell += trade_usd
                    if trade_usd >= threshold:
                        large_sell = True

            buy_sell_ratio = None
            if total_sell > 0:
                buy_sell_ratio = round(total_buy / total_sell, 4)
            elif total_buy > 0:
                buy_sell_ratio = 999.0  # effectively infinite buy pressure

            result[sym] = {
                "large_buy": large_buy,
                "large_sell": large_sell,
                "total_taker_buy_usd": round(total_buy, 2),
                "total_taker_sell_usd": round(total_sell, 2),
                "buy_sell_ratio": buy_sell_ratio,
            }

        return result

    def fetch_okx_trades(self, symbol: str) -> list[dict]:
        """
        Fetch the most recent 50 trades from OKX for a given symbol.

        GET https://www.okx.com/api/v5/market/trades?instId={instId}&limit=50

        Args:
            symbol: e.g. "BTCUSDT"

        Returns:
            list of trade dicts from OKX API, or empty list on failure.
        """
        inst_id = _symbol_to_okx_inst_id(symbol)
        try:
            resp = requests.get(
                "https://www.okx.com/api/v5/market/trades",
                params={"instId": inst_id, "limit": 50},
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code != 200:
                logger.debug("OKX trades HTTP %d for %s", resp.status_code, symbol)
                return []
            data = resp.json()
            if data.get("code") != "0":
                logger.debug("OKX trades API error for %s: code=%s", symbol, data.get("code"))
                return []
            return data.get("data", [])
        except requests.RequestException as e:
            logger.debug("OKX trades request failed %s: %s", symbol, e)
        except Exception as e:
            logger.debug("OKX trades unexpected error %s: %s", symbol, e)
        return []

    def fetch_all(self, symbols: list[str]) -> dict[str, list[dict]]:
        """
        Parallel-fetch trades for all symbols using ThreadPoolExecutor.

        Args:
            symbols: list of symbol strings e.g. ["BTCUSDT", "ETHUSDT"]

        Returns:
            {symbol: [list of trade dicts]} — each list is the raw API response data.
        """
        if not symbols:
            return {}

        result: dict[str, list[dict]] = {s: [] for s in symbols}

        with ThreadPoolExecutor(max_workers=8) as exe:
            future_to_sym = {exe.submit(self.fetch_okx_trades, sym): sym for sym in symbols}
            for future in as_completed(future_to_sym):
                sym = future_to_sym[future]
                try:
                    trades = future.result(timeout=REQUEST_TIMEOUT + 5)
                    if trades:
                        result[sym] = trades
                except Exception as exc:
                    logger.debug("fetch_all trades failed %s: %s", sym, exc)

        ok_count = sum(1 for v in result.values() if v)
        logger.debug("LargeTaker: fetched trades for %d/%d symbols", ok_count, len(symbols))
        return result
