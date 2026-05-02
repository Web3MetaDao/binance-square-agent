"""
bitget_fetcher.py — Bitget USDT-FUTURES ticker & kline data fetching module.

Provides:
  fetch_bitget_tickers(min_vol=1_000_000) -> dict[str, dict]
  fetch_bitget_kline(symbol, timeframe) -> dict | None
  batch_fetch_bitget_klines(symbols, timeframe, max_workers=15) -> dict

Bitget uses direct symbol format ("BTCUSDT"), no conversion needed.
Kline granularity: "1H", "2H", "4H" (uppercase).
"""

import logging
import time
import requests
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("bitget.fetcher")

BITGET_API_BASE = "https://api.bitget.com"
TICKER_ENDPOINT = f"{BITGET_API_BASE}/api/v2/mix/market/tickers"
KLINE_ENDPOINT = f"{BITGET_API_BASE}/api/v2/mix/market/candles"
REQUEST_TIMEOUT = 30
RETRY_MAX = 2
RETRY_BASE_DELAY = 1.0  # seconds

VALID_TIMEFRAMES = {"1H", "2H", "4H"}


# ── Ticker fetch ───────────────────────────────────────

def fetch_bitget_tickers(min_vol: float = 1_000_000) -> dict[str, dict]:
    """Fetch all Bitget USDT-FUTURES tickers with volume >= min_vol.

    Returns dict keyed by symbol (e.g. "BTCUSDT") with:
      {sym, price, chg24h, vol_usd, sod_utc0_chg}
    """
    try:
        resp = requests.get(
            TICKER_ENDPOINT,
            params={"productType": "USDT-FUTURES"},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning("fetch_bitget_tickers HTTP %d", resp.status_code)
            return {}
        data = resp.json()
        if data.get("code") != "00000":
            logger.warning("fetch_bitget_tickers API error: code=%s msg=%s",
                           data.get("code"), data.get("msg", ""))
            return {}
    except requests.RequestException as e:
        logger.error("fetch_bitget_tickers request failed: %s", e)
        return {}

    result = {}
    for t in data.get("data", []):
        symbol = t.get("symbol", "")
        if not symbol:
            continue
        try:
            last = float(t.get("lastPr", 0))
            if last <= 0:
                continue

            # change24h is a decimal ratio (e.g. 0.00438 = +0.438%)
            change24h_raw = t.get("change24h")
            chg24h = round(float(change24h_raw) * 100, 2) if change24h_raw is not None else 0.0

            vol_usd = int(float(t.get("usdtVolume", 0)))

            if vol_usd < min_vol:
                continue

            # SOD (start of day UTC+0) change
            # Bitget provides "openUtc" — the price at UTC 00:00
            sod_utc0_str = t.get("openUtc")
            if sod_utc0_str:
                sod_utc0 = float(sod_utc0_str)
                sod_utc0_chg = round((last - sod_utc0) / sod_utc0 * 100, 2) if sod_utc0 else 0.0
            else:
                sod_utc0_chg = 0.0

            result[symbol] = {
                "sym": symbol,
                "price": last,
                "chg24h": chg24h,
                "vol_usd": vol_usd,
                "sod_utc0_chg": sod_utc0_chg,
            }
        except (ValueError, TypeError, KeyError) as e:
            logger.debug("skip ticker %s: %s", symbol, e)
            continue

    logger.info("fetch_bitget_tickers: %d symbols (vol>=%.0fM USD)", len(result), min_vol / 1_000_000)
    return result


# ── Kline fetch (single) ───────────────────────────────

def fetch_bitget_kline(symbol: str, timeframe: str) -> dict | None:
    """Fetch kline/candles for one Bitget symbol.

    Args:
        symbol: Symbol (e.g. "BTCUSDT")
        timeframe: "1H", "2H", or "4H"

    Returns:
        {open, high, low, close, volume, times} with numpy arrays,
        or None on failure.

    Bitget kline response: [[ts_ms, open, high, low, close, vol_coin, vol_usdt], ...]
    We use vol_usdt (index 6) for dollar-denominated volume.
    Returns newest first; we reverse to get chronological order.
    """
    if timeframe not in VALID_TIMEFRAMES:
        logger.error("fetch_bitget_kline: invalid timeframe '%s'", timeframe)
        return None

    last_exc = None
    for attempt in range(RETRY_MAX):
        try:
            resp = requests.get(
                KLINE_ENDPOINT,
                params={
                    "symbol": symbol,
                    "productType": "USDT-FUTURES",
                    "granularity": timeframe,
                    "limit": 130,
                },
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 429:
                wait = RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(
                    "fetch_bitget_kline rate-limited (429) for %s, retry %d/%d in %.1fs",
                    symbol, attempt + 1, RETRY_MAX, wait,
                )
                time.sleep(wait)
                last_exc = None
                continue
            if resp.status_code != 200:
                logger.warning("fetch_bitget_kline HTTP %d for %s", resp.status_code, symbol)
                return None
            data = resp.json()
            if data.get("code") != "00000":
                logger.warning("fetch_bitget_kline API error for %s: code=%s msg=%s",
                               symbol, data.get("code"), data.get("msg", ""))
                return None
            last_exc = None
            break
        except requests.RequestException as e:
            last_exc = e
            wait = RETRY_BASE_DELAY * (2 ** attempt)
            logger.warning(
                "fetch_bitget_kline request failed for %s (attempt %d/%d): %s, retry in %.1fs",
                symbol, attempt + 1, RETRY_MAX, e, wait,
            )
            time.sleep(wait)

    if last_exc is not None:
        logger.error("fetch_bitget_kline failed for %s after %d retries: %s", symbol, RETRY_MAX, last_exc)
        return None

    candles = data.get("data", [])
    if not candles:
        logger.warning("fetch_bitget_kline: no candles for %s", symbol)
        return None

    # Bitget returns newest first; reverse to get oldest first
    candles = list(reversed(candles))

    try:
        # Response: [ts_ms, open, high, low, close, vol_coin, vol_usdt]
        # We use: ts(c[0]), o(c[1]), h(c[2]), l(c[3]), c(c[4]), volUsd(c[6])
        opens = np.array([float(c[1]) for c in candles], dtype=np.float64)
        highs = np.array([float(c[2]) for c in candles], dtype=np.float64)
        lows = np.array([float(c[3]) for c in candles], dtype=np.float64)
        closes = np.array([float(c[4]) for c in candles], dtype=np.float64)
        # Use USD volume (index 6) for dollar-denominated volume
        volumes = np.array([float(c[6]) for c in candles], dtype=np.float64)
        # Convert ms timestamps to seconds (int)
        times = [int(c[0]) // 1000 for c in candles]

        return {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
            "times": times,
        }
    except (ValueError, IndexError, TypeError) as e:
        logger.error("fetch_bitget_kline parse error for %s: %s", symbol, e)
        return None


# ── Batch kline fetch ──────────────────────────────────

def batch_fetch_bitget_klines(
    symbols: list[dict], timeframe: str, max_workers: int = 15
) -> dict:
    """Batch fetch klines for multiple Bitget symbols concurrently.

    Args:
        symbols: list of {"sym": str} — Bitget uses symbol directly, no conversion.
        timeframe: "1H", "2H", or "4H"
        max_workers: ThreadPool size (default 15)

    Returns:
        {sym_str: kline_dict} for successful fetches only.
    """
    if not symbols:
        return {}

    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(fetch_bitget_kline, s["sym"], timeframe): s["sym"]
            for s in symbols
        }
        for future in as_completed(future_map):
            sym = future_map[future]
            try:
                kline = future.result()
                if kline is not None:
                    results[sym] = kline
            except Exception as e:
                logger.error("batch fetch bitget kline exception for %s: %s", sym, e)

    logger.info(
        "batch_fetch_bitget_klines(%s): %d/%d succeeded",
        timeframe, len(results), len(symbols),
    )
    return results
