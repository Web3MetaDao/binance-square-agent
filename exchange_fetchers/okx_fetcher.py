"""
okx_fetcher.py — OKX USDT-SWAP ticker & kline data fetching module.

Provides:
  fetch_okx_tickers(min_vol=1_000_000) -> dict[str, dict]
  fetch_okx_kline(sym, inst_id, timeframe) -> dict | None
  batch_fetch_okx_klines(symbols, timeframe, max_workers=15) -> dict
  okx_symbol_to_raw(sym) -> str          # "BTCUSDT" -> "BTC-USDT-SWAP"
  okx_raw_to_symbol(inst_id) -> str      # "BTC-USDT-SWAP" -> "BTCUSDT"
"""

import logging
import time
import requests
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("okx.fetcher")

OKX_API_BASE = "https://www.okx.com"
TICKER_ENDPOINT = f"{OKX_API_BASE}/api/v5/market/tickers?instType=SWAP"
KLINE_ENDPOINT = f"{OKX_API_BASE}/api/v5/market/candles"
REQUEST_TIMEOUT = 30
RETRY_MAX = 3
RETRY_BASE_DELAY = 1.5  # seconds


# ── Symbol helpers ─────────────────────────────────────

def okx_symbol_to_raw(sym: str) -> str:
    """Convert standard symbol to OKX instrument ID.
    
    "BTCUSDT" -> "BTC-USDT-SWAP"
    "1000PEPEUSDT" -> "1000PEPE-USDT-SWAP"
    """
    s = sym.upper()
    # Find the position of "USDT" — it could be "USDT" at the end
    # or something like "1000PEPEUSDT" where "USDT" is the suffix
    if s.endswith("USDT"):
        base = s[:-4]
        return f"{base}-USDT-SWAP"
    # Fallback: try to split on first occurrence of USDT
    idx = s.find("USDT")
    if idx != -1:
        base = s[:idx]
        return f"{base}-USDT-SWAP"
    return f"{s}-USDT-SWAP"


def okx_raw_to_symbol(inst_id: str) -> str:
    """Convert OKX instrument ID to standard symbol.
    
    "BTC-USDT-SWAP" -> "BTCUSDT"
    "1000PEPE-USDT-SWAP" -> "1000PEPEUSDT"
    """
    # Strip "-SWAP" suffix, then remove all hyphens
    s = inst_id.upper()
    if s.endswith("-SWAP"):
        s = s[:-5]
    return s.replace("-", "")


# ── Ticker fetch ───────────────────────────────────────

def fetch_okx_tickers(min_vol: float = 1_000_000) -> dict[str, dict]:
    """Fetch all OKX USDT-SWAP tickers with volume >= min_vol.
    
    Returns dict keyed by symbol (e.g. "BTCUSDT") with:
      {sym, inst_id, price, chg24h, vol_usd, sod_utc0_chg}
    """
    try:
        resp = requests.get(
            TICKER_ENDPOINT,
            params={"instType": "SWAP"},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning("fetch_okx_tickers HTTP %d", resp.status_code)
            return {}
        data = resp.json()
        if data.get("code") != "0":
            logger.warning("fetch_okx_tickers API error: code=%s msg=%s",
                           data.get("code"), data.get("msg", ""))
            return {}
    except requests.RequestException as e:
        logger.error("fetch_okx_tickers request failed: %s", e)
        return {}

    result = {}
    for t in data.get("data", []):
        inst_id = t.get("instId", "")
        if not inst_id.endswith("USDT-SWAP"):
            continue
        try:
            last = float(t.get("last", 0))
            if last <= 0:
                continue
            # 24h open price
            open24 = float(t.get("open24h", 0))
            chg24h = round((last - open24) / open24 * 100, 2) if open24 else 0.0

            # Volume in USD (volCcy24h * last price)
            vol_ccy = float(t.get("volCcy24h", 0))
            vol_usd = int(vol_ccy * last)

            if vol_usd < min_vol:
                continue

            sym = okx_raw_to_symbol(inst_id)

            # SOD (start of day UTC+0) change
            # OKX provides "sodUtc0" — the price at UTC 00:00
            sod_utc0 = float(t.get("sodUtc0", 0))
            sod_utc0_chg = round((last - sod_utc0) / sod_utc0 * 100, 2) if sod_utc0 else 0.0

            result[sym] = {
                "sym": sym,
                "inst_id": inst_id,
                "price": last,
                "chg24h": chg24h,
                "vol_usd": vol_usd,
                "sod_utc0_chg": sod_utc0_chg,
            }
        except (ValueError, TypeError, KeyError) as e:
            logger.debug("skip ticker %s: %s", inst_id, e)
            continue

    logger.info("fetch_okx_tickers: %d symbols (vol>=%.0fM USD)", len(result), min_vol / 1_000_000)
    return result


# ── Kline fetch (single) ───────────────────────────────

def fetch_okx_kline(sym: str, inst_id: str, timeframe: str) -> dict | None:
    """Fetch kline/candles for one instrument.
    
    Args:
        sym: Standard symbol (e.g. "BTCUSDT")
        inst_id: OKX instrument ID (e.g. "BTC-USDT-SWAP")
        timeframe: "1H", "2H", or "4H"
    
    Returns:
        {open, high, low, close, volume, times} with numpy arrays,
        or None on failure.
    """
    if timeframe not in ("1H", "2H", "4H"):
        logger.error("fetch_okx_kline: invalid timeframe '%s'", timeframe)
        return None

    last_exc = None
    for attempt in range(RETRY_MAX):
        try:
            resp = requests.get(
                KLINE_ENDPOINT,
                params={
                    "instId": inst_id,
                    "bar": timeframe,
                    "limit": 200,  # KLINE_LIMIT (need ~130+ for MA88 + lookahead)
                },
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 429:
                wait = RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(
                    "fetch_okx_kline rate-limited (429) for %s, retry %d/%d in %.1fs",
                    sym, attempt + 1, RETRY_MAX, wait,
                )
                time.sleep(wait)
                last_exc = None
                continue
            if resp.status_code != 200:
                logger.warning("fetch_okx_kline HTTP %d for %s", resp.status_code, sym)
                return None
            data = resp.json()
            if data.get("code") != "0":
                logger.warning("fetch_okx_kline API error for %s: code=%s msg=%s",
                               sym, data.get("code"), data.get("msg", ""))
                return None
            last_exc = None
            break
        except requests.RequestException as e:
            last_exc = e
            wait = RETRY_BASE_DELAY * (2 ** attempt)
            logger.warning(
                "fetch_okx_kline request failed for %s (attempt %d/%d): %s, retry in %.1fs",
                sym, attempt + 1, RETRY_MAX, e, wait,
            )
            time.sleep(wait)

    if last_exc is not None:
        logger.error("fetch_okx_kline failed for %s after %d retries: %s", sym, RETRY_MAX, last_exc)
        return None

    candles = data.get("data", [])
    if not candles:
        logger.warning("fetch_okx_kline: no candles for %s", sym)
        return None

    # OKX returns newest first; reverse to get oldest first
    candles = list(reversed(candles))

    try:
        # Response: [ts, o, h, l, c, vol, volCcy, volUsd, confirm]
        # We use: ts(c[0]), o(c[1]), h(c[2]), l(c[3]), c(c[4]), volUsd(c[7])
        opens = np.array([float(c[1]) for c in candles], dtype=np.float64)
        highs = np.array([float(c[2]) for c in candles], dtype=np.float64)
        lows = np.array([float(c[3]) for c in candles], dtype=np.float64)
        closes = np.array([float(c[4]) for c in candles], dtype=np.float64)
        # Use USD volume (index 7) for dollar-denominated volume
        volumes = np.array([float(c[7]) for c in candles], dtype=np.float64)
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
        logger.error("fetch_okx_kline parse error for %s: %s", sym, e)
        return None


# ── Batch kline fetch ──────────────────────────────────

def batch_fetch_okx_klines(
    symbols: list[dict], timeframe: str, max_workers: int = 15
) -> dict:
    """Batch fetch klines for multiple symbols concurrently.
    
    Args:
        symbols: list of {"sym": str, "inst_id": str}
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
            executor.submit(fetch_okx_kline, s["sym"], s["inst_id"], timeframe): s["sym"]
            for s in symbols
        }
        for future in as_completed(future_map):
            sym = future_map[future]
            try:
                kline = future.result()
                if kline is not None:
                    results[sym] = kline
            except Exception as e:
                logger.error("batch fetch kline exception for %s: %s", sym, e)

    logger.info(
        "batch_fetch_okx_klines(%s): %d/%d succeeded",
        timeframe, len(results), len(symbols),
    )
    return results
