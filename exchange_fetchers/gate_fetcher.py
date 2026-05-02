""""
gate_fetcher.py — Gate.io USDT futures ticker & kline data fetching module.

Provides:
  fetch_gate_tickers(min_vol=1_000_000) -> dict[str, dict]
  fetch_gate_kline(contract, timeframe) -> dict | None
  batch_fetch_gate_klines(symbols, timeframe, max_workers=15) -> dict
  gate_symbol_to_contract(sym) -> str          # "BTCUSDT" -> "BTC_USDT"
  gate_contract_to_symbol(contract) -> str     # "BTC_USDT" -> "BTCUSDT"
"""

import logging
import time
import requests
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("gate.fetcher")

GATE_API_BASE = "https://api.gateio.ws"
TICKER_ENDPOINT = f"{GATE_API_BASE}/api/v4/futures/usdt/tickers"
KLINE_ENDPOINT = f"{GATE_API_BASE}/api/v4/futures/usdt/candlesticks"
REQUEST_TIMEOUT = 30
RETRY_MAX = 2
RETRY_BASE_DELAY = 1.0  # seconds

VALID_TIMEFRAMES = {"1h", "2h", "4h"}


# ── Symbol helpers ─────────────────────────────────────────

def gate_symbol_to_contract(sym: str) -> str:
    """Convert standard symbol to Gate.io contract name.

    "BTCUSDT" -> "BTC_USDT"
    "1000PEPEUSDT" -> "1000PEPE_USDT"
    """
    s = sym.upper()
    if s.endswith("USDT"):
        base = s[:-4]
        return f"{base}_USDT"
    idx = s.find("USDT")
    if idx != -1:
        base = s[:idx]
        return f"{base}_USDT"
    return f"{s}_USDT"


def gate_contract_to_symbol(contract: str) -> str:
    """Convert Gate.io contract name to standard symbol.

    "BTC_USDT" -> "BTCUSDT"
    "1000PEPE_USDT" -> "1000PEPEUSDT"
    """
    return contract.upper().replace("_", "")


# ── Ticker fetch ──────────────────────────────────────────

def fetch_gate_tickers(min_vol: float = 1_000_000) -> dict[str, dict]:
    """Fetch all Gate.io USDT futures tickers with volume >= min_vol.

    Returns dict keyed by symbol (e.g. "BTCUSDT") with:
      {sym, contract, price, chg24h, vol_usd}
    """
    try:
        resp = requests.get(
            TICKER_ENDPOINT,
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning("fetch_gate_tickers HTTP %d", resp.status_code)
            return {}
        data = resp.json()
    except requests.RequestException as e:
        logger.error("fetch_gate_tickers request failed: %s", e)
        return {}

    if not isinstance(data, list):
        logger.warning("fetch_gate_tickers: unexpected response type %s", type(data))
        return {}

    result = {}
    for t in data:
        contract = t.get("contract", "")
        if not isinstance(contract, str) or not contract.endswith("_USDT"):
            continue
        try:
            last = float(t.get("last", 0))
            if last <= 0:
                continue

            change_pct = t.get("change_percentage")
            # Gate change_percentage can be a string like "+0.12" or float
            chg24h = float(change_pct) if change_pct is not None else 0.0

            vol_usd_str = t.get("volume_24h_quote", "0")
            vol_usd = int(float(vol_usd_str))

            if vol_usd < min_vol:
                continue

            sym = gate_contract_to_symbol(contract)

            result[sym] = {
                "sym": sym,
                "contract": contract,
                "price": last,
                "chg24h": chg24h,
                "vol_usd": vol_usd,
            }
        except (ValueError, TypeError, KeyError) as e:
            logger.debug("skip ticker %s: %s", contract, e)
            continue

    logger.info(
        "fetch_gate_tickers: %d symbols (vol>=%.0fM USD)", len(result), min_vol / 1_000_000
    )
    return result


# ── Kline fetch (single) ──────────────────────────────────

def fetch_gate_kline(contract: str, timeframe: str) -> dict | None:
    """Fetch kline/candlesticks for one Gate.io USDT futures contract.

    Args:
        contract: Gate contract name (e.g. "BTC_USDT")
        timeframe: "1h", "2h", or "4h"

    Returns:
        {open, high, low, close, volume, times} with numpy float64 arrays,
        or None on failure.

    Gate kline response: array of {o, h, l, c, v (USDT volume), t (Unix sec), sum}
    We reverse the response so oldest candles come first.
    """
    if timeframe not in VALID_TIMEFRAMES:
        logger.error("fetch_gate_kline: invalid timeframe '%s'", timeframe)
        return None

    last_exc = None
    for attempt in range(RETRY_MAX):
        try:
            resp = requests.get(
                KLINE_ENDPOINT,
                params={
                    "contract": contract,
                    "interval": timeframe,
                    "limit": 130,
                },
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 429:
                wait = RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(
                    "fetch_gate_kline rate-limited (429) for %s, retry %d/%d in %.1fs",
                    contract, attempt + 1, RETRY_MAX, wait,
                )
                time.sleep(wait)
                last_exc = None
                continue
            if resp.status_code != 200:
                logger.warning("fetch_gate_kline HTTP %d for %s", resp.status_code, contract)
                return None
            data = resp.json()
            last_exc = None
            break
        except requests.RequestException as e:
            last_exc = e
            wait = RETRY_BASE_DELAY * (2 ** attempt)
            logger.warning(
                "fetch_gate_kline request failed for %s (attempt %d/%d): %s, retry in %.1fs",
                contract, attempt + 1, RETRY_MAX, e, wait,
            )
            time.sleep(wait)

    if last_exc is not None:
        logger.error("fetch_gate_kline failed for %s after %d retries: %s", contract, RETRY_MAX, last_exc)
        return None

    if not isinstance(data, list) or len(data) == 0:
        logger.warning("fetch_gate_kline: no candles for %s", contract)
        return None

    # Gate returns newest first; reverse to get chronological order (oldest first)
    candles = list(reversed(data))

    try:
        opens = np.array([float(c["o"]) for c in candles], dtype=np.float64)
        highs = np.array([float(c["h"]) for c in candles], dtype=np.float64)
        lows = np.array([float(c["l"]) for c in candles], dtype=np.float64)
        closes = np.array([float(c["c"]) for c in candles], dtype=np.float64)
        # Gate "v" field is USDT quote volume
        volumes = np.array([float(c["v"]) for c in candles], dtype=np.float64)
        # "t" is already Unix seconds (int)
        times = [int(c["t"]) for c in candles]

        return {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
            "times": times,
        }
    except (ValueError, IndexError, TypeError, KeyError) as e:
        logger.error("fetch_gate_kline parse error for %s: %s", contract, e)
        return None


# ── Batch kline fetch ─────────────────────────────────────

def batch_fetch_gate_klines(
    symbols: list[dict], timeframe: str, max_workers: int = 15
) -> dict:
    """Batch fetch klines for multiple Gate contracts concurrently.

    Args:
        symbols: list of {"sym": str, "contract": str}
        timeframe: "1h", "2h", or "4h"
        max_workers: ThreadPool size (default 15)

    Returns:
        {sym_str: kline_dict} for successful fetches only.
    """
    if not symbols:
        return {}

    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(fetch_gate_kline, s["contract"], timeframe): s["sym"]
            for s in symbols
        }
        for future in as_completed(future_map):
            sym = future_map[future]
            try:
                kline = future.result()
                if kline is not None:
                    results[sym] = kline
            except Exception as e:
                logger.error("batch fetch gate kline exception for %s: %s", sym, e)

    logger.info(
        "batch_fetch_gate_klines(%s): %d/%d succeeded",
        timeframe, len(results), len(symbols),
    )
    return results
