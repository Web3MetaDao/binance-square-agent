#!/usr/bin/env python3
"""
extras_fetcher.py — Multi-dimensional extra data fetcher for surge scanner.

Collects funding rates, OI growth, and long/short ratios from OKX, Gate.io,
and Bitget in parallel. Designed to be resilient: all API calls wrapped in
try/except, failures logged as warnings, never crash the calling pipeline.

Exports:
    ExtrasFetcher — class with fetch_all_extras(symbols) -> dict
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

logger = logging.getLogger("extras.fetcher")

REQUEST_TIMEOUT = 15
MAX_WORKERS = 10

BITGET_TICKER_ENDPOINT = "https://api.bitget.com/api/v2/mix/market/tickers"
GATE_TICKER_ENDPOINT = "https://api.gateio.ws/api/v4/futures/usdt/tickers"
OKX_FUNDING_ENDPOINT = "https://www.okx.com/api/v5/public/funding-rate"
OKX_LS_CONTRACT = "https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio-contract"
OKX_TRADES_ENDPOINT = "https://www.okx.com/api/v5/market/trades"


# ═══════════════════════════════════════════════════════════
# Symbol normalization helpers
# ═══════════════════════════════════════════════════════════

def _sym_to_okx_inst_id(sym: str) -> str:
    """BTCUSDT -> BTC-USDT-SWAP"""
    s = sym.upper()
    if s.endswith("USDT"):
        return f"{s[:-4]}-USDT-SWAP"
    return f"{s}-USDT-SWAP"


def _okx_inst_to_sym(inst_id: str) -> str:
    """BTC-USDT-SWAP -> BTCUSDT"""
    s = inst_id.upper()
    if s.endswith("-SWAP"):
        s = s[:-5]
    return s.replace("-", "")


def _sym_to_gate_contract(sym: str) -> str:
    """BTCUSDT -> BTC_USDT"""
    s = sym.upper()
    if s.endswith("USDT"):
        return f"{s[:-4]}_USDT"
    return f"{s}_USDT"


def _gate_contract_to_sym(contract: str) -> str:
    """BTC_USDT -> BTCUSDT"""
    c = contract.upper()
    if c.endswith("_USDT"):
        return c.replace("_USDT", "USDT")
    return c.replace("_", "")


# ═══════════════════════════════════════════════════════════
# ExtrasFetcher class
# ═══════════════════════════════════════════════════════════

class ExtrasFetcher:
    """
    Fetches multi-dimensional extra data for scoring.

    Data sources (all resilient with try/except):
      - OKX:   funding rate (public/funding-rate, per-symbol via ThreadPool)
      - OKX:   long/short ratio (rubik API, per-symbol via ThreadPool)
      - Gate:  funding rate (futures/usdt/tickers, all-in-one)
      - Bitget: funding rate (mix/market/tickers, all-in-one)

    Output format per symbol:
        {
            "funding_rate": float or None,
            "oi_growth_pct": float or None,
            "long_short_ratio": float or None,
        }
    """

    def __init__(self):
        self._max_workers = MAX_WORKERS

    # ── Public entrypoint ─────────────────────────────────

    def fetch_all_extras(self, symbols: list[str]) -> dict[str, dict]:
        """
        Parallel-fetch all extra dimensions from all sources.

        Args:
            symbols: list of standard symbol strings, e.g. ["BTCUSDT", "ETHUSDT"]

        Returns:
            dict keyed by symbol, each value being a dict with:
                funding_rate, oi_growth_pct, long_short_ratio
            Missing data points are None. Entire fetch never raises.
        """
        if not symbols:
            return {}

        de_dup = list(dict.fromkeys(symbols))

        result: dict[str, dict] = {}
        for sym in de_dup:
            result[sym] = {
                "funding_rate": None,
                "oi_growth_pct": None,
                "long_short_ratio": None,
            }

        data_sources: dict[str, dict[str, Optional[float]]] = {}

        with ThreadPoolExecutor(max_workers=self._max_workers) as exe:
            futures = {
                exe.submit(self._fetch_gate_funding): "gate_funding",
                exe.submit(self._fetch_bitget_funding): "bitget_funding",
                exe.submit(self._fetch_okx_funding, de_dup): "okx_funding",
                exe.submit(self._fetch_okx_ls_ratio, de_dup): "okx_ls_ratio",
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    data = future.result(timeout=REQUEST_TIMEOUT + 10)
                    if data:
                        data_sources[name] = data
                        logger.debug("Extras source '%s': %d items", name, len(data))
                except Exception as exc:
                    logger.warning("Extras source '%s' failed: %s", name, exc)

        # ── Merge ─────────────────────────────────────────
        for sym in result:
            fr = None
            for src in ("okx_funding", "gate_funding", "bitget_funding"):
                src_data = data_sources.get(src, {})
                if sym in src_data and src_data[sym] is not None:
                    fr = src_data[sym]
                    break
            if fr is not None:
                result[sym]["funding_rate"] = round(fr, 8)

            ls_data = data_sources.get("okx_ls_ratio", {})
            if sym in ls_data and ls_data[sym] is not None:
                result[sym]["long_short_ratio"] = round(ls_data[sym], 4)

        fr_ok = sum(1 for v in result.values() if v["funding_rate"] is not None)
        ls_ok = sum(1 for v in result.values() if v["long_short_ratio"] is not None)
        logger.info(
            "Extras: %d symbols | funding=%d ls_ratio=%d",
            len(result), fr_ok, ls_ok,
        )
        return result

    # ── OKX funding rate (per-symbol batch) ───────────────

    def _fetch_okx_funding(self, symbols: list[str]) -> dict[str, Optional[float]]:
        """
        Fetch funding rates from OKX. Per-symbol queries via ThreadPool.

        GET https://www.okx.com/api/v5/public/funding-rate?instId=BTC-USDT-SWAP

        NOTE: OKX does NOT support ?instType=SWAP for this endpoint.
        Each instrument must be queried individually.
        """
        result: dict[str, Optional[float]] = {}

        def _fetch_one(sym: str) -> tuple[str, Optional[float]]:
            inst_id = _sym_to_okx_inst_id(sym)
            try:
                resp = requests.get(
                    OKX_FUNDING_ENDPOINT,
                    params={"instId": inst_id},
                    timeout=REQUEST_TIMEOUT,
                )
                if resp.status_code != 200:
                    return sym, None
                data = resp.json()
                if data.get("code") != "0":
                    return sym, None
                items = data.get("data", [])
                if not items:
                    return sym, None
                fr = float(items[0].get("fundingRate", 0))
                return sym, fr
            except (requests.RequestException, ValueError, TypeError, IndexError):
                return sym, None

        # Process top volume symbols only to avoid rate limits
        # Typically 50–100 is enough for signal detection
        top_n = symbols[:80] if len(symbols) > 80 else symbols
        with ThreadPoolExecutor(max_workers=min(15, len(top_n) or 1)) as exe:
            futures = {exe.submit(_fetch_one, sym): sym for sym in top_n}
            for future in as_completed(futures):
                sym, fr = future.result()
                if fr is not None:
                    result[sym] = fr

        logger.debug("OKX funding rate: %d/%d symbols", len(result), len(top_n))
        return result

    # ── OKX long/short ratio (per-symbol v5 rubik) ────────

    def _fetch_okx_ls_ratio(self, symbols: list[str]) -> dict[str, Optional[float]]:
        """
        Fetch long/short account ratios from OKX v5 rubik API.

        GET https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio-contract
          ?instId=BTC-USDT-SWAP&period=5m

        Returns the latest ratio value (most recent data point).
        Limited to top 25 volume symbols to respect rate limits.
        """
        result: dict[str, Optional[float]] = {}

        def _fetch_one(sym: str) -> tuple[str, Optional[float]]:
            inst_id = _sym_to_okx_inst_id(sym)
            try:
                resp = requests.get(
                    OKX_LS_CONTRACT,
                    params={"instId": inst_id, "period": "5m"},
                    timeout=REQUEST_TIMEOUT,
                )
                if resp.status_code != 200:
                    return sym, None
                data = resp.json()
                if data.get("code") != "0":
                    return sym, None
                rows = data.get("data", [])
                if not rows or not rows[-1]:
                    return sym, None
                # rows[-1] = [timestamp, ratio]
                ratio = float(rows[-1][1])
                return sym, ratio
            except (requests.RequestException, ValueError, TypeError, IndexError):
                return sym, None

        top_n = symbols[:25] if len(symbols) > 25 else symbols
        with ThreadPoolExecutor(max_workers=min(10, len(top_n) or 1)) as exe:
            futures = {exe.submit(_fetch_one, sym): sym for sym in top_n}
            for future in as_completed(futures):
                sym, ratio = future.result()
                if ratio is not None:
                    result[sym] = ratio

        if result:
            logger.debug("OKX L/S ratio: %d/%d symbols", len(result), len(top_n))
        return result

    # ── Gate.io funding rate ──────────────────────────────

    def _fetch_gate_funding(self) -> dict[str, Optional[float]]:
        """
        Fetch funding rates from Gate.io tickers (all-in-one endpoint).
        GET https://api.gateio.ws/api/v4/futures/usdt/tickers
        """
        result: dict[str, Optional[float]] = {}
        try:
            resp = requests.get(GATE_TICKER_ENDPOINT, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                logger.warning("Gate funding rate HTTP %d", resp.status_code)
                return result

            raw = resp.json()
            for item in raw:
                contract = item.get("contract", "")
                if not contract.endswith("_USDT"):
                    continue
                try:
                    fr = float(item.get("funding_rate", 0))
                    sym = _gate_contract_to_sym(contract)
                    result[sym] = fr
                except (ValueError, TypeError):
                    continue

            logger.debug("Gate funding rate: %d symbols", len(result))
        except requests.RequestException as e:
            logger.warning("Gate funding rate request failed: %s", e)
        except Exception as e:
            logger.warning("Gate funding rate unexpected error: %s", e)
        return result

    # ── Bitget funding rate ───────────────────────────────

    def _fetch_bitget_funding(self) -> dict[str, Optional[float]]:
        """
        Fetch funding rates from Bitget (all-in-one endpoint).
        GET https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES

        Note: Bitget symbol format is simply "BTCUSDT" (no _UMCBL suffix).
        """
        result: dict[str, Optional[float]] = {}
        try:
            resp = requests.get(
                BITGET_TICKER_ENDPOINT,
                params={"productType": "USDT-FUTURES"},
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code != 200:
                logger.warning("Bitget funding rate HTTP %d", resp.status_code)
                return result
            data = resp.json()
            if data.get("code") != "00000":
                logger.warning("Bitget funding rate API error: code=%s", data.get("code"))
                return result

            for item in data.get("data", []):
                symbol = item.get("symbol", "")
                try:
                    fr = float(item.get("fundingRate", 0))
                    # Bitget symbol is already in standard format like "BTCUSDT"
                    result[symbol] = fr
                except (ValueError, TypeError):
                    continue

            logger.debug("Bitget funding rate: %d symbols", len(result))
        except requests.RequestException as e:
            logger.warning("Bitget funding rate request failed: %s", e)
        except Exception as e:
            logger.warning("Bitget funding rate unexpected error: %s", e)
        return result
