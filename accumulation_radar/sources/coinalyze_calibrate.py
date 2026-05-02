"""Coinalyze 行情校对模块。

用 Coinalyze（币安数据源）校对 OKX 的行情数据。
Coinalyze 价格与交易所有 ±0.2% 微小差异（正常），
但 OKX 的 24h 涨跌幅用独立 open24h 可能导致偏差，
用 Coinalyze daily candle 做修正。

用法:
    from .sources.coinalyze_calibrate import calibrate_prices
    calibrated = calibrate_prices(ticker_map)
"""

import time as _time
import threading

import requests

from ..config import logger

# ── Coinalyze API ────────────────────────────────
BASE = "https://api.coinalyze.net/v1"
from ..config import COINALYZE_API_KEY

# 限频
_lock = threading.Lock()
_last_req = 0.0
_MIN_INTERVAL = 1.0  # 40/min

# COIN-M（用 USD_PERP）的币种
COIN_M = {
    "BTC", "ETH", "SOL", "XRP", "ADA", "DOGE", "DOT", "AVAX",
    "LINK", "UNI", "BCH", "LTC", "ATOM", "FIL", "TRX",
    "EOS", "XLM", "ETC", "XTZ", "ALGO", "VET", "THETA",
}


def _cz_get(endpoint, params=None):
    """Coinalyze API 请求（带限速）。"""
    global _last_req
    p = {"api_key": COINALYZE_API_KEY}
    if params:
        p.update(params)
    for attempt in range(3):
        with _lock:
            now = _time.monotonic()
            wait = _MIN_INTERVAL - (now - _last_req)
            if wait > 0:
                _time.sleep(wait)
            _last_req = _time.monotonic()
        try:
            r = requests.get(f"{BASE}{endpoint}", params=p, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                retry = int(float(r.headers.get("Retry-After", 5)))
                _time.sleep(retry)
                continue
            logger.warning(f"[CZ] {endpoint} -> {r.status_code}")
            return None
        except Exception as e:
            if attempt < 2:
                _time.sleep(2)
                continue
            logger.warning(f"[CZ] {endpoint} 失败: {e}")
    return None


def _resolve_cz_symbol(coin: str) -> str:
    """内部 coin → Coinalyze 币安symbol。

    优先 COIN-M (USD_PERP)，没有则用 USDT-M (USDT_PERP)。
    交易所代码 .A = Binance
    """
    if coin.upper() in COIN_M:
        return f"{coin.upper()}USD_PERP.A"
    return f"{coin.upper()}USDT_PERP.A"


def _parse_cz_sym(cz_sym: str) -> str:
    """Coinalyze symbol → coin name (BTCUSD_PERP.A → BTC)"""
    base = cz_sym.split("USD")[0] if "USD" in cz_sym else cz_sym.split("_")[0]
    return base.upper()


def fetch_cz_ticker(symbols: list) -> dict:
    """批量获取 Coinalyze 币安数据源的最新价格+24h涨跌幅+成交量。

    使用 OHLCV daily 接口（最近2根 K 线算涨跌，更准确）。

    Args:
        symbols: Coinalyze symbol 列表

    Returns:
        {coin: {px, chg24h, vol}}
    """
    if not symbols:
        return {}
    result = {}
    to_ts = int(_time.time())
    from_ts = to_ts - 3 * 86400

    for batch in [symbols[i:i+10] for i in range(0, len(symbols), 10)]:
        raw = _cz_get("/ohlcv-history", {
            "symbols": ",".join(batch),
            "interval": "daily",
            "from": from_ts,
            "to": to_ts,
            "convert_to_usd": "true",
        })
        if raw and isinstance(raw, list):
            for item in raw:
                cz_sym = item.get("symbol", "")
                hist = item.get("history", [])
                coin = _parse_cz_sym(cz_sym)
                if len(hist) >= 3:
                    try:
                        px = float(hist[-1]["c"])
                        prev_close = float(hist[-2]["c"])
                        chg = ((px - prev_close) / prev_close * 100) if prev_close > 0 else 0
                        vol = float(hist[-1].get("v", 0))
                        result[coin] = {"px": px, "chg24h": round(chg, 2), "vol": vol}
                    except (KeyError, IndexError, ValueError, TypeError):
                        continue
        if len(batch) > 0:
            _time.sleep(0.3)
    return result


def calibrate_prices(ticker_map: dict, top_n: int = 50) -> tuple:
    """用 Coinalyze 校对 OKX 行情数据。

    Args:
        ticker_map: OKX 的行情 {sym: {px, chg24h, vol}}
        top_n: 校对前 N 个成交量最大的币

    Returns:
        (calibrated_map, deviation_report)
        calibrated_map: 与 ticker_map 同结构，chg24h 已被 Coinalyze 修正
        deviation_report: [{"coin", "okx_chg", "cz_chg", "diff"}, ...]
    """
    # 选 Top N 大币做校对
    top_syms = sorted(ticker_map.keys(), key=lambda s: ticker_map[s]["vol"], reverse=True)[:top_n]
    coins = [s.replace("USDT", "") for s in top_syms]
    cz_syms = [_resolve_cz_symbol(c) for c in coins]

    logger.info(f"[CZ] 校对 {len(cz_syms)} 个币...")
    cz_data = fetch_cz_ticker(cz_syms)
    logger.info(f"[CZ] 返回 {len(cz_data)} 个")

    if not cz_data:
        return ticker_map, []

    deviation_report = []
    calibrated = dict(ticker_map)  # 拷贝

    for coin, cz in cz_data.items():
        sym = f"{coin}USDT"
        if sym not in ticker_map:
            continue
        okx = ticker_map[sym]

        # 价格校对：如果偏差 > 0.5%，记录但不修正价格（OKX价格本身可靠）
        px_diff = abs((cz["px"] - okx["px"]) / okx["px"] * 100) if okx["px"] else 0

        # 24h涨跌幅校对：用 Coinalyze 的值修正 OKX
        chg_diff = cz["chg24h"] - okx["chg24h"]
        if abs(chg_diff) > 0.5:  # 偏差超过 0.5% 才记录&修正
            deviation_report.append({
                "coin": coin,
                "okx_chg": okx["chg24h"],
                "cz_chg": cz["chg24h"],
                "diff": round(chg_diff, 2),
                "px_diff": round(px_diff, 3),
            })
            # 修正：用 Coinalyze 的24h涨跌幅替换
            calibrated[sym] = dict(okx)
            calibrated[sym]["chg24h"] = cz["chg24h"]

    if deviation_report:
        logger.info(f"[CZ] ✅ 修正 {len(deviation_report)} 个币的24h涨跌幅")
    else:
        logger.info("[CZ] ✅ 无需修正")

    return calibrated, deviation_report
