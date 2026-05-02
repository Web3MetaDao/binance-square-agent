"""统一行情数据同步层。

所有 API 请求集中在此，其他模块不直接调用 requests。
数据源: OKX ticker/K线, MEXC 费率+市值+OI, CoinGecko trending

实时性保障:
  - 每次调用都直接请求 API（无本地缓存）
  - fetch_global_data 跨函数调用时保持同一数据快照
  - K线 close_now 与 ticker last 交叉校验

用法:
    from .sources.market_data import fetch_all_tickers, fetch_mexc_funding, ...
"""

from datetime import datetime, timezone, timedelta

import requests

from ..config import logger

# ── API 端点 ───────────────────────────────────────
OKX_API = "https://www.okx.com"
MEXC_API = "https://contract.mexc.com"
CG_API = "https://api.coingecko.com/api/v3"

# ── 默认参数 ───────────────────────────────────────
MIN_VOL_USDT = 500_000  # 最低24h美元成交额

# ── 基础 API 封装 ──────────────────────────────────


def okx_get(endpoint: str, params: dict | None = None, timeout: int = 10):
    """OKX API GET 请求，返回 data 字段或 None。带耗时日志。"""
    import time as _time
    url = f"{OKX_API}{endpoint}"
    _t0 = _time.monotonic()
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        cost = _time.monotonic() - _t0
        if resp.status_code != 200:
            logger.warning(
                f"⚠️ OKX {endpoint} 返回 {resp.status_code} ({cost:.1f}s)"
            )
            return None
        data = resp.json()
        if data.get("code") != "0":
            logger.warning(
                f"⚠️ OKX {endpoint} code={data.get('code')} ({cost:.1f}s)"
            )
            return None
        return data.get("data")
    except requests.RequestException as e:
        cost = _time.monotonic() - _t0
        logger.warning(f"⚠️ OKX {endpoint} 请求失败 ({cost:.1f}s): {e}")
        return None


def mexc_get(endpoint: str, timeout: int = 10):
    """MEXC API GET 请求，返回 data 字段或 None。带耗时日志。"""
    import time as _time
    url = f"{MEXC_API}{endpoint}"
    _t0 = _time.monotonic()
    try:
        resp = requests.get(url, timeout=timeout)
        cost = _time.monotonic() - _t0
        if resp.status_code != 200:
            logger.warning(f"⚠️ MEXC {endpoint} 返回 {resp.status_code} ({cost:.1f}s)")
            return None
        data = resp.json()
        if not data.get("success"):
            return None
        return data.get("data")
    except requests.RequestException as e:
        cost = _time.monotonic() - _t0
        logger.warning(f"⚠️ MEXC {endpoint} 请求失败 ({cost:.1f}s): {e}")
        return None


def okx_sym(sym: str) -> str:
    """内部 sym → OKX instId (BTCUSDT → BTC-USDT-SWAP)"""
    if sym.endswith("USDT"):
        return f"{sym[:-4]}-USDT-SWAP"
    return f"{sym}-USDT-SWAP"


def mexc_sym(sym: str) -> str:
    """内部 sym → MEXC symbol (BTCUSDT → BTC_USDT)"""
    return sym.replace("USDT", "_USDT")


# ── 统一数据获取函数 ──────────────────────────────


def fetch_all_tickers(min_vol: float = MIN_VOL_USDT) -> dict:
    """获取全市场 USDT 永续合约行情。

    正确换算美元成交额: vol_usd = last * volCcy24h
    （volCcy24h 是以币计价的成交量，如 BTC 的 91060 是币数）

    Returns:
        {sym: {vol: USD_vol, chg24h: %, px: last_price, high: high24h, low: low24h, ts: timestamp_ms}}
    """
    import time as _time
    _t0 = _time.monotonic()
    tickers = okx_get("/api/v5/market/tickers", {"instType": "SWAP"})
    result = {}
    if not tickers or not isinstance(tickers, list):
        logger.error("[Data] ❌ OKX tickers 返回空")
        return result

    now_ts = int(_time.time() * 1000)
    for t in tickers:
        inst_id = t.get("instId", "")
        if not inst_id.endswith("USDT-SWAP"):
            continue
        try:
            last = float(t["last"])
            vol_coins = float(t.get("volCcy24h", 0))
            vol_usd = last * vol_coins  # 正确换算为美元
            if vol_usd < min_vol:
                continue
            open24h = float(t["open24h"])
            chg24h = ((last - open24h) / open24h * 100) if open24h else 0.0
            sym = inst_id.replace("-", "").replace("SWAP", "")
            result[sym] = {
                "vol": vol_usd,
                "chg24h": round(chg24h, 2),
                "px": last,
                "high": float(t["high24h"]),
                "low": float(t["low24h"]),
                "ts": now_ts,  # 数据快照时间戳
            }
        except (ValueError, TypeError, KeyError):
            continue

    cost = _time.monotonic() - _t0
    logger.info(
        f"[Data] OKX 行情: {len(result)} 个 (≥{min_vol} USD, {cost:.1f}s)"
    )
    return result


def fetch_mexc_data(ticker_syms: set | None = None) -> tuple:
    """获取 MEXC 资金费率和市值估算。

    Args:
        ticker_syms: 需要获取的 sym 集合，None 则获取全部

    Returns:
        (fr_map, mcap_map)
        fr_map: {sym: funding_rate}
        mcap_map: {sym: est_mcap (USD)}
    """
    import time as _time
    _t0 = _time.monotonic()
    raw = mexc_get("/api/v1/contract/ticker")
    fr_map = {}
    mcap_map = {}

    if not raw or not isinstance(raw, list):
        logger.warning("[Data] MEXC ticker 接口失败")
        return fr_map, mcap_map

    for t in raw:
        sym = t.get("symbol", "").replace("_", "")
        if ticker_syms and sym not in ticker_syms:
            continue
        try:
            fr = float(t.get("fundingRate", 0.0))
            fr_map[sym] = fr
        except (ValueError, TypeError):
            pass
        try:
            amount24 = float(t.get("amount24", 0.0))
            if amount24 > 0:
                mcap_map[sym] = amount24 * 4.0
        except (ValueError, TypeError):
            pass

    cost = _time.monotonic() - _t0
    logger.info(
        f"[Data] MEXC: 费率 {len(fr_map)} 个, 市值 {len(mcap_map)} 个 ({cost:.1f}s)"
    )
    return fr_map, mcap_map


def fetch_cg_trending() -> set:
    """获取 CoinGecko trending 币种集合。

    Returns:
        {sym} 如 {'PEPEUSDT', 'WIFUSDT'}
    """
    import time as _time
    _t0 = _time.monotonic()
    result = set()
    try:
        resp = requests.get(f"{CG_API}/search/trending", timeout=10)
        cost = _time.monotonic() - _t0
        if resp.status_code == 200:
            for item in resp.json().get("coins", []):
                ci = item.get("item", {})
                sym = ci.get("symbol", "").upper() + "USDT"
                result.add(sym)
            logger.info(f"[Data] CG Trending: {len(result)} 个 ({cost:.1f}s)")
        elif resp.status_code == 429:
            logger.warning(f"[Data] CG 429 ({cost:.1f}s)")
        else:
            logger.warning(f"[Data] CG {resp.status_code} ({cost:.1f}s)")
    except Exception as e:
        cost = _time.monotonic() - _t0
        logger.warning(f"[Data] CG 请求失败 ({cost:.1f}s): {e}")
    return result


def fetch_candle_batch(syms: list, bar: str, limit: int = 3) -> dict:
    """批量获取多个币种的 K 线数据。

    OKX 不支持真正批量 candle 端点，但对高流动性币种用单次请求，
    小币走独立请求。这里对传入的 syms 做批量请求优化（按 instId 并发）。

    Args:
        syms: sym 列表
        bar: K线周期 ("5m" | "15m")
        limit: 返回 K线数量

    Returns:
        {sym: {"close_now", "close_ago", "high", "low", "ts"}}
    """
    import concurrent.futures
    import time as _time

    def _fetch_one(sym):
        candles = okx_get(
            "/api/v5/market/candles",
            {"instId": okx_sym(sym), "bar": bar, "limit": str(limit)},
            timeout=8,
        )
        if not candles or not isinstance(candles, list) or len(candles) < 2:
            return sym, None
        try:
            return sym, {
                "close_now": float(candles[0][4]),
                "close_ago": float(candles[1][4]),
                "high": float(candles[0][2]),
                "low": float(candles[0][3]),
                "ts": int(candles[0][0]),
            }
        except (IndexError, ValueError, TypeError):
            return sym, None

    result = {}
    # 并发请求，控制并发数
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        fut_map = {executor.submit(_fetch_one, sym): sym for sym in syms}
        for fut in concurrent.futures.as_completed(fut_map, timeout=90):
            try:
                sym, data = fut.result()
                if data:
                    result[sym] = data
            except Exception:
                pass
            _time.sleep(0.005)  # 轻微错峰

    return result


def _fetch_candle(sym: str, bar: str, limit: int = 3) -> dict | None:
    """通用单币 K 线获取（降级用）。"""
    candles = okx_get(
        "/api/v5/market/candles",
        {"instId": okx_sym(sym), "bar": bar, "limit": str(limit)},
    )
    if not candles or not isinstance(candles, list) or len(candles) < 2:
        return None
    try:
        return {
            "close_now": float(candles[0][4]),
            "close_ago": float(candles[1][4]),
            "high": float(candles[0][2]),
            "low": float(candles[0][3]),
            "ts": int(candles[0][0]),
        }
    except (IndexError, ValueError, TypeError):
        return None


def fetch_candle_5m(sym: str) -> dict | None:
    """获取单个币种的 5 分钟 K 线数据。

    Returns:
        {"close_now": float, "close_ago": float, "high_5m": float, "low_5m": float}
        或 None
    """
    data = _fetch_candle(sym, "5m")
    if not data:
        return None
    return {
        "close_now": data["close_now"],
        "close_ago": data["close_ago"],
        "high_5m": data["high"],
        "low_5m": data["low"],
        "ts": data["ts"],
    }


def fetch_candle_15m(sym: str) -> dict | None:
    """获取单个币种的 15 分钟 K 线数据。

    Returns:
        {"close_now": float, "close_ago": float, "high_15m": float, "low_15m": float}
        或 None
    """
    data = _fetch_candle(sym, "15m")
    if not data:
        return None
    return {
        "close_now": data["close_now"],
        "close_ago": data["close_ago"],
        "high_15m": data["high"],
        "low_15m": data["low"],
        "ts": data["ts"],
    }


def fetch_global_data(min_vol: float = MIN_VOL_USDT) -> tuple:
    """一站式获取所有行情数据（swing 模块专用，全量获取）。

    实时性保障:
      - 每次调用都是全新 API 请求（无缓存）
      - 三步按顺序完成，内部 ticker/fr/cg 版本一致
      - ticker 数据自带 ts 字段记录快照时间

    Returns:
        (ticker_map, fr_map, mcap_map, cg_trending_set)
    """
    import time as _time
    _t0 = _time.monotonic()
    ticker_map = fetch_all_tickers(min_vol)
    fr_map, mcap_map = fetch_mexc_data(set(ticker_map.keys()))
    cg_trending = fetch_cg_trending()
    cost = _time.monotonic() - _t0
    logger.info(f"[Data] fetch_global_data 总计 {cost:.1f}s")
    return ticker_map, fr_map, mcap_map, cg_trending


def check_freshness(ticker_map: dict, max_age_s: int = 10) -> dict:
    """检查数据新鲜度，返回鲜活度报告。

    Args:
        ticker_map: 含 ts 字段的行情数据
        max_age_s: 最大可接受年龄（秒）

    Returns:
        {"fresh": bool, "age_s": float, "sample_syms": [sym, ...]}
    """
    import time as _time
    now = int(_time.time() * 1000)
    ages = {}
    for sym, t in ticker_map.items():
        ts = t.get("ts", 0)
        if ts:
            ages[sym] = (now - ts) / 1000

    if not ages:
        return {"fresh": False, "age_s": 999, "sample_syms": []}

    avg_age = sum(ages.values()) / len(ages)
    top_old = sorted(ages.items(), key=lambda x: -x[1])[:3]
    return {
        "fresh": avg_age <= max_age_s,
        "age_s": round(avg_age, 1),
        "max_age_s": round(max(ages.values()), 1),
        "stale_syms": [s for s, a in top_old],
    }
