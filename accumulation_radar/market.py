"""行情 + 资金费率 + 市值 + OI 数据获取模块。

数据源:
  - OKX (行情 tickers + K线)
  - MEXC (持仓量 OI + 资金费率 + 成交额)
  - CoinGecko (trending)
  - Coinalyze (爆仓)

底层 API 调用统一在 sources/market_data.py，本模块只做业务组装。
"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import DATA_DIR, logger
from .sources.market_data import (
    mexc_get,
    okx_sym,
    fetch_cg_trending,
    fetch_mexc_data,
    fetch_all_tickers,
    MIN_VOL_USDT,
)

# ── 公开数据获取 ─────────────────────────────────


def fetch_market_data() -> tuple[dict, dict, dict]:
    """获取全部 USDT 永续合约行情、资金费率、市值估算。

    Returns:
        ticker_map: {sym: {vol, chg, px, high, low}}
        funding_map: {sym: funding_rate (float)}
        mcap_map: {sym: est_mcap (float)}
    """
    # ── 1. OKX SWAP tickers（行情）─────────
    ticker_raw = fetch_all_tickers()
    if not ticker_raw:
        logger.error("❌ fetch_market_data: fetch_all_tickers 返回空")
        return {}, {}, {}

    # fetch_all_tickers 返回 chg24h, 改成 chg 以兼容下游
    ticker_map = {}
    for sym, t in ticker_raw.items():
        ticker_map[sym] = {
            "vol": t["vol"],
            "chg": t["chg24h"],
            "px": t["px"],
            "high": t["high"],
            "low": t["low"],
        }

    logger.info(f"📊 OKX SWAP 行情: {len(ticker_map)} 个 USDT 合约")

    if not ticker_map:
        logger.error("❌ fetch_market_data: 没有有效的 USDT 合约")
        return {}, {}, {}

    # ── 2. 资金费率 + 市值（从 sources 统一接口获取）────
    fr_map, mcap_map = fetch_mexc_data(set(ticker_map.keys()))

    # 补充真实市值
    _real_mcap = _fetch_real_mcap()
    real_count = 0
    for sym in list(mcap_map.keys()):
        coin = sym.replace("USDT", "")
        if coin in _real_mcap:
            mcap_map[sym] = _real_mcap[coin]
            real_count += 1

    logger.info(f"💰 市值: 真实 {real_count} 个 + MEXC粗估 {len(mcap_map) - real_count} 个")

    return ticker_map, fr_map, mcap_map


def fetch_heat_data(ticker_map: dict) -> tuple[dict, list[str], list[str]]:
    """获取热度数据。

    Returns:
        (heat_map, cg_trending_list, vol_surge_coins)
    """
    heat_map = {}
    cg_trending_set = fetch_cg_trending()
    cg_trending_list = [s.replace("USDT", "") for s in cg_trending_set]
    vol_surge_coins = _detect_vol_surge(ticker_map)
    return heat_map, cg_trending_list, vol_surge_coins


def _detect_vol_surge(ticker_map: dict) -> list[str]:
    """检测成交量异动币种。"""
    result = []
    for sym, t in ticker_map.items():
        vol = t.get("vol", 0)
        chg = t.get("chg", 0)
        if vol > 1_000_000 and chg > 5:
            result.append(sym.replace("USDT", ""))
    return result


# ── OI 历史扫描 ──────────────────────────────────


def scan_oi_history(symbols: set | list[str]) -> dict[str, dict]:
    """扫描多币种 OI 历史变化。

    使用 MEXC 接口获取持仓量，同时从本地缓存读取上一轮 OI 值
    计算 OI 变化百分比。

    Returns:
        {sym: {oi_usd, oi_chg_6h_pct}}
    """
    syms = list(symbols) if isinstance(symbols, set) else list(symbols)
    if not syms:
        return {}

    # 加载上一轮的 OI 缓存
    OI_CACHE_PATH = os.path.join(DATA_DIR, "oi_snapshot.json")
    prev_oi_cache = {}
    if os.path.exists(OI_CACHE_PATH):
        try:
            with open(OI_CACHE_PATH, "r") as f:
                prev_oi_cache = json.load(f)
        except (json.JSONDecodeError, OSError):
            prev_oi_cache = {}

    oi_map = {}
    batch_size = 200

    for i in range(0, len(syms), batch_size):
        batch = syms[i:i + batch_size]
        raw_data = mexc_get("/api/v1/contract/ticker")
        if not raw_data or not isinstance(raw_data, list):
            continue

        for t in raw_data:
            sym = t.get("symbol", "").replace("_", "")
            if sym not in symbols:
                continue
            try:
                oi_usd = float(t.get("holdVol", 0)) * float(t.get("lastPrice", 0))
                oi_map[sym] = {"oi_usd": oi_usd}
            except (ValueError, TypeError):
                continue

    # 计算 OI 变化百分比（基于缓存）
    for sym, entry in oi_map.items():
        current_oi = entry["oi_usd"]
        prev_oi = prev_oi_cache.get(sym, 0)
        if prev_oi > 0 and current_oi > 0:
            entry["oi_chg_6h_pct"] = (current_oi - prev_oi) / prev_oi * 100

    # 保存当前 OI 到缓存（覆盖）
    try:
        current_snapshot = {sym: entry["oi_usd"] for sym, entry in oi_map.items()}
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(OI_CACHE_PATH, "w") as f:
            json.dump(current_snapshot, f)
    except (OSError, TypeError):
        pass

    logger.info(f"📊 OI 数据: {len(oi_map)} 个")
    return oi_map


# ── 数据组装 ──────────────────────────────────────


def build_coin_data(
    pool_map: dict,
    oi_map: dict,
    ticker_map: dict,
    funding_map: dict,
    mcap_map: dict,
    heat_map: dict | None = None,
    cg_trending: list | None = None,
    vol_surge_coins: list | None = None,
) -> dict[str, dict]:
    """合并多源数据为统一的 coin_data 字典。"""
    coin_data = {}

    # 计算 OI 变化百分比（需要至少两段历史）
    # 从 oi_map 中提取 oi_usd 作为基础值
    # 但由于当前 scan_oi_history 只返回当前 OI，
    # OI 变化百分比需要从 pool_map 中的历史或 ai_segments 计算

    all_syms = set(ticker_map.keys())
    all_syms.update(pool_map.keys())
    all_syms.update(oi_map.keys())

    for sym in all_syms:
        t = ticker_map.get(sym, {})
        pm = pool_map.get(sym, {})
        oi_entry = oi_map.get(sym, {})

        coin_name = sym.replace("USDT", "")
        fr = funding_map.get(sym, 0.0)
        oi_usd = oi_entry.get("oi_usd", 0.0)

        # OI 变化百分比：优先用 oi_chg_6h_pct（如果有），否则从 oi_usd 推算
        d6h = oi_entry.get("oi_chg_6h_pct", 0.0)
        # 如果 scan_oi_history 没有提供变化率，用 oi_segments 计算
        oi_segments = pm.get("oi_segments", [])

        coin_data[sym] = {
            "sym": sym,
            "coin": coin_name,
            "px": t.get("px", 0.0),
            "px_chg": t.get("chg", 0.0),
            "vol": t.get("vol", 0.0),
            "fr": fr,
            "fr_pct": round(fr * 100, 4),
            "est_mcap": mcap_map.get(sym, 0.0),
            "oi_usd": oi_usd,
            "d6h": d6h,  # OI 6h 变化百分比
            "oi_segments": oi_segments,
            "sw_days": pm.get("sideways_days", 0),
            "pool_rng": pm.get("range_pct", 0.0),
            "status": pm.get("status", ""),
            "cg_trending": (cg_trending and coin_name in cg_trending) or False,
            "vol_surge": (vol_surge_coins and coin_name in vol_surge_coins) or False,
        }

    return coin_data


# ── 市值数据源 ────────────────────────────────────


def _fetch_real_mcap() -> dict[str, float]:
    """从 CoinGecko 获取真实流通市值。

    Returns:
        {coin_name: mcap_usd}
    """
    import requests

    cache_path = os.path.join(DATA_DIR, "mcap_cache.json")
    cache_age_max = 3600  # 1小时缓存

    # 尝试读缓存
    if os.path.exists(cache_path):
        try:
            age = time.time() - os.path.getmtime(cache_path)
            if age < cache_age_max:
                with open(cache_path) as f:
                    return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass

    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 250,
                "page": 1,
                "sparkline": "false",
            },
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(f"⚠️ CG mcap API 返回 {resp.status_code}")
            return {}

        result = {}
        for coin in resp.json():
            sym = (coin.get("symbol") or "").upper()
            mcap = coin.get("market_cap")
            if sym and mcap:
                result[sym] = float(mcap)

        if result:
            with open(cache_path, "w") as f:
                json.dump(result, f)

        return result
    except Exception as e:
        logger.warning(f"⚠️ CG mcap 请求失败: {e}")
        return {}


def _get_fr_snapshot_path() -> str:
    return os.path.join(DATA_DIR, "fr_snapshot.json")


def save_fr_snapshot(funding_map: dict) -> dict:
    """保存当前费率快照并返回上一轮快照。"""
    path = _get_fr_snapshot_path()
    prev = {}
    if os.path.exists(path):
        try:
            with open(path) as f:
                prev = json.load(f)
        except (json.JSONDecodeError, OSError):
            pass

    with open(path, "w") as f:
        json.dump(funding_map, f)

    return prev
