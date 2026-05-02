"""
utils/price_sync.py
币安期货合约实时价格同步模块

核心设计：
- 只使用币安期货合约 API（fapi.binance.com），与内容挖矿 W2E 收益直接挂钩
- 启动时一次性拉取全市场 609 个合约的 24h 行情，缓存 60 秒
- 发帖前调用 batch_refresh_prices() 刷新信号中的价格字段
- 三种模式（聪明钱、热点、W2E）统一使用此模块

W2E 收益逻辑：
    用户通过帖子中的 $CashTag → 点击期货合约 → 产生交易 → 作者获得挖矿收益
    因此价格必须是期货合约实时价格，不能用现货或 Hyperliquid markPx
"""

import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

# 币安 U 本位永续合约 API
FAPI = "https://fapi.binance.com/fapi/v1"
PRICE_FRESHNESS_TTL = 600

# 全市场快照缓存（一次拉取所有合约）
_snapshot: dict = {}       # symbol → price_dict
_snapshot_ts: float = 0
SNAPSHOT_TTL = 60          # 全市场快照 60 秒有效

# 单币种缓存（用于按需获取）
_single_cache: dict = {}
SINGLE_TTL = 30            # 单币种缓存 30 秒


def _sym(coin: str) -> str:
    """代币名 → 期货合约交易对，如 BTC → BTCUSDT"""
    coin = coin.upper().strip()
    return coin if coin.endswith("USDT") else f"{coin}USDT"


def _parse_ticker(d: dict) -> dict:
    """解析币安期货 24hr ticker 响应为标准格式"""
    return {
        "coin": d["symbol"].replace("USDT", ""),
        "symbol": d["symbol"],
        "price": float(d["lastPrice"]),
        "change_24h": float(d["priceChangePercent"]),
        "high_24h": float(d["highPrice"]),
        "low_24h": float(d["lowPrice"]),
        "volume_24h": float(d["quoteVolume"]),   # 24h 成交额（USDT）
        "open_24h": float(d["openPrice"]),
        "ts": time.time(),
    }


def refresh_snapshot(force: bool = False) -> bool:
    """
    拉取全市场期货合约快照（一次请求覆盖所有 609 个合约）

    返回：是否成功
    """
    global _snapshot, _snapshot_ts
    now = time.time()

    if not force and _snapshot and now - _snapshot_ts < SNAPSHOT_TTL:
        return True  # 缓存有效，无需刷新

    try:
        r = requests.get(f"{FAPI}/ticker/24hr", timeout=15)
        if r.status_code != 200:
            logger.warning(f"[PriceSync] 全市场快照请求失败 HTTP {r.status_code}")
            return False

        data = r.json()
        new_snapshot = {}
        for d in data:
            sym = d.get("symbol", "")
            if sym.endswith("USDT"):
                new_snapshot[sym] = _parse_ticker(d)

        _snapshot = new_snapshot
        _snapshot_ts = now
        logger.info(f"[PriceSync] 期货合约快照已刷新，共 {len(_snapshot)} 个合约")
        return True

    except Exception as e:
        logger.warning(f"[PriceSync] 全市场快照异常: {e}")
        return False


# ── 现货 API 降级（不受 geoblock 影响，fapi.binance.com HTTP 451 时的备选）──
SPOT_API = "https://api.binance.com/api/v3"


def _spot_parse_ticker(d: dict) -> dict:
    """解析币安现货 24hr ticker 为标准格式"""
    return {
        "coin": d["symbol"].replace("USDT", ""),
        "symbol": d["symbol"],
        "price": float(d["lastPrice"]),
        "change_24h": float(d["priceChangePercent"]),
        "high_24h": float(d["highPrice"]),
        "low_24h": float(d["lowPrice"]),
        "volume_24h": float(d["quoteVolume"]),
        "open_24h": float(d["openPrice"]),
        "ts": time.time(),
        "_source": "binance_spot",
    }


def get_spot_price(coin: str) -> Optional[dict]:
    """从币安现货 API 获取单个币种实时价格（备选源，futures 不可用时使用）。"""
    sym = _sym(coin)
    try:
        r = requests.get(f"{SPOT_API}/ticker/24hr", params={"symbol": sym}, timeout=10)
        if r.status_code == 200:
            data = _spot_parse_ticker(r.json())
            return annotate_price_freshness(data)
        else:
            logger.warning(f"[PriceSync] 现货 {sym} HTTP {r.status_code}")
            return None
    except Exception as e:
        logger.warning(f"[PriceSync] 现货 {sym} 请求异常: {e}")
        return None


def get_authoritative_price(coin: str) -> Optional[dict]:
    """
    权威价格获取：优先期货 → 降级现货 → 降级 CoinGecko → annotate freshness。

    返回格式与 get_futures_price 完全一致（含 is_live / _source 等字段）。
    上层可直接用于判断是否发数据帖还是分析帖。
    """
    fp = get_futures_price(coin)
    if fp:
        fp["_source"] = "binance_futures"
        return fp
    sp = get_spot_price(coin)
    if sp:
        sp["_source"] = "binance_spot"
        return sp
    cg = get_coingecko_price(coin)
    if cg:
        cg["_source"] = "coingecko"
        return cg
    return None


def get_futures_price(coin: str, force: bool = False) -> Optional[dict]:
    """
    获取单个币种的期货合约实时价格

    参数：
        coin: 代币名称，如 "BTC"、"ETH"、"SOL"
        force: 是否强制刷新（忽略缓存）

    返回：
        {
            "coin": "BTC",
            "symbol": "BTCUSDT",
            "price": 77360.0,        # 期货最新成交价
            "change_24h": -0.315,    # 24h涨跌幅（%）
            "high_24h": 78000.0,
            "low_24h": 76500.0,
            "volume_24h": 2.3e10,    # 24h成交额（USDT）
            "ts": 1714000000.0,
        }
        或 None（该币种无期货合约）
    """
    sym = _sym(coin)
    now = time.time()

    # 优先从全市场快照获取
    if not force and _snapshot and now - _snapshot_ts < SNAPSHOT_TTL:
        cached = _snapshot.get(sym)
        return annotate_price_freshness(cached) if cached else None

    # 单独请求
    if not force and sym in _single_cache:
        cached = _single_cache[sym]
        if now - cached["ts"] < SINGLE_TTL:
            return annotate_price_freshness(cached)

    try:
        r = requests.get(f"{FAPI}/ticker/24hr", params={"symbol": sym}, timeout=8)
        if r.status_code == 200:
            data = _parse_ticker(r.json())
            _single_cache[sym] = data
            _snapshot[sym] = data  # 同步更新快照
            return annotate_price_freshness(data)
        elif r.status_code == 400:
            # 该币种无期货合约
            logger.debug(f"[PriceSync] {sym} 无期货合约（HTTP 400）")
            return None
        else:
            logger.warning(f"[PriceSync] {sym} HTTP {r.status_code}")
            return None
    except Exception as e:
        logger.warning(f"[PriceSync] {sym} 请求异常: {e}")
        return None


def batch_refresh_prices(signals: list) -> list:
    """
    批量刷新信号列表中所有币种的期货合约实时价格

    在发帖前调用，确保帖子中的价格为最新期货合约价格

    参数：
        signals: 信号列表，每个信号含 "coin" 和 "data" 字段

    返回：
        更新后的信号列表（原地修改 data 中的价格字段）
    """
    if not signals:
        return signals

    # 先刷新全市场快照（一次请求搞定所有币种）
    refresh_snapshot(force=True)

    coins = [s.get("coin", "").upper() for s in signals if s.get("coin")]
    logger.info(f"[PriceSync] 发帖前同步期货价格: {', '.join(set(coins))}")

    updated = []
    for signal in signals:
        coin = signal.get("coin", "").upper()
        if not coin:
            updated.append(signal)
            continue

        price_info = annotate_price_freshness(_snapshot.get(_sym(coin)))
        if not price_info:
            # 快照中没有，尝试单独请求
            price_info = get_futures_price(coin)

        if price_info and is_price_fresh(price_info, max_age=PRICE_FRESHNESS_TTL):
            data = signal.get("data", {})
            old_price = data.get("mark_px") or data.get("price") or 0

            # 更新所有价格相关字段
            for field in ("mark_px", "price"):
                if field in data:
                    data[field] = price_info["price"]
            for field in ("change_24h", "h24_change_pct"):
                if field in data:
                    data[field] = price_info["change_24h"]
            for field in ("high_24h", "low_24h", "volume_24h", "open_24h"):
                if field in price_info:
                    data[field] = price_info[field]

            # 若原来没有价格字段，也注入
            if "mark_px" not in data and "price" not in data:
                data["price"] = price_info["price"]
                data["change_24h"] = price_info["change_24h"]

            signal["data"] = data
            signal["_price_synced"] = True
            signal["_price_source"] = "binance_futures"
            signal["_price_ts"] = price_info["ts"]

            # Mirror freshness metadata into data so prompt builders that only
            # receive signal['data'] can avoid embedding stale/unsynced prices.
            data["_price_synced"] = True
            data["_price_source"] = "binance_futures"
            data["_price_ts"] = price_info["ts"]
            data["is_live"] = True
            data["price_age_sec"] = price_info.get("price_age_sec")
            signal["source"] = "binance_futures"
            signal["is_live"] = price_info.get("is_live", is_price_fresh(price_info))
            if signal["is_live"]:
                signal.pop("warning_reason", None)
            else:
                signal["warning_reason"] = "stale_or_unverified_futures_price"

            new_price = price_info["price"]
            if old_price and abs(old_price - new_price) / max(old_price, 1e-8) > 0.001:
                logger.info(
                    f"[PriceSync] ✅ {coin}: ${old_price:,.4f} → ${new_price:,.4f} "
                    f"({price_info['change_24h']:+.2f}%)"
                )
            else:
                logger.info(
                    f"[PriceSync] ✅ {coin}: ${new_price:,.4f} ({price_info['change_24h']:+.2f}%)"
                )
        else:
            logger.warning(f"[PriceSync] ⚠️  {coin} 无新鲜期货价格，保留原有价格但标记为未同步")
            data = signal.get("data", {})
            data["_price_synced"] = False
            data["_price_source"] = None
            data["_price_ts"] = price_info.get("ts") if isinstance(price_info, dict) else None
            data["is_live"] = False
            data["price_age_sec"] = price_info.get("price_age_sec") if isinstance(price_info, dict) else None
            signal["data"] = data
            signal["_price_synced"] = False
            signal["_price_source"] = None
            signal["_price_ts"] = data["_price_ts"]
            signal["is_live"] = False
            signal["warning_reason"] = "no_fresh_binance_futures_price"

        updated.append(signal)

    synced = sum(1 for s in updated if s.get("_price_synced"))
    logger.info(f"[PriceSync] 完成: {synced}/{len(updated)} 个信号价格已同步为期货合约实时价格")
    return updated


def enrich_signal_price(signal: dict) -> dict:
    """
    刷新单个信号的期货合约实时价格（便捷函数）
    """
    return batch_refresh_prices([signal])[0]


def get_market_snapshot_for_post(coins: list) -> dict:
    """
    为发帖获取指定币种的期货合约价格快照

    用于 W2E 模式和热点模式在生成帖子前注入价格

    参数：
        coins: 代币列表，如 ["BTC", "ETH", "SOL"]

    返回：
        {coin: price_dict, ...}
    """
    refresh_snapshot(force=True)
    result = {}
    for coin in coins:
        sym = _sym(coin.upper())
        if sym in _snapshot:
            result[coin.upper()] = _snapshot[sym]
        else:
            # 尝试单独请求
            info = get_futures_price(coin)
            if info:
                result[coin.upper()] = info
    return result


# CoinGecko 免费 API（币安 451 时的三级降级源）
COINGECKO_API = "https://api.coingecko.com/api/v3"
# CoinGecko ID 映射（兼容 config.settings.FUTURES_MAP 中的各种别名）
_COINGECKO_IDS = {
    "BTC": "bitcoin", "BITCOIN": "bitcoin",
    "ETH": "ethereum", "ETHEREUM": "ethereum",
    "BNB": "binancecoin",
    "SOL": "solana", "SOLANA": "solana",
    "XRP": "ripple",
    "DOGE": "dogecoin",
    "ADA": "cardano",
    "DOT": "polkadot",
    "LINK": "chainlink",
    "AVAX": "avalanche-2",
    "SUI": "sui",
    "PEPE": "pepe",
    "ARB": "arbitrum",
    "OP": "optimism",
    "WIF": "dogwifcoin",
    "BONK": "bonk",
    "TRUMP": "official-trump",
    "MEME": "memecoin",
    "TON": "the-open-network",
    "NOT": "notcoin",
    "JUP": "jupiter-exchange-solana",
    "TIA": "celestia",
    "INJ": "injective-protocol",
    "APT": "aptos",
    "NEAR": "near",
    "ATOM": "cosmos",
    "LTC": "litecoin",
    "MATIC": "matic-network",
    "W": "wormhole",
}

# 核心币种（前 10 个高频使用币），批量快照只刷这些
_CORE_COINS = ["BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT"]

# CoinGecko 单币种缓存 + 全局批量快照
_cg_cache: dict = {}
_cg_snapshot: dict = {}        # {coin_upper: price_dict, ...} 批量快照
_cg_snapshot_ts: float = 0
CG_CACHE_TTL = 30  # 单币缓存 30 秒
CG_SNAPSHOT_TTL = 25  # 批量快照 25 秒（略短于单币缓存，避免频繁刷新）


def _get_coingecko_id(coin: str) -> Optional[str]:
    """代币名 → CoinGecko ID"""
    return _COINGECKO_IDS.get(coin.upper().strip())


def _cg_refresh_batch() -> None:
    """
    刷新全局 CoinGecko 批量快照（只拉核心币种，避免 429）。
    一次请求 10 个核心币，其余币种按需独立请求。
    如果快照还在寿命内则跳过，避免反复 429。
    遇到 429 时自动退避 2 秒后重试一次。
    """
    global _cg_snapshot, _cg_snapshot_ts
    now = time.time()
    if _cg_snapshot and now - _cg_snapshot_ts < CG_SNAPSHOT_TTL:
        return

    if not _CORE_COINS:
        return

    cg_ids = []
    for coin in _CORE_COINS:
        cg_id = _COINGECKO_IDS.get(coin)
        if cg_id:
            cg_ids.append(cg_id)

    if not cg_ids:
        return

    for attempt in range(3):
        try:
            url = f"{COINGECKO_API}/simple/price"
            params = {
                "ids": ",".join(cg_ids),
                "vs_currencies": "usd",
                "include_24hr_change": "true",
                "include_24hr_vol": "true",
                "include_market_cap": "false",
            }
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                data = r.json()
                now = time.time()
                new_snapshot = {}
                for coin_name in _CORE_COINS:
                    cg_id = _COINGECKO_IDS.get(coin_name)
                    raw = data.get(cg_id, {}) if cg_id else {}
                    usd = raw.get("usd")
                    if usd is None:
                        continue
                    new_snapshot[coin_name] = {
                        "coin": coin_name,
                        "symbol": f"{coin_name}USDT",
                        "price": float(usd),
                        "change_24h": raw.get("usd_24h_change") or 0.0,
                        "high_24h": 0.0,
                        "low_24h": 0.0,
                        "volume_24h": raw.get("usd_24h_vol") or 0.0,
                        "ts": now,
                    }

                _cg_snapshot = new_snapshot
                _cg_snapshot_ts = now
                logger.info(f"[PriceSync] ✅ CoinGecko 全局快照刷新: {len(new_snapshot)} 个核心币")
                return
            elif r.status_code == 429 and attempt < 2:
                wait = 2 * (attempt + 1)
                logger.warning(f"[PriceSync] CoinGecko 429，退避 {wait}s 重试 (第{attempt+2}次)")
                time.sleep(wait)
            else:
                logger.warning(f"[PriceSync] CoinGecko 全局快照 HTTP {r.status_code}")
                return
        except Exception as e:
            logger.warning(f"[PriceSync] CoinGecko 全局快照异常: {e}")
            return


def _cg_single_fetch(coin: str) -> Optional[dict]:
    """
    按需独立请求单个非核心币的 CoinGecko 价格。
    核心币由 _cg_refresh_batch 批量处理，避免重复请求。
    遇到 429 自动退避重试。
    """
    cg_id = _get_coingecko_id(coin)
    if cg_id is None:
        return None

    for attempt in range(3):
        try:
            url = f"{COINGECKO_API}/simple/price"
            params = {
                "ids": cg_id,
                "vs_currencies": "usd",
                "include_24hr_change": "true",
                "include_24hr_vol": "true",
                "include_market_cap": "false",
            }
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                data = r.json()
                raw = data.get(cg_id, {})
                usd = raw.get("usd")
                if usd is None:
                    return None

                now = time.time()
                return {
                    "coin": coin.upper(),
                    "symbol": f"{coin.upper()}USDT",
                    "price": float(usd),
                    "change_24h": raw.get("usd_24h_change") or 0.0,
                    "high_24h": 0.0,
                    "low_24h": 0.0,
                    "volume_24h": raw.get("usd_24h_vol") or 0.0,
                    "ts": now,
                }
            elif r.status_code == 429 and attempt < 2:
                wait = 2 * (attempt + 1)
                logger.warning(f"[PriceSync] CoinGecko {coin} 429，退避 {wait}s 重试 (第{attempt+2}次)")
                time.sleep(wait)
            else:
                logger.warning(f"[PriceSync] CoinGecko {coin} HTTP {r.status_code}")
                return None
        except Exception as e:
            logger.warning(f"[PriceSync] CoinGecko {coin} 请求异常: {e}")
            return None
    return None


def get_coingecko_price(coin: str) -> Optional[dict]:
    """
    从 CoinGecko 免费 API 获取单个币种实时价格（通过全局批量快照 + 按需回退）。

    币安 API HTTP 451（法律限制）时的三级降级源。
    核心币由全局快照批量刷新（~25秒一次），非核心币按需独立请求。

    返回格式（与 get_futures_price 兼容）：
        {
            "coin": "BTC",
            "symbol": "BTCUSDT",        # 保持兼容
            "price": 77360.0,
            "change_24h": -0.315,       # 24h涨跌幅（%）
            "high_24h": 0.0,
            "low_24h": 0.0,
            "volume_24h": 0.0,
            "ts": 1714000000.0,
        }
        或 None（CoinGecko 不可用/不认识该币种）
    """
    upper = coin.upper().strip()

    # 尝试从全局核心币快照获取
    _cg_refresh_batch()
    if upper in _cg_snapshot:
        _cg_cache[upper] = _cg_snapshot[upper]
        return annotate_price_freshness(dict(_cg_snapshot[upper]))

    # 缓存回退
    now = time.time()
    if upper in _cg_cache and now - _cg_cache[upper]["ts"] < CG_CACHE_TTL:
        return annotate_price_freshness(dict(_cg_cache[upper]))

    # 非核心币：按需独立请求
    if upper not in _CORE_COINS:
        result = _cg_single_fetch(upper)
        if result:
            _cg_cache[upper] = result
            return annotate_price_freshness(dict(result))

    return None


def format_price(price: float) -> str:
    """格式化期货合约价格显示"""
    if price >= 10000:
        return f"${price:,.0f}"
    elif price >= 100:
        return f"${price:,.2f}"
    elif price >= 1:
        return f"${price:.4f}"
    else:
        return f"${price:.6f}"


def is_price_fresh(price_info: Optional[dict], max_age: float = PRICE_FRESHNESS_TTL) -> bool:
    """仅当价格带有有效时间戳且未超过 freshness 窗口时才视为实时。"""
    if not isinstance(price_info, dict):
        return False
    ts = price_info.get("ts")
    if ts is None:
        return False
    try:
        ts = float(ts)
    except (TypeError, ValueError):
        return False
    age = time.time() - ts
    if age < 0:
        age = 0
    return age <= max_age


def annotate_price_freshness(price_info: Optional[dict], max_age: float = PRICE_FRESHNESS_TTL) -> Optional[dict]:
    """补充 freshness 运行态字段，供上层决定能否称为实时/最新价。"""
    if not isinstance(price_info, dict):
        return price_info
    enriched = dict(price_info)
    enriched["is_live"] = is_price_fresh(enriched, max_age=max_age)
    ts = enriched.get("ts")
    try:
        enriched["price_age_sec"] = max(0.0, time.time() - float(ts)) if ts is not None else None
    except (TypeError, ValueError):
        enriched["price_age_sec"] = None
    return enriched


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print("🧪 测试币安期货合约实时价格同步\n")

    # 测试全市场快照
    print("=== 全市场快照 ===")
    ok = refresh_snapshot(force=True)
    print(f"快照状态: {'✅ 成功' if ok else '❌ 失败'}, 合约数: {len(_snapshot)}")

    # 测试主流币
    print("\n=== 主流期货合约价格 ===")
    for coin in ["BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ORCA", "HYPE"]:
        info = get_futures_price(coin)
        if info:
            print(f"  {coin}: {format_price(info['price'])} ({info['change_24h']:+.2f}%) "
                  f"| 24h高: {format_price(info['high_24h'])} 低: {format_price(info['low_24h'])}")
        else:
            print(f"  {coin}: ⚠️  无期货合约（将在帖子中跳过价格）")

    # 测试信号批量刷新
    print("\n=== 信号批量价格刷新 ===")
    test_signals = [
        {"coin": "BTC", "type": "LONG_HIGH", "data": {"mark_px": 70000.0, "change_24h": 0.0}},
        {"coin": "ETH", "type": "SHORT_HIGH", "data": {"mark_px": 2000.0, "change_24h": 0.0}},
        {"coin": "SOL", "type": "TG_OI_SURGE", "data": {"price": 100.0, "h24_change_pct": 0.0}},
        {"coin": "HYPE", "type": "TG_COMBINED", "data": {"price": 10.0, "change_24h": 0.0}},
    ]
    print("  刷新前:")
    for s in test_signals:
        p = s["data"].get("mark_px") or s["data"].get("price")
        print(f"    {s['coin']}: ${p}")

    refreshed = batch_refresh_prices(test_signals)
    print("  刷新后:")
    for s in refreshed:
        p = s["data"].get("mark_px") or s["data"].get("price")
        synced = "✅" if s.get("_price_synced") else "⚠️"
        print(f"    {s['coin']}: ${p:,.4f} {synced}")
