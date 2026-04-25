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
        return _snapshot.get(sym)

    # 单独请求
    if not force and sym in _single_cache:
        cached = _single_cache[sym]
        if now - cached["ts"] < SINGLE_TTL:
            return cached

    try:
        r = requests.get(f"{FAPI}/ticker/24hr", params={"symbol": sym}, timeout=8)
        if r.status_code == 200:
            data = _parse_ticker(r.json())
            _single_cache[sym] = data
            _snapshot[sym] = data  # 同步更新快照
            return data
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

        price_info = _snapshot.get(_sym(coin))
        if not price_info:
            # 快照中没有，尝试单独请求
            price_info = get_futures_price(coin)

        if price_info:
            data = signal.get("data", {})
            old_price = data.get("mark_px") or data.get("price") or 0

            # 更新所有价格相关字段
            for field in ("mark_px", "price"):
                if field in data:
                    data[field] = price_info["price"]
            for field in ("change_24h", "h24_change_pct"):
                if field in data:
                    data[field] = price_info["change_24h"]

            # 若原来没有价格字段，也注入
            if "mark_px" not in data and "price" not in data:
                data["price"] = price_info["price"]
                data["change_24h"] = price_info["change_24h"]

            signal["data"] = data
            signal["_price_synced"] = True
            signal["_price_source"] = "binance_futures"
            signal["_price_ts"] = price_info["ts"]

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
            logger.warning(f"[PriceSync] ⚠️  {coin} 无期货合约，保留原有价格")
            signal["_price_synced"] = False

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
