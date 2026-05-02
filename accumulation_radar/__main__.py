"""庄家收筹雷达 — 入口

用法: python -m accumulation_radar [pool|oi|full]
"""
import sys
from datetime import datetime

from .config import logger
from .db import get_db, save_watchlist, load_watchlist_symbols, load_pool_map
from .scanner import scan_accumulation_pool
from .market import fetch_market_data, fetch_heat_data, scan_oi_history, build_coin_data
from .strategy import score_chase, score_combined, score_ambush
from .report import build_pool_report, build_strategy_report
from .notify import send_square, send_telegram


def run_pool(conn):
    """模块A: 更新收筹标的池"""
    results = scan_accumulation_pool()
    if results:
        save_watchlist(conn, results)
        report = build_pool_report(results)
        if report:
            send_telegram(report)
            send_square(report)


def run_oi(conn):
    """模块B: OI异动 + 三策略评分"""
    watchlist = load_watchlist_symbols(conn)
    if not watchlist:
        logger.warning("⚠️ 标的池为空，先运行 pool 模式")
        return

    # 1. 全市场数据
    ticker_map, funding_map, mcap_map = fetch_market_data()
    if not ticker_map:
        logger.error("❌ API失败")
        return

    # 2. 热度数据
    heat_map, cg_trending, vol_surge_coins = fetch_heat_data(ticker_map)

    # 3. 收筹池 + OI历史
    pool_map = load_pool_map(conn)
    scan_syms = set()
    for sym, pd in pool_map.items():
        st = pd.get("status", "")
        if st in ("firing", "warming"):
            scan_syms.add(sym)
    top_by_vol = sorted(ticker_map.items(), key=lambda x: x[1]["vol"], reverse=True)[:100]
    for sym, _ in top_by_vol:
        scan_syms.add(sym)

    oi_map = scan_oi_history(scan_syms)

    # 4. 合并数据 + 三策略评分
    coin_data = build_coin_data(
        pool_map, oi_map, ticker_map, funding_map, mcap_map,
        heat_map, cg_trending, vol_surge_coins,
    )
    chase = score_chase(coin_data)
    combined = score_combined(coin_data)
    ambush = score_ambush(coin_data)

    # 5. 生成报告并推送
    report = build_strategy_report(coin_data, chase, combined, ambush)
    send_telegram(report)
    send_square(report)


def run_swing(conn):
    """模块S: 异动机会评分检测，推送 TG + Square，持久化异动历史"""
    from .sources.market_data import fetch_global_data
    from .sources.coinalyze_calibrate import calibrate_prices
    from .swing import score_opportunities
    from .report import build_swing_report, build_swing_silent_report
    from .db import save_swing_results

    ticker_map, fr_map, mcap_map, cg_trending = fetch_global_data()
    if not ticker_map:
        logger.error("[Swing] ❌ 无法获取行情数据")
        return

    # Coinalyze 校对：修正 OKX 24h 涨跌幅偏差
    ticker_map, dev_report = calibrate_prices(ticker_map, top_n=30)
    if dev_report:
        logger.info(f"[Swing] Coinalyze 修正 {len(dev_report)} 个币的24h涨跌幅")

    result = score_opportunities(ticker_map, fr_map, mcap_map, cg_trending)
    if not result:
        logger.info("[Swing] 未发现异动机会，发送静默摘要")
        # 构建摘要信息，即使零候选也推送
        btc_info = ticker_map.get("BTCUSDT", {})
        btc_chg = btc_info.get("px_chg", 0) if isinstance(btc_info, dict) else 0
        result_info = {
            "candidates_scanned": len(ticker_map),
            "btc_chg24h": btc_chg,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        msg = build_swing_silent_report(result_info)
        send_telegram(msg)
        return

    # 持久化异动历史
    try:
        saved = save_swing_results(conn, result)
        if saved:
            logger.info(f"[Swing] 持久化 {saved} 条异动记录")
            conn.commit()
    except Exception as e:
        logger.warning(f"[Swing] 持久化失败: {e}")

    msg = build_swing_report(result)
    if msg:
        send_telegram(msg)
        send_square(msg)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "full"
    logger.info(f"🏦 庄家收筹雷达 v2 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 模式: {mode}")

    conn = get_db()
    try:
        if mode in ("full", "pool"):
            run_pool(conn)
        if mode in ("full", "oi"):
            run_oi(conn)
        if mode in ("full", "swing", "s"):
            run_swing(conn)
    finally:
        conn.close()

    logger.info("✅ 完成")


if __name__ == "__main__":
    main()
