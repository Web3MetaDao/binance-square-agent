"""
🏦 抓庄雷达 — wx: accumulation_radar → Binance Square 自动转发
===============================================================
直接调用 accumulation_radar 模块完成扫描 + 评分，然后将完整报告
同时推送到 Telegram 和 Binance Square（只转发报告，不做额外筛选）。
"""

import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── 路径 ──
RADAR_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
RADAR_PKG = os.path.join(RADAR_ROOT, "accumulation_radar")
# 原始 accumulation_radar 的父目录（含 .env.oi）
ORIGINAL_RADAR = os.path.join(os.path.dirname(RADAR_ROOT), "accumulation_radar-clean")

# 将原始项目加入 sys.path 以复用其包
if ORIGINAL_RADAR not in sys.path:
    sys.path.insert(0, ORIGINAL_RADAR)

from config.settings import SQUARE_API_KEY, DATA_DIR
from layers.executor import SquarePoster


def _send_to_square(text):
    """通过 Binance Square API 发帖"""
    if not SQUARE_API_KEY:
        print("[抓庄雷达] ⚠️ 未配置 SQUARE_API_KEY，帖子内容：")
        print(text[:200] + "...")
        return True  # 模拟模式也算成功

    try:
        sq = SquarePoster()
        result = sq.post(text)
        if result:
            print(f"[抓庄雷达] ✅ Binance Square 发帖成功")
            return True
        else:
            print(f"[抓庄雷达] ❌ Binance Square 发帖失败")
            return False
    except Exception as e:
        print(f"[抓庄雷达] ❌ 发帖异常: {e}")
        return False


def _truncate_report(report: str, max_len: int = 1800) -> str:
    """截断报告到 Binance Square 允许的最大长度，保留开头和结尾的关键信息"""
    if len(report) <= max_len:
        return report
    # 保留前 2/3 和后 1/3
    head_len = max_len * 2 // 3
    tail_len = max_len - head_len - 20
    return report[:head_len] + "\n\n...（中间省略）...\n\n" + report[-tail_len:]


def run_pool_and_post():
    """
    执行 pool 扫描 + 结果处理：
    1. 运行 scan_accumulation_pool()
    2. 推送原始报告到 Telegram
    3. 把报告内容也发到 Binance Square
    """
    from accumulation_radar.scanner import scan_accumulation_pool
    from accumulation_radar.report import build_pool_report
    from accumulation_radar.notify import send_telegram
    from accumulation_radar.db import get_db, save_watchlist

    print("[抓庄雷达] 开始 pool 扫描...")
    results = scan_accumulation_pool()
    if not results:
        print("[抓庄雷达] 无收筹标的，跳过")
        return

    # 保存到数据库
    conn = get_db()
    try:
        save_watchlist(conn, results)
    finally:
        conn.close()

    # Telegram 推送
    report = build_pool_report(results)
    if report:
        send_telegram(report)
        # 同步转发到 Binance Square
        square_text = _truncate_report(report)
        _send_to_square(square_text)
        print(f"[抓庄雷达] Binance Square 报告转发完成")


def run_oi_and_post():
    """
    执行 OI 扫描 + 三策略评分 + 结果处理：
    1. 加载标的池
    2. 评分
    3. 推送原始报告到 Telegram
    4. 把报告内容也发到 Binance Square
    """
    from accumulation_radar.config import logger, COINALYZE_API_KEY
    from accumulation_radar.db import get_db, load_watchlist_symbols, load_pool_map
    from accumulation_radar.market import (
        fetch_market_data, fetch_heat_data, scan_oi_history, build_coin_data,
    )
    from accumulation_radar.strategy import score_chase, score_combined, score_ambush
    # score_liquidation requires Coinalyze API; fallback to empty if unavailable
    try:
        from accumulation_radar.strategy import score_liquidation
    except ImportError:
        def score_liquidation(cd, liq): return []
    from accumulation_radar.report import build_strategy_report
    from accumulation_radar.notify import send_telegram

    conn = get_db()
    try:
        watch_syms = load_watchlist_symbols(conn)
    finally:
        conn.close()

    if not watch_syms:
        print("[抓庄雷达] 标的池为空，跳过 OI 扫描")
        return

    print(f"[抓庄雷达] 开始 OI 扫描（{len(watch_syms)} 个标的 + Top100 大盘）...")

    # 全市场数据
    ticker_map, funding_map, mcap_map = fetch_market_data()
    if not ticker_map:
        print("[抓庄雷达] API 失败，跳过")
        return

    heat_map, cg_trending, vol_surge_coins = fetch_heat_data(ticker_map)

    conn = get_db()
    try:
        pool_map = load_pool_map(conn)
    finally:
        conn.close()

    # 扫描标的 + Top100 大盘
    scan_syms = set()
    for sym, pd in pool_map.items():
        st = pd.get("status", "")
        if st in ("firing", "warming"):
            scan_syms.add(sym)
    top100 = sorted(ticker_map.items(), key=lambda x: x[1]["vol"], reverse=True)[:100]
    for sym, _ in top100:
        scan_syms.add(sym)

    oi_map = scan_oi_history(list(scan_syms))

    # Coinalyze 数据（无 key 时跳过）
    coinalyze_data = {"liq": {}}

    # 合并数据 + 三策略
    coin_data = build_coin_data(
        pool_map, oi_map, ticker_map, funding_map, mcap_map,
        heat_map, cg_trending, vol_surge_coins,
    )
    chase = score_chase(coin_data)
    combined = score_combined(coin_data)
    ambush = score_ambush(coin_data)
    liq_signals = score_liquidation(coin_data, coinalyze_data.get("liq", {}))

    # Telegram 推送
    report = build_strategy_report(coin_data, chase, combined, ambush, liq_signals)
    send_telegram(report)

    # 同步转发到 Binance Square
    square_text = _truncate_report(report)
    _send_to_square(square_text)
    print(f"[抓庄雷达] Binance Square 报告转发完成")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "full"
    print(f"🏦 抓庄雷达 — 模式: {mode}")

    if mode in ("full", "pool"):
        run_pool_and_post()
    if mode in ("full", "oi"):
        run_oi_and_post()

    print("✅ 抓庄雷达完成")


if __name__ == "__main__":
    main()
