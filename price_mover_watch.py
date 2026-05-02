#!/usr/bin/env python3
"""
涨跌幅异动监控 — Price Mover Watch
=====================================
轮询 OKX USDT 永续合约全市场行情，识别短时涨跌幅异常币种，
推送 TOP 涨/跌榜到 Telegram。

运行方式:
  python price_mover_watch.py          # 单次运行，推送到TG
  python price_mover_watch.py --top 5  # 只看前5名
  python price_mover_watch.py --dry    # 仅打印，不发TG

数据源: OKX /api/v5/market/tickers (全市场 USDT-SWAP)
"""

import os
import sys
import json
import time as _time
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── 配置 ─────────────────────────────────────────────
DATA_DIR = os.getenv("DATA_DIR", "/root/binance-square-agent/data")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

MIN_VOL_USDT = 300_000        # 最低24h成交额 (过滤垃圾币)
TOP_N = 10                    # 涨跌榜各显示多少个
CHG_ALERT_THRESHOLD = 5.0     # 单一异动告警阈值 (%)

# ── 日志 ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("price_mover")

# ── API ──────────────────────────────────────────────
OKX_API = "https://www.okx.com"


def okx_tickers(min_vol: float = MIN_VOL_USDT) -> dict:
    """获取 OKX USDT-SWAP 全市场行情。"""
    url = f"{OKX_API}/api/v5/market/tickers"
    try:
        resp = requests.get(url, params={"instType": "SWAP"}, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"OKX 返回 {resp.status_code}")
            return {}
        data = resp.json()
        if data.get("code") != "0":
            logger.warning(f"OKX code={data.get('code')}")
            return {}
    except requests.RequestException as e:
        logger.warning(f"OKX 请求失败: {e}")
        return {}

    result = {}
    for t in data.get("data", []):
        inst_id = t.get("instId", "")
        if not inst_id.endswith("USDT-SWAP"):
            continue
        try:
            last = float(t["last"])
            open24h = float(t["open24h"])
            vol_coins = float(t.get("volCcy24h", 0))
            vol_usd = last * vol_coins
            if vol_usd < min_vol:
                continue
            chg24h = ((last - open24h) / open24h * 100) if open24h else 0.0
            sym = inst_id.replace("-", "").replace("SWAP", "")
            result[sym] = {
                "chg24h": round(chg24h, 2),
                "px": last,
                "vol_usd": int(vol_usd),
                "high24h": float(t["high24h"]),
                "low24h": float(t["low24h"]),
            }
        except (ValueError, TypeError, KeyError):
            continue

    logger.info(f"OKX 行情: {len(result)} 个 (≥{min_vol/1000:.0f}K USD)")
    return result


def format_tg_message(top_gainers: list, top_losers: list, ts: str) -> str:
    """格式化 Telegram 消息。"""
    lines = []
    lines.append(f"📊 *涨跌幅异动监控* · {ts}")
    lines.append(f"数据源: OKX 永续合约 | 最低成交额: ≥{MIN_VOL_USDT/1000:.0f}K USD")
    lines.append("")

    # 涨幅榜
    lines.append(f"🟢 *涨幅 TOP {len(top_gainers)}*")
    for i, s in enumerate(top_gainers, 1):
        chg = s["chg24h"]
        arrow = "🟢" if chg > 0 else "🔴"
        vol_str = f"{s['vol_usd']/1_000_000:.1f}M" if s['vol_usd'] >= 1_000_000 else f"{s['vol_usd']/1000:.0f}K"
        lines.append(
            f"{i}. {arrow} *${s['sym']}*  `{chg:+.2f}%`  "
            f"${s['px']}  |  {vol_str}"
        )
    lines.append("")

    # 跌幅榜
    lines.append(f"🔴 *跌幅 TOP {len(top_losers)}*")
    for i, s in enumerate(top_losers, 1):
        chg = s["chg24h"]
        arrow = "🔴" if chg < 0 else "🟢"
        vol_str = f"{s['vol_usd']/1_000_000:.1f}M" if s['vol_usd'] >= 1_000_000 else f"{s['vol_usd']/1000:.0f}K"
        lines.append(
            f"{i}. {arrow} *${s['sym']}*  `{chg:+.2f}%`  "
            f"${s['px']}  |  {vol_str}"
        )
    lines.append("")

    # 单独异动告警 (涨跌幅超过阈值但没进前十的)
    lines.append(f"⚡ *异动关注* (|chg24h| > {CHG_ALERT_THRESHOLD}%)")
    # 涨的
    alerts = [s for s in top_gainers[TOP_N:] if abs(s["chg24h"]) >= CHG_ALERT_THRESHOLD]
    alerts += [s for s in top_losers[TOP_N:] if abs(s["chg24h"]) >= CHG_ALERT_THRESHOLD]
    if alerts:
        for s in alerts[:5]:
            lines.append(f"  • ${s['sym']}  `{s['chg24h']:+.2f}%`  ${s['px']}")
    else:
        lines.append("  (无)")

    lines.append("")
    lines.append(f"🤖 BWE 价格监控 | 更新: {ts}")
    return "\n".join(lines)


def send_tg(text: str, dry_run: bool = False):
    """发送消息到 Telegram。"""
    if dry_run:
        print(text)
        return True

    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        logger.warning("TG_BOT_TOKEN / TG_CHAT_ID 未配置")
        return False

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id": TG_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }, timeout=15)
        if resp.status_code == 200:
            logger.info("TG 推送成功")
            return True
        else:
            logger.warning(f"TG 推送失败: {resp.status_code} {resp.text[:200]}")
            return False
    except requests.RequestException as e:
        logger.warning(f"TG 请求异常: {e}")
        return False


def save_snapshot(all_data: dict, gainers: list, losers: list):
    """保存快照到文件，供历史比较用。"""
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, "price_mover_snapshot.json")
    snapshot = {
        "ts": int(_time.time()),
        "ts_human": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "total_count": len(all_data),
        "gainers": [{"sym": s["sym"], "chg24h": s["chg24h"], "px": s["px"], "vol_usd": s["vol_usd"]} for s in gainers],
        "losers": [{"sym": s["sym"], "chg24h": s["chg24h"], "px": s["px"], "vol_usd": s["vol_usd"]} for s in losers],
    }
    with open(path, "w") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    logger.info(f"快照已保存: {path}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="涨跌幅异动监控")
    parser.add_argument("--top", type=int, default=TOP_N, help=f"涨跌榜显示数量 (默认 {TOP_N})")
    parser.add_argument("--dry", action="store_true", help="仅打印，不发 TG")
    parser.add_argument("--min-vol", type=float, default=MIN_VOL_USDT, help=f"最低成交额 (默认 {MIN_VOL_USDT})")
    args = parser.parse_args()

    t0 = _time.monotonic()

    # ── 获取行情 ──
    tickers = okx_tickers(min_vol=args.min_vol)
    if not tickers:
        logger.error("❌ 无法获取行情数据")
        sys.exit(1)

    # ── 排序 ──
    sorted_gainers = sorted(tickers.items(), key=lambda x: -x[1]["chg24h"])
    sorted_losers = sorted(tickers.items(), key=lambda x: x[1]["chg24h"])

    # 构建完整列表（含 rank 信息）
    def build_list(items, is_gainer: bool):
        result = []
        for sym, data in items:
            result.append({
                "sym": sym,
                "chg24h": data["chg24h"],
                "px": data["px"],
                "vol_usd": data["vol_usd"],
                "rank": len(result) + 1,
            })
        return result

    gainers_full = build_list(sorted_gainers, True)
    losers_full = build_list(sorted_losers, False)

    top_n = args.top
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # ── 格式化 ──
    text = format_tg_message(gainers_full[:top_n], losers_full[:top_n], ts)

    # ── 发送 ──
    if args.dry:
        print(text)
    else:
        send_tg(text)

    # ── 保存快照 ──
    save_snapshot(tickers, gainers_full[:top_n], losers_full[:top_n])

    cost = _time.monotonic() - t0
    logger.info(f"完成 ({cost:.1f}s)")
    print(f"\n(运行耗时 {cost:.1f}s)")


if __name__ == "__main__":
    main()
