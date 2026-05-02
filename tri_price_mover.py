#!/usr/bin/env python3
"""
三源合涨跌幅异动监控 — Tri-Exchange Price Mover
=================================================
聚合 OKX + Gate.io + MEXC 三大合约交易所的 USDT 永续合约涨跌幅数据，
输出综合涨跌榜，发现跨所共振异动。

数据源:
  - OKX:    /api/v5/market/tickers (instType=SWAP) — 全市场
  - Gate.io: /api/v4/futures/usdt/tickers — 全市场
  - MEXC:   /api/v1/contract/ticker — 全市场

运行:
  python3 tri_price_mover.py              # 正常推送
  python3 tri_price_mover.py --dry        # 预览不发
  python3 tri_price_mover.py --top 10     # TOP 10
  python3 tri_price_mover.py --min-vol 500_000  # 提高成交量门槛
"""

import os
import sys
import json
import time as _time
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from dotenv import load_dotenv

import requests

load_dotenv("/root/binance-square-agent/.env")

# ── 配置 ─────────────────────────────────────────────
TG_BOT_TOKEN = os.getenv("SIGNAL_BOT_TOKEN", os.getenv("TG_BOT_TOKEN", ""))
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")
DATA_DIR = os.getenv("DATA_DIR", "/root/binance-square-agent/data")

MIN_VOL_USDT = 300_000        # 最低24h美元成交额
TOP_N = 10                    # 涨跌榜显示数量
CONSENSUS_MIN = 2             # 最少几个交易所一致才算共振信号

# ── 日志 ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("tri_price_mover")

# ── API 端点 ─────────────────────────────────────────
OKX_API = "https://www.okx.com"
GATE_API = "https://api.gateio.ws/api/v4"
MEXC_API = "https://contract.mexc.com"


# ═══════════════════════════════════════════════════════
#  1. 各交易所行情抓取
# ═══════════════════════════════════════════════════════


def fetch_okx(min_vol: float = MIN_VOL_USDT) -> dict:
    """OKX USDT-SWAP 全市场 tickers。"""
    try:
        resp = requests.get(
            f"{OKX_API}/api/v5/market/tickers",
            params={"instType": "SWAP"},
            timeout=15,
        )
        if resp.status_code != 200:
            return {}
        data = resp.json()
        if data.get("code") != "0":
            return {}
    except requests.RequestException as e:
        logger.warning(f"OKX 失败: {e}")
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
                "sym": sym,
                "chg24h": round(chg24h, 2),
                "px": last,
                "vol_usd": vol_usd,
                "source": "OKX",
            }
        except (ValueError, TypeError, KeyError):
            continue

    logger.info(f"  OKX: {len(result)} symbols")
    return result


def fetch_gate(min_vol: float = MIN_VOL_USDT) -> dict:
    """Gate.io USDT 永续合约全市场 tickers。"""
    try:
        resp = requests.get(
            f"{GATE_API}/futures/usdt/tickers",
            timeout=15,
        )
        if resp.status_code != 200:
            return {}
        raw = resp.json()
    except requests.RequestException as e:
        logger.warning(f"Gate.io 失败: {e}")
        return {}

    result = {}
    for t in raw:
        contract = t.get("contract", "")
        if not contract.endswith("_USDT"):
            continue
        try:
            sym = contract.replace("_USDT", "USDT")
            last = float(t["last"])
            change_pct = float(t.get("change_percentage", 0))
            vol_quote = float(t.get("volume_24h_quote", 0))
            if vol_quote < min_vol:
                continue
            result[sym] = {
                "sym": sym,
                "chg24h": round(change_pct, 2),
                "px": last,
                "vol_usd": vol_quote,
                "source": "Gate",
            }
        except (ValueError, TypeError, KeyError):
            continue

    logger.info(f"  Gate: {len(result)} symbols")
    return result


def fetch_mexc(min_vol: float = MIN_VOL_USDT) -> dict:
    """MEXC 永续合约全市场 tickers。"""
    try:
        resp = requests.get(
            f"{MEXC_API}/api/v1/contract/ticker",
            timeout=15,
        )
        if resp.status_code != 200:
            return {}
        data = resp.json()
        if not data.get("success"):
            return {}
        raw = data.get("data", [])
    except (requests.RequestException, ValueError) as e:
        logger.warning(f"MEXC 失败: {e}")
        return {}

    result = {}
    for t in raw:
        try:
            sym = t.get("symbol", "").replace("_", "")
            if not sym.endswith("USDT"):
                continue
            last = float(t.get("lastPrice", 0))
            change = float(t.get("riseFallRate", 0))
            vol_usd = float(t.get("volume24", 0)) * last if t.get("volume24") else 0
            if vol_usd < min_vol:
                continue
            result[sym] = {
                "sym": sym,
                "chg24h": round(change * 100, 2),  # MEXC 返回的是比率(0.01=1%)
                "px": last,
                "vol_usd": vol_usd,
                "source": "MEXC",
            }
        except (ValueError, TypeError, KeyError):
            continue

    logger.info(f"  MEXC: {len(result)} symbols")
    return result


# ═══════════════════════════════════════════════════════
#  2. 信号聚合
# ═══════════════════════════════════════════════════════


def aggregate(all_exchange_data: dict) -> dict:
    """
    all_exchange_data: {"okx": {...}, "gate": {...}, "mexc": {...}}
    
    Returns:
        {
            "merged": {sym: {avg_chg, sources_count, exchanges, details}}
            "resonance": [共振信号列表]
        }
    """
    merged = {}

    for exchange_name, tickers in all_exchange_data.items():
        for sym, data in tickers.items():
            if sym not in merged:
                merged[sym] = {
                    "chgs": {},
                    "vol_usd_max": 0,
                    "min_vol_usd": float("inf"),
                    "px_latest": 0,
                }
            merged[sym]["chgs"][exchange_name] = data["chg24h"]
            merged[sym]["vol_usd_max"] = max(merged[sym]["vol_usd_max"], data["vol_usd"])
            merged[sym]["min_vol_usd"] = min(merged[sym]["min_vol_usd"], data["vol_usd"])
            merged[sym]["px_latest"] = data["px"]

    # 计算聚合指标
    final = {}
    for sym, m in merged.items():
        exchanges = list(m["chgs"].keys())
        num_exchanges = len(exchanges)
        avg_chg = sum(m["chgs"].values()) / num_exchanges

        # 跨所一致性: 最大涨跌幅差
        chg_values = list(m["chgs"].values())
        max_spread = max(chg_values) - min(chg_values)

        # 用最小成交量作为保守估计
        vol_est = m["min_vol_usd"]

        final[sym] = {
            "sym": sym,
            "avg_chg24h": round(avg_chg, 2),
            "exchanges": exchanges,
            "exchanges_count": num_exchanges,
            "details": {ex: m["chgs"][ex] for ex in exchanges},
            "spread": round(max_spread, 2),
            "vol_usd_est": int(vol_est),
            "px": m["px_latest"],
            "consensus": num_exchanges >= CONSENSUS_MIN,
        }

    return final


def find_resonance(final: dict, chg_threshold: float = 3.0) -> list:
    """
    寻找跨所共振信号:
      - 至少 CONSENSUS_MIN 个交易所数据一致
      - |avg_chg24h| >= chg_threshold
      - 跨所价差 spread 不过大 (< 2%)
    """
    signals = []
    for sym, data in final.items():
        if not data["consensus"]:
            continue
        if abs(data["avg_chg24h"]) < chg_threshold:
            continue
        signals.append(data)

    # 按 |avg_chg24h| 排序
    signals.sort(key=lambda x: -abs(x["avg_chg24h"]))
    return signals


# ═══════════════════════════════════════════════════════
#  3. 格式化 & 推送
# ═══════════════════════════════════════════════════════


def format_tg(gainers: list, losers: list, resonance: list, ts: str) -> str:
    """
    电报推送格式化 — 精美排版设计。

    设计原则：
    - 分层清晰：标题→摘要→亮点→表格→尾注
    - 图标化：涨跌分色、量级图标、数据源标识
    - 信息密度高但可扫描：`编号. 币种 涨幅% 价格 成交量 来源`
    - 尾注留系统信息
    """

    def fmt_price(px: float) -> str:
        """智能价格格式化：>=1显示2位小数，<1显示4位，<0.01显示6位"""
        if px >= 100:
            return f"${px:,.2f}"
        elif px >= 1:
            return f"${px:.4f}"
        elif px >= 0.01:
            return f"${px:.6f}"
        else:
            return f"${px:.8f}"

    def fmt_vol(vol: float) -> str:
        """成交量智能缩写"""
        if vol >= 10_000_000:
            return f"{vol/1_000_000:.1f}M"
        elif vol >= 1_000_000:
            return f"{vol/1_000_000:.2f}M"
        elif vol >= 1_000:
            return f"{vol/1_000:.0f}K"
        else:
            return f"{vol:.0f}"

    def fmt_chg(chg: float) -> str:
        """涨跌幅格式：>=10%加粗，正负分色"""
        if abs(chg) >= 10:
            return f"**{chg:+.2f}%**"
        return f"{chg:+.2f}%"

    def fmt_exchanges(exs: list) -> str:
        """交易所标识emoji"""
        mapping = {"okx": "🅾️", "gate": "🅶", "mexc": "🅼"}
        return " ".join(mapping.get(e, e.upper()) for e in exs)

    def render_row(i: int, s: dict, is_gainer: bool) -> str:
        """渲染单行"""
        if is_gainer:
            arrow = "🟢"
            tag = ""
        else:
            arrow = "🔴"
            tag = " ⚠️"

        vol = fmt_vol(s["vol_usd_est"])
        px = fmt_price(s["px"])
        ex = fmt_exchanges(s.get("exchanges", ["——"]))
        chg = fmt_chg(s["avg_chg24h"])

        # 共识徽章
        badge = ""
        if s.get("consensus", False) and s["exchanges_count"] >= 3:
            badge = " 🌟三所共振"
        elif s.get("consensus", False) and s["exchanges_count"] >= 2:
            badge = " 🔗双所共振"

        return f"`{i:>2}.` {arrow} **{s['sym']}** {chg:>8}  {px:<12} {vol:>6}  {ex}{badge}{tag}"

    lines = []

    # ── 头部 ──────────────────────────────────────────
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("📊 **三源合涨跌幅异动 | Tri-Price Mover**")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"🕐 `{ts}`")
    lines.append(f"📡 OKX + Gate.io + MEXC | 24h涨跌 | ≥{MIN_VOL_USDT/1000:.0f}K USD")
    lines.append("")

    # ── 标杆摘要栏 ────────────────────────────────────
    # 找出最大涨、最大跌、最大成交量
    max_gainer = max(gainers, key=lambda x: x["avg_chg24h"]) if gainers else None
    max_loser = min(losers, key=lambda x: x["avg_chg24h"]) if losers else None
    all_sorted = gainers + losers
    max_vol_sym = max(all_sorted, key=lambda x: x["vol_usd_est"]) if all_sorted else None

    summary_parts = []
    if max_gainer:
        g_chg = fmt_chg(max_gainer["avg_chg24h"])
        summary_parts.append(f"🟢 最大涨 **{max_gainer['sym']}** {g_chg}")
    if max_loser:
        l_chg = fmt_chg(max_loser["avg_chg24h"])
        if l_chg == max_gainer["avg_chg24h"]:
            pass
        summary_parts.append(f"🔴 最大跌 **{max_loser['sym']}** {l_chg}")
    if max_vol_sym:
        summary_parts.append(f"💎 最活跃 **{max_vol_sym['sym']}** {fmt_vol(max_vol_sym['vol_usd_est'])}")
    if summary_parts:
        lines.append("  ⚡ 标杆 | " + "  ·  ".join(summary_parts))
        lines.append("")

    # ── 跨所共振榜 ─────────────────────────────────
    if resonance:
        lines.append("━━━━ ⚡ **跨所共振信号** ━━━━━")
        for i, s in enumerate(resonance[:8], 1):
            lines.append(render_row(i, s, s["avg_chg24h"] > 0))
        lines.append("")

    # ── 涨幅榜 ──────────────────────────────────────
    lines.append("━━━━ 🟢 **综合涨幅榜 TOP 10** ━━━━━")
    lines.append("` #  币种        涨幅     价           成交量   来源`")
    for i, s in enumerate(gainers[:10], 1):
        lines.append(render_row(i, s, True))
    lines.append("")

    # ── 跌幅榜 ──────────────────────────────────────
    lines.append("━━━━ 🔴 **综合跌幅榜 TOP 10** ━━━━━")
    lines.append("` #  币种        跌幅     价           成交量   来源`")
    for i, s in enumerate(losers[:10], 1):
        lines.append(render_row(i, s, False))
    lines.append("")

    # ── 尾注 ────────────────────────────────────────
    # 统计
    total_triple = sum(1 for s in all_sorted if s.get("consensus", False) and s["exchanges_count"] >= 3)
    total_dual = sum(1 for s in all_sorted if s.get("consensus", False) and s["exchanges_count"] == 2)

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    footer_parts = []
    footer_parts.append(f"🔎 共监测 {len(all_sorted)} 个标的" if all_sorted else "🔎 无合格标的")
    if total_triple > 0:
        footer_parts.append(f"🌟 三所共振 {total_triple} 个")
    if total_dual > 0:
        footer_parts.append(f"🔗 双所共振 {total_dual} 个")
    lines.append("  " + "  |  ".join(footer_parts))

    lines.append("  🤖 BWE Tri-Price Monitor | 每60分钟自动更新")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    return "\n".join(lines)


def send_tg(text: str, dry_run: bool = False):
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


def save_snapshot(data: dict, ts_str: str):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, "tri_price_snapshot.json")
    snapshot = {
        "ts": int(_time.time()),
        "ts_human": ts_str,
        "count": len(data),
        "symbols": [
            {
                "sym": s["sym"],
                "avg_chg": s["avg_chg24h"],
                "px": s["px"],
                "vol": s["vol_usd_est"],
                "exchanges": s["exchanges"],
            }
            for s in sorted(data.values(), key=lambda x: -abs(x["avg_chg24h"]))[:100]
        ],
    }
    with open(path, "w") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    logger.info(f"快照: {path}")


# ═══════════════════════════════════════════════════════
#  4. 主逻辑
# ═══════════════════════════════════════════════════════


def main():
    import argparse
    parser = argparse.ArgumentParser(description="三源合涨跌幅异动监控")
    parser.add_argument("--top", type=int, default=TOP_N)
    parser.add_argument("--dry", action="store_true")
    parser.add_argument("--min-vol", type=float, default=MIN_VOL_USDT)
    parser.add_argument("--consensus", type=int, default=CONSENSUS_MIN)
    args = parser.parse_args()

    t0 = _time.monotonic()
    top_n = args.top
    ts = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M UTC+8")

    logger.info("🚀 三源合涨跌幅异动监控启动")
    logger.info(f"阈值: min_vol={args.min_vol/1000:.0f}K, consensus≥{args.consensus}, top={top_n}")

    # ── 并行获取三所数据 ──
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as exe:
        fut_okx = exe.submit(fetch_okx, args.min_vol)
        fut_gate = exe.submit(fetch_gate, args.min_vol)
        fut_mexc = exe.submit(fetch_mexc, args.min_vol)

        okx_data = fut_okx.result()
        gate_data = fut_gate.result()
        mexc_data = fut_mexc.result()

    total_raw = len(okx_data) + len(gate_data) + len(mexc_data)
    logger.info(f"原始数据: OKX={len(okx_data)}, Gate={len(gate_data)}, MEXC={len(mexc_data)} (共{total_raw})")

    # ── 聚合 ──
    merged = aggregate({"okx": okx_data, "gate": gate_data, "mexc": mexc_data})
    logger.info(f"聚合后: {len(merged)} 个独立标的")

    # ── 排序 ──
    sorted_all = sorted(merged.values(), key=lambda x: -x["avg_chg24h"])
    gainers = [s for s in sorted_all if s["avg_chg24h"] > 0][:top_n]
    losers = [s for s in sorted_all if s["avg_chg24h"] <= 0][-top_n:]
    losers.reverse()  # 最跌的排前面

    # ── 共振信号 ──
    resonance = find_resonance(merged, chg_threshold=3.0)
    # 排除已经在 gainers/losers 里的
    top_syms = {s["sym"] for s in gainers + losers}
    resonance_extra = [s for s in resonance if s["sym"] not in top_syms][:5]

    # ── 格式化 ──
    text = format_tg(gainers, losers, resonance_extra, ts)

    # ── 推送 ──
    if args.dry:
        print(text)
    else:
        # 分段推送（TG 消息有长度限制）
        if len(text) > 4000:
            # 太长就分开发
            send_tg(f"📊 *三源合涨跌幅异动* · {ts}\n\n🟢 涨幅榜\n" + "\n".join(
                [f"{i}. *${s['sym']}*  `{s['avg_chg24h']:+.2f}%`  ${s['px']}"
                 for i, s in enumerate(gainers, 1)]
            ), args.dry)
            send_tg("🔴 跌幅榜\n" + "\n".join(
                [f"{i}. *${s['sym']}*  `{s['avg_chg24h']:+.2f}%`  ${s['px']}"
                 for i, s in enumerate(losers, 1)]
            ), args.dry)
        else:
            send_tg(text, args.dry)

    # ── 保存 ──
    save_snapshot(merged, ts)

    cost = _time.monotonic() - t0
    logger.info(f"✅ 完成 ({cost:.1f}s)")
    print(f"\n运行耗时 {cost:.1f}s | 聚合 {len(merged)} 标的 | 共振 {len(resonance_extra)} 个")


if __name__ == "__main__":
    main()
