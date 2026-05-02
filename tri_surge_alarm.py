#!/usr/bin/env python3
"""
三源合约冲榜预警 — Tri-Exchange Surge Alarm
============================================
聚合 OKX + Gate.io + MEXC 三大合约交易所数据。

数据获取策略:
  - OKX:    批量 tickers (all-in-one, 含5m K线) + 批量 OI (all-in-one)
  - Gate:   批量 tickers (all-in-one, 含OI+费率)
  - MEXC:   批量 tickers (all-in-one, 含OI+费率+多周期涨跌)
  - 5m K线: 仅 OKX 一条批量请求 (其他所的5m用ticker自带数据估算)

四级过滤:
  1️⃣ 准入: 24h涨幅≥阈值 且 成交量≥阈值
  2️⃣ 动能: 5m/15m 加速度 + 排名跃迁
  3️⃣ 持仓: OI 可视化 (多源)
  4️⃣ 风险: 资金费率检查

推送模板:
🚨 【合约冲榜预警】
币种： $SYMBOL (源)
当前涨幅： +X.X% (排名：#N)
异动： X分钟内排名从 #M 杀入 #N
成交量： 15m成交额 X万 USDT (放量 X倍)
持仓量(OI)： +X.XM (多源)
资金费率： X.XX% (健康)
操作提示： XXXXXXXXX

运行:
  python3 tri_surge_alarm.py              # 完整运行+推送
  python3 tri_surge_alarm.py --dry        # 预览不发
  python3 tri_surge_alarm.py --top 5      # 只看前5名
  python3 tri_surge_alarm.py --chg 5.0    # 涨幅阈值提高到5%
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

# ── 加载 .env ─────────────────────────────────────────
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# ── 配置 ─────────────────────────────────────────────
TG_BOT_TOKEN = os.getenv("SIGNAL_BOT_TOKEN", os.getenv("TG_BOT_TOKEN", os.getenv("TELEGRAM_BOT_TOKEN", "")))
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")
DATA_DIR = os.getenv("DATA_DIR", "/root/binance-square-agent/data")

MIN_VOL_USDT = 1_000_000        # 最低24h美元成交额
TOP_N = 3                       # 预警TOP N
CHG_THRESHOLD = 3.0             # 24h涨跌幅最低阈值%
FUNDING_WARN = 0.0003           # 资金费率预警阈值

# ── 日志 ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("tri_surge_alarm")

# ── API 端点 ─────────────────────────────────────────
OKX_API = "https://www.okx.com"
GATE_API = "https://api.gateio.ws/api/v4"
MEXC_API = "https://contract.mexc.com"


# ═══════════════════════════════════════════════════════
#  1. 各交易所行情抓取
# ═══════════════════════════════════════════════════════


def fetch_okx(min_vol: float = MIN_VOL_USDT) -> dict:
    """OKX USDT-SWAP — 全市场 tickers + 批量 OI + 5m K线(一条请求含所有)。"""
    # ── 全市场 tickers ──
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
        raw_tickers = data.get("data", [])
    except requests.RequestException as e:
        logger.warning(f"OKX tickers 失败: {e}")
        return {}

    # ── 批量 OI ──
    oi_map = {}
    try:
        resp_oi = requests.get(
            f"{OKX_API}/api/v5/public/open-interest",
            params={"instType": "SWAP"},
            timeout=15,
        )
        if resp_oi.status_code == 200:
            oi_data = resp_oi.json()
            if oi_data.get("code") == "0":
                for t in oi_data.get("data", []):
                    oi_map[t["instId"]] = float(t.get("oiUsd", 0))
    except Exception:
        pass

    # ── 5m K线 (批量获取 — 一条请求含全市场K线不算, 需按instId)
    # OKX 不支持全市场K线, 只对Top200热门标的查
    ticker_syms = {}
    for t in raw_tickers:
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
            ticker_syms[inst_id] = {
                "sym": sym,
                "last": last,
                "chg24h": chg24h,
                "vol_usd": vol_usd,
            }
        except (ValueError, TypeError, KeyError):
            continue

    # 只查 Top 100 热门的 K 线
    sorted_ts = sorted(ticker_syms.items(), key=lambda x: -x[1]["vol_usd"])[:100]
    kline_cache = {}
    for inst_id, _ in sorted_ts:
        try:
            resp_k = requests.get(
                f"{OKX_API}/api/v5/market/candles",
                params={"instId": inst_id, "bar": "5m", "limit": 4},
                timeout=8,
            )
            if resp_k.status_code == 200:
                kd = resp_k.json()
                if kd.get("code") == "0":
                    klines = kd.get("data", [])
                    if len(klines) >= 2:
                        kline_cache[inst_id] = klines
        except Exception:
            pass

    # 构建结果
    result = {}
    for inst_id, info in ticker_syms.items():
        sym = info["sym"]
        last = info["last"]
        chg24h = info["chg24h"]
        vol_usd = info["vol_usd"]

        change_5m = 0.0
        change_15m = 0.0
        vol_15m = 0.0
        klines = kline_cache.get(inst_id, [])
        if len(klines) >= 2:
            c5o = float(klines[-2][1])
            c5c = float(klines[0][4])
            change_5m = ((c5c - c5o) / c5o * 100) if c5o else 0.0
            if len(klines) >= 4:
                k15o = float(klines[-4][1])
                change_15m = ((c5c - k15o) / k15o * 100) if k15o else 0.0
            for k in klines[:3]:
                vol_15m += float(k[5]) * float(klines[0][4])

        result[sym] = {
            "sym": sym,
            "chg24h": round(chg24h, 2),
            "change_5m": round(change_5m, 2),
            "change_15m": round(change_15m, 2),
            "vol_15m": vol_15m,
            "px": last,
            "vol_usd": int(vol_usd),
            "oi": oi_map.get(inst_id, 0),
            "funding_rate": 0.0,
            "source": "OKX",
        }

    logger.info(f"  OKX: {len(result)} symbols (kline_top={len(kline_cache)})")
    return result


def fetch_gate(min_vol: float = MIN_VOL_USDT) -> dict:
    """Gate.io USDT 永续合约 — 一条ticker内包含OI+费率+成交量。"""
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
            # OI = total_size * quanto_multiplier
            oi = float(t.get("total_size", 0)) * float(t.get("quanto_multiplier", 1)) * last
            funding_rate = float(t.get("funding_rate", 0))

            # Gate 没有 5m/15m 涨跌幅在ticker内, 用24h百分比近似估算
            # 同方向但保守
            change_5m = change_pct * 0.02  # 5m占24h约2%
            change_15m = change_pct * 0.05

            result[sym] = {
                "sym": sym,
                "chg24h": round(change_pct, 2),
                "change_5m": round(change_5m, 2),
                "change_15m": round(change_15m, 2),
                "vol_15m": vol_quote / 96.0,  # 平均15m成交量
                "px": last,
                "vol_usd": int(vol_quote),
                "oi": oi,
                "funding_rate": funding_rate,
                "source": "Gate",
            }
        except (ValueError, TypeError, KeyError):
            continue

    logger.info(f"  Gate: {len(result)} symbols")
    return result


def fetch_mexc(min_vol: float = MIN_VOL_USDT) -> dict:
    """MEXC 永续合约 — ticker内含OI+费率+多周期涨跌幅。"""
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
            chg24h = round(change * 100, 2)
            if vol_usd < min_vol:
                continue
            oi = float(t.get("holdVol", 0)) * last
            funding_rate = float(t.get("fundingRate", 0))

            # MEXC riseFallRates 有 r7(7d)/r30/r90, 无5m
            # 用24h涨跌幅的方向作为短期方向估算
            change_5m = chg24h * 0.02
            change_15m = chg24h * 0.05

            result[sym] = {
                "sym": sym,
                "chg24h": chg24h,
                "change_5m": round(change_5m, 2),
                "change_15m": round(change_15m, 2),
                "vol_15m": vol_usd / 96.0,
                "px": last,
                "vol_usd": int(vol_usd),
                "oi": oi,
                "funding_rate": funding_rate,
                "source": "MEXC",
            }
        except (ValueError, TypeError, KeyError):
            continue

    logger.info(f"  MEXC: {len(result)} symbols")
    return result


# ═══════════════════════════════════════════════════════
#  2. 信号聚合 & 四级过滤
# ═══════════════════════════════════════════════════════


def aggregate(all_exchange_data: dict) -> dict:
    """三源聚合: 同币种取均值/最优值。"""
    merged = {}
    for exchange_name, tickers in all_exchange_data.items():
        for sym, data in tickers.items():
            if sym not in merged:
                merged[sym] = {
                    "sym": sym,
                    "chg24h": 0.0,
                    "change_5m": 0.0,
                    "change_15m": 0.0,
                    "px": 0.0,
                    "vol_usd": 0,
                    "vol_15m": 0.0,
                    "oi": 0.0,
                    "funding_rate": 0.0,
                    "exchanges": [],
                }
            m = merged[sym]
            m["exchanges"].append(exchange_name)
            count = len(m["exchanges"])
            m["chg24h"] = (m["chg24h"] * (count - 1) + data["chg24h"]) / count
            m["change_5m"] = (m["change_5m"] * (count - 1) + data["change_5m"]) / count
            m["change_15m"] = (m["change_15m"] * (count - 1) + data["change_15m"]) / count
            m["px"] = data["px"]
            m["vol_usd"] = max(m["vol_usd"], data["vol_usd"])
            m["vol_15m"] += data["vol_15m"]
            m["oi"] += data["oi"]
            m["funding_rate"] = (m["funding_rate"] * (count - 1) + data["funding_rate"]) / count

    for sym in merged:
        m = merged[sym]
        m["chg24h"] = round(m["chg24h"], 2)
        m["change_5m"] = round(m["change_5m"], 2)
        m["change_15m"] = round(m["change_15m"], 2)
        m["funding_rate"] = round(m["funding_rate"], 6)

    return merged


def load_previous_snapshot() -> dict:
    """加载上一次排名快照，用于排名跃迁检测。"""
    path = os.path.join(DATA_DIR, "tri_surge_snapshot.json")
    try:
        with open(path) as f:
            data = json.load(f)
        return data.get("rankings", {})
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return {}


def save_snapshot(merged: dict, rankings: dict, ts_str: str):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, "tri_surge_snapshot.json")
    snapshot = {
        "ts": int(_time.time()),
        "ts_human": ts_str,
        "count": len(merged),
        "rankings": rankings,
        "symbols": [
            {"sym": s["sym"], "chg24h": s["chg24h"], "chg5m": s["change_5m"],
             "px": s["px"], "vol": s["vol_usd"], "oi": int(s["oi"]),
             "fr": s["funding_rate"], "ex": s["exchanges"]}
            for s in sorted(merged.values(), key=lambda x: -abs(x["chg24h"]))[:100]
        ],
    }
    with open(path, "w") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    logger.info(f"快照: {path}")


def generate_alerts(merged: dict, prev_rankings: dict, top_n: int = TOP_N) -> list:
    """四级过滤 → 格式化预警。"""
    sorted_list = sorted(merged.values(), key=lambda x: -x["chg24h"])
    current_rankings = {s["sym"]: i + 1 for i, s in enumerate(sorted_list)}
    alerts = []

    for rank, s in enumerate(sorted_list[:top_n], 1):
        sym = s["sym"]
        prev_rank = prev_rankings.get(sym)

        # ── 动能指标 ──
        change_5m = s["change_5m"]
        change_15m = s["change_15m"]

        # ── 排名跃迁 ──
        rank_jump = 0
        if prev_rank is not None:
            rank_jump = prev_rank - rank
        is_new_entry = prev_rank is None

        # ── 成交量倍率 ──
        vol_15m_avg = s["vol_usd"] / 96.0 if s["vol_usd"] else 1
        vol_ratio = (s["vol_15m"] / vol_15m_avg) if vol_15m_avg else 0

        # ── OI状态 ──
        oi_usd = s["oi"]

        # ── 综合评分 ──
        score = 0
        score += min(s["chg24h"] / 2, 10)
        score += min(change_5m * 2, 8)
        score += min(rank_jump * 0.5, 5) if rank_jump > 0 else 0
        score += min(vol_ratio * 2, 5)
        score += 3 if is_new_entry else 0

        # ── 资金费率状态 ──
        fr = s["funding_rate"]
        if fr >= FUNDING_WARN:
            fr_status = "⚠️ 偏高"
        elif fr >= FUNDING_WARN * 0.5:
            fr_status = "稍高"
        elif fr <= -FUNDING_WARN:
            fr_status = "🔴 负费率(空头踩踏)"
        else:
            fr_status = "✅ 健康"

        # ── 成交量状态 ──
        if vol_ratio >= 5:
            vol_status = f"爆量 {vol_ratio:.1f}x"
        elif vol_ratio >= 2:
            vol_status = f"放量 {vol_ratio:.1f}x"
        else:
            vol_status = f"{vol_ratio:.1f}x"

        # ── 操作提示 ──
        hints = []
        if change_5m >= 1:
            hints.append("5m动能强劲")
        if change_15m >= 2:
            hints.append("15m趋势向上")
        if vol_ratio >= 2:
            hints.append("量能支持")
        if 0 < fr < FUNDING_WARN:
            hints.append("资金费率合理")
        if rank_jump > 0:
            hints.append(f"排名上升{abs(rank_jump)}位")
        if is_new_entry:
            hints.append("新入榜标的")

        if len(hints) >= 3:
            action_hint = "🚀 动能强劲，关注回调机会"
        elif len(hints) >= 1:
            action_hint = "📌 持续观察，等确认信号"
        else:
            action_hint = "⚠️ 需进一步确认"

        # ── 构建消息 ──
        ex_str = "+".join(s["exchanges"]).upper()
        rank_str = f"#{rank}"
        if rank_jump > 0:
            rank_str += f" 🔼{abs(rank_jump)}"
        elif rank_jump < 0:
            rank_str += f" 🔻{abs(rank_jump)}"

        anomaly_parts = []
        if is_new_entry:
            anomaly_parts.append("新入榜🔥")
        if rank_jump >= 3:
            anomaly_parts.append(f"排名跃迁{abs(rank_jump)}位")
        if vol_ratio >= 3:
            anomaly_parts.append("成交量爆增")
        anomaly_str = " | ".join(anomaly_parts) if anomaly_parts else "常规上涨"

        vol_15m_str = f"{s['vol_15m']/1_000_000:.2f}M" if s['vol_15m'] >= 1_000_000 else f"{s['vol_15m']/1000:.0f}K"
        oi_str = f"{oi_usd/1_000_000:.2f}M" if oi_usd >= 1_000_000 else f"{oi_usd/1000:.0f}K"

        msg = (
            f"🚨 *合约冲榜预警*\n"
            f"币种： *${sym}* ({ex_str})\n"
            f"当前涨幅： `{s['chg24h']:+.2f}%` (排名：{rank_str})\n"
            f"异动： {anomaly_str}\n"
            f"成交量： 15m成交额 {vol_15m_str} ({vol_status})\n"
            f"持仓量(OI)： {oi_str} (多源)\n"
            f"资金费率： {fr*100:.4f}% ({fr_status})\n"
            f"操作提示： {action_hint}"
        )

        alerts.append({
            "sym": sym,
            "score": score,
            "msg": msg,
            "chg24h": s["chg24h"],
            "change_5m": change_5m,
            "vol_ratio": vol_ratio,
            "rank_jump": rank_jump,
            "is_new_entry": is_new_entry,
        })

    alerts.sort(key=lambda x: -x["score"])
    return alerts, current_rankings


# ═══════════════════════════════════════════════════════
#  3. 推送
# ═══════════════════════════════════════════════════════


def send_tg(text: str, dry_run: bool = False) -> bool:
    if dry_run:
        print(f"\n{'='*50}")
        print(text)
        print(f"{'='*50}")
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


# ═══════════════════════════════════════════════════════
#  4. 主逻辑
# ═══════════════════════════════════════════════════════


def main():
    import argparse
    parser = argparse.ArgumentParser(description="三源合约冲榜预警")
    parser.add_argument("--top", type=int, default=TOP_N)
    parser.add_argument("--dry", action="store_true")
    parser.add_argument("--min-vol", type=float, default=MIN_VOL_USDT)
    parser.add_argument("--chg", type=float, default=CHG_THRESHOLD)
    args = parser.parse_args()

    min_vol = args.min_vol
    chg_threshold = args.chg
    top_n = args.top

    t0 = _time.monotonic()
    ts = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M UTC+8")

    logger.info(f"🚀 三源合约冲榜预警启动")
    logger.info(f"阈值: min_vol={min_vol/1000:.0f}K, chg≥{chg_threshold}%, top={top_n}")

    # ── 并行抓取 ──
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as exe:
        fut_okx = exe.submit(fetch_okx, min_vol)
        fut_gate = exe.submit(fetch_gate, min_vol)
        fut_mexc = exe.submit(fetch_mexc, min_vol)

        okx_data = fut_okx.result()
        gate_data = fut_gate.result()
        mexc_data = fut_mexc.result()

    total_raw = len(okx_data) + len(gate_data) + len(mexc_data)
    logger.info(f"原始数据: OKX={len(okx_data)}, Gate={len(gate_data)}, MEXC={len(mexc_data)} (共{total_raw})")

    # ── 聚合 ──
    merged = aggregate({"okx": okx_data, "gate": gate_data, "mexc": mexc_data})
    logger.info(f"聚合后: {len(merged)} 个独立标的")

    # ── 24h涨幅阈值过滤 ──
    merged = {sym: data for sym, data in merged.items() if abs(data["chg24h"]) >= chg_threshold}
    logger.info(f"阈值过滤(≥{chg_threshold}%): {len(merged)} 个")

    # ── 加载上一次排名 ──
    prev_rankings = load_previous_snapshot()
    logger.info(f"上次快照: {len(prev_rankings)} 个排名记录")

    # ── 生成预警 ──
    alerts, current_rankings = generate_alerts(merged, prev_rankings, top_n)
    logger.info(f"生成预警: {len(alerts)} 条")

    # ── 推送 ──
    if not alerts:
        msg = f"📊 *三源合约冲榜监控* · {ts}\n\n暂无符合条件的冲榜信号。"
        send_tg(msg, args.dry)
        logger.info("无预警信号")
    else:
        for i, alert in enumerate(alerts, 1):
            send_tg(alert["msg"], args.dry)
            logger.info(f"  预警 #{i}: ${alert['sym']} (score={alert['score']:.1f})")

    # ── 保存快照 ──
    save_snapshot(merged, current_rankings, ts)

    cost = _time.monotonic() - t0
    logger.info(f"✅ 完成 ({cost:.1f}s)")
    print(f"\n运行耗时 {cost:.1f}s | 聚合 {len(merged)} 标的 | 预警 {len(alerts)} 条")


if __name__ == "__main__":
    main()
