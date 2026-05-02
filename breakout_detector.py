#!/usr/bin/env python3
"""
Breakout Detector — 独立起涨点检测通道 (v2.2 — 复盘优化版)
===========================================================
独立于 scoring.py 的 MA88/EMA/OBV 前置条件，专门捕捉低价小市值币种
的突然放量爆拉起涨点。

八触发条件（任一满足即告警）:
  A: 常规起涨 — 单根5m K线涨幅>1.5%，按价格层级设量门槛(常规200K/低价20K/微市值5K)
  B: 放量拉升 — 成交量突增>2.5倍(共振1.5倍)且涨幅>1.0%
  C: 低位启动 — 距24h低点<5%但突然爆量>3倍(共振2倍)且涨幅>1.5%
  D: 累计拉升 — 连续2根5m K线每根涨>0.5%+最新根放量(捕捉慢热爬坡)
  E: 极端振幅 — 单根5m K线振幅>6%，拆分为E1下影线看涨/E2上影线看跌/E3宽幅震荡
  F: 暴力拉升 — 单根5m涨幅>5%(不设量门槛，极端行情捕获器)
  G: 蓄力突破 — 前3根缩量横盘(|chg|<1%)+最新根放量>1.5倍突破
  H: 突破前高 — 价格距24h高点<1%且量比>1.5倍确认突破

v2.2复盘优化重点 (2026-05-02):
  - 三价格层级量门槛: 微市值($<0.01)用5K/低价($0.01-0.3)用20K/常规用200K
  - 条件A阈值从3.0%降至1.5% — 正常行情也能触发
  - 新增F(暴力拉升) — 不设量门槛捕获极端行情
  - 新增G(横盘蓄力) — 缩量整理后放量突破的起爆前信号
  - 新增H(突破前高) — 价格逼近24h新高+量确认
  - 条件D从3根降为2根+最新根放量替代逐根递增检查
  - 条件E拆分上下影线: E1下影看涨/E2上影看跌/E3宽幅震荡
  - 量比计算用前3根中位数替代单根对比
  - 评分系统: 条件组合协同加分+量比趋势+24h涨跌幅细化+微市值α信号
  - 满分从100提至120，空扫描每次推送

低价币阈值自动降低: $0.3以下的币种成交量门槛降低，$0.01以下降至5K。
跨所共振: 同一币种在≥2家交易所(OKX/Gate/MEXC)出现，自动降低触发门槛+评分加成。

数据源: OKX + Gate + MEXC tickers (全市场) + OKX 5m K线

推送: 独立TG Bot + 日志
"""

import os
import sys
import json
import time as _time
import logging
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
import requests

# ── 加载 .env ──
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# ── 配置 ──
# 起涨检测专用的 Bot Token（用户新提供的）
BREAKOUT_BOT_TOKEN = "8647374599:AAFXlurnjt22uD1htgtdmUBrw-NKIwPgU-I"
BREAKOUT_CHAT_ID = "1077054086"

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")
DATA_DIR = os.getenv("DATA_DIR", "/root/binance-square-agent/data")

OKX_API = "https://www.okx.com"
BITGET_API = "https://api.bitget.com"
GATE_API = "https://api.gateio.ws"
MEXC_API = "https://contract.mexc.com"

# ── 阈值 - v2.2 复盘优化 ──
# 条件A: 常规起涨
SURGE_5M_MIN_PCT = 1.5            # 单根5m涨幅%（原3.0，降到1.5）
SURGE_MIN_VOL = 200_000           # 常规最低成交量USDT
SURGE_MIN_VOL_LOW_PRICE = 20_000  # 低价币($0.01-0.3)最低成交量（原80K，降到20K）
SURGE_MIN_VOL_MICRO = 5_000       # 微市值币($<0.01)最低成交量（新增5K）
LOW_PRICE_THRESHOLD = 0.3         # 低价币阈值
MICRO_PRICE_THRESHOLD = 0.01      # 微市值币阈值（新增）

# 条件B: 放量拉升
VOL_SPIKE_MIN_RATIO = 2.5         # 成交量突增倍数（原3.0，降到2.5）
VOL_SPIKE_MIN_CHG = 1.0           # 对应最小涨幅%（原1.5，降到1.0）

# 条件C: 低位启动
LOW_STARTUP_NEAR_LOW24_PCT = 5.0  # 距24h低点%（原3.0，放宽到5.0）
LOW_STARTUP_VOL_RATIO = 3.0       # 成交量突增倍数（原5.0，降到3.0）
LOW_STARTUP_MIN_CHG = 1.5         # 最小涨幅%（原2.0，降到1.5）

# 条件D: 累计拉升（v2.2紧缩化: 2根K线替代3根）
CUMULATIVE_RAMP_MIN_CANDLES = 2    # 连续K线根数（原3，降到2）
CUMULATIVE_RAMP_MIN_CHG = 0.5      # 每根最小涨幅%（原0.8，降到0.5）
CUMULATIVE_RAMP_MIN_VOL = 5_000    # 每根最低成交量USDT（原8K，降到5K）

# 条件E: 极端振幅（v2.2拆分上下影线）
EXTREME_WICK_MIN_RANGE = 6.0       # 单根K线最小振幅%（原8.0，降到6.0）
EXTREME_WICK_MIN_VOL_DIVISOR = 2   # 成交量门槛降为原值的1/N
LOWER_WICK_MIN_PCT = 3.0           # 下影线最小%(看涨信号，新增)
UPPER_WICK_MIN_PCT = 3.0           # 上影线最小%(看跌信号，新增)

# 条件F: 暴力拉升（新增 — 捕捉极端行情，不设量门槛）
SURGE_VIOLENT_MIN_PCT = 5.0        # 单根5m暴力涨幅%
SURGE_VIOLENT_MIN_VOL = 1_000      # 暴力拉升最低成交量（极低门槛）

# 条件G: 横盘蓄力（新增 — 连续缩量整理后放量启动）
ACCUMULATION_BARS = 3              # 蓄力观察K线数
ACCUMULATION_MAX_CHG = 1.0         # 蓄力期单根最大涨跌幅%
ACCUMULATION_MIN_VOL_RATIO = 1.5   # 最新K线成交量/蓄力期均量比

# 条件H: 突破前高（新增 — 价格突破24h新高+量确认）
BREAKOUT_24H_DIST = 1.0             # 距24h高点<=此百分比视为接近突破
BREAKOUT_MIN_VOL_RATIO = 1.5        # 突破时的成交量/前一根比

# 条件I: 缓涨起跑（v2.3新增 — 连续小阳线+量能递增爬坡，捕捉ORDI式起涨）
SLOW_RAMP_WINDOW = 8                # 检测窗口K线数
SLOW_RAMP_MIN_BULLISH = 5           # 至少5根收阳
SLOW_RAMP_ACCUM_MIN = 1.2           # 窗口累计涨幅>=1.2%
SLOW_RAMP_VOL_SLOPE_MIN = 0.1       # 成交量后半/前半比>=1.1
SLOW_RAMP_AVG_VOL_MIN = 60_000      # 窗口平均成交量USDT

# 新币首次检测动态量门槛
MIN_VOL_USDT_24H_NEW = 50_000      # 新币首次检测的低量门槛（替代200K）
NEW_COIN_SEEN_PATH = os.path.join(os.path.dirname(__file__), "data", "breakout_new_coin_seen.json")

# 低分信号池
LOW_SCORE_BREAKOUTS_PATH = os.path.join(os.path.dirname(__file__), "data", "low_score_breakouts.json")
LOW_SCORE_THRESHOLD = 50           # score<50的信号进低分池

# 风险控制
MAX_FUNDING_RATE = 0.0005       # 资金费率超过此值跳过（过高追高风险）

# 市值过滤 (CoinGecko free API — 流通市值)
MCAP_MIN = 10_000_000           # 最低流通市值 1000万USDT
MCAP_MAX = 500_000_000          # 最高流通市值 5亿USDT
MCAP_CACHE_PATH = os.path.join(os.path.dirname(__file__), "data", "cg_mcap_cache.json")

# 运行控制
MIN_VOL_USDT_24H = 200_000      # 24h成交量至少要这么多才能进扫描
MAX_RETRY = 2

# ── 日志 ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("breakout_detector")


# ════════════════════════════════════════════════════════════
#  1. 数据采集
# ════════════════════════════════════════════════════════════


def fetch_okx_tickers(min_vol_24h: float = MIN_VOL_USDT_24H) -> dict:
    """拉取 OKX USDT-SWAP 全市场 tickers，返回 {sym: info}。"""
    try:
        resp = requests.get(
            f"{OKX_API}/api/v5/market/tickers",
            params={"instType": "SWAP"},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(f"OKX tickers HTTP {resp.status_code}")
            return {}
        data = resp.json()
        if data.get("code") != "0":
            logger.warning(f"OKX tickers code={data.get('code')}")
            return {}
        raw_tickers = data.get("data", [])
    except requests.RequestException as e:
        logger.warning(f"OKX tickers 请求失败: {e}")
        return {}

    # ── 批量 OI + 资金费率 ──
    oi_map = {}
    fr_map = {}
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

    try:
        resp_fr = requests.get(
            f"{OKX_API}/api/v5/public/funding-rate",
            params={"instType": "SWAP"},
            timeout=15,
        )
        if resp_fr.status_code == 200:
            fr_data = resp_fr.json()
            if fr_data.get("code") == "0":
                for t in fr_data.get("data", []):
                    fr_map[t["instId"]] = float(t.get("fundingRate", 0))
    except Exception:
        pass

    # ── 构建初步结果 ──
    ticker_map = {}
    for t in raw_tickers:
        inst_id = t.get("instId", "")
        if not inst_id.endswith("USDT-SWAP"):
            continue
        try:
            last = float(t["last"])
            open24h = float(t["open24h"])
            high24h = float(t.get("high24h", 0))
            low24h = float(t.get("low24h", 0))
            vol_coins = float(t.get("volCcy24h", 0))
            vol_usd = last * vol_coins
            if vol_usd < min_vol_24h:
                continue
            chg24h = ((last - open24h) / open24h * 100) if open24h else 0.0
            sym = inst_id.replace("-", "").replace("SWAP", "")
            ticker_map[inst_id] = {
                "sym": sym,
                "last": last,
                "chg24h": chg24h,
                "vol_usd_24h": vol_usd,
                "high24h": high24h,
                "low24h": low24h,
                "open24h": open24h,
                "oi": oi_map.get(inst_id, 0),
                "funding_rate": fr_map.get(inst_id, 0.0),
                "source_exchange": "okx",
            }
        except (ValueError, TypeError, KeyError):
            continue

    logger.info(f"OKX tickers: {len(ticker_map)} 个(成交量≥{min_vol_24h/1000:.0f}K USDT)")
    return ticker_map


def fetch_bitget_tickers(min_vol_24h: float = MIN_VOL_USDT_24H) -> dict:
    """拉取 Bitget USDT-FUTURES tickers，返回 {sym: info}。"""
    try:
        resp = requests.get(
            f"{BITGET_API}/api/v2/mix/market/tickers",
            params={"productType": "USDT-FUTURES"},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(f"Bitget tickers HTTP {resp.status_code}")
            return {}
        data = resp.json()
        if data.get("code") != "00000":
            logger.warning(f"Bitget tickers code={data.get('code')}")
            return {}
        raw_tickers = data.get("data", [])
    except requests.RequestException as e:
        logger.warning(f"Bitget tickers 请求失败: {e}")
        return {}

    ticker_map = {}
    for t in raw_tickers:
        try:
            symbol = t.get("symbol", "")
            if not symbol or not symbol.endswith("USDT"):
                continue
            last = float(t["lastPr"])
            open24h = float(t.get("open24h", 0))
            high24h = float(t.get("high24h", 0))
            low24h = float(t.get("low24h", 0))
            # usdtVol is total USDT volume in 24h
            vol_usd = float(t.get("usdtVolume", 0))
            if vol_usd < min_vol_24h:
                continue
            chg24h = ((last - open24h) / open24h * 100) if open24h else 0.0
            # Build inst_id-like key for uniqueness
            sym = symbol.replace("_", "").replace("-", "")
            ticker_map[symbol] = {
                "sym": sym,
                "last": last,
                "chg24h": chg24h,
                "vol_usd_24h": vol_usd,
                "high24h": high24h,
                "low24h": low24h,
                "open24h": open24h,
                "oi": 0,
                "funding_rate": 0.0,
                "source_exchange": "bitget",
            }
        except (ValueError, TypeError, KeyError):
            continue

    logger.info(f"Bitget tickers: {len(ticker_map)} 个(成交量≥{min_vol_24h/1000:.0f}K USDT)")
    return ticker_map


def fetch_gate_tickers(min_vol_24h: float = MIN_VOL_USDT_24H) -> dict:
    """拉取 Gate USDT perpetual tickers，返回 {sym: info}。"""
    try:
        resp = requests.get(
            f"{GATE_API}/api/v4/futures/usdt/tickers",
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(f"Gate tickers HTTP {resp.status_code}")
            return {}
        raw_tickers = resp.json()
        if not isinstance(raw_tickers, list):
            logger.warning(f"Gate tickers unexpected format")
            return {}
    except requests.RequestException as e:
        logger.warning(f"Gate tickers 请求失败: {e}")
        return {}

    ticker_map = {}
    for t in raw_tickers:
        try:
            contract = t.get("contract", "")
            if not contract:
                continue
            last = float(t["last"])
            open24h = float(t.get("open24h_24h", 0))
            high24h = float(t.get("high24h_24h", 0))
            low24h = float(t.get("low24h_24h", 0))
            # volume_24h_base is base currency vol; multiply by last for USDT
            vol_base = float(t.get("volume_24h_base", 0))
            vol_usd = vol_base * last
            if vol_usd < min_vol_24h:
                continue
            chg24h = ((last - open24h) / open24h * 100) if open24h else 0.0
            sym = contract.replace("_", "").replace("-", "")
            ticker_map[contract] = {
                "sym": sym,
                "last": last,
                "chg24h": chg24h,
                "vol_usd_24h": vol_usd,
                "high24h": high24h,
                "low24h": low24h,
                "open24h": open24h,
                "oi": 0,
                "funding_rate": 0.0,
                "source_exchange": "gate",
            }
        except (ValueError, TypeError, KeyError):
            continue

    logger.info(f"Gate tickers: {len(ticker_map)} 个(成交量≥{min_vol_24h/1000:.0f}K USDT)")
    return ticker_map


def fetch_mexc_tickers(min_vol_24h: float = MIN_VOL_USDT_24H) -> dict:
    """拉取 MEXC USDT perpetual tickers，返回 {sym: info}。"""
    try:
        resp = requests.get(
            f"{MEXC_API}/api/v1/contract/ticker",
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(f"MEXC tickers HTTP {resp.status_code}")
            return {}
        data = resp.json()
        if data.get("code") != 0 and data.get("code") != "0":
            logger.warning(f"MEXC tickers code={data.get('code')}")
            return {}
        raw_list = data.get("data", [])
        if not isinstance(raw_list, list):
            logger.warning(f"MEXC tickers unexpected format")
            return {}
    except requests.RequestException as e:
        logger.warning(f"MEXC tickers 请求失败: {e}")
        return {}

    ticker_map = {}
    for t in raw_list:
        try:
            symbol = t.get("symbol", "")
            if not symbol:
                continue
            last = float(t["lastPrice"])
            # riseFallRate is decimal percentage, e.g. 0.05 = 5%
            raw_chg = t.get("riseFallRate", 0)
            if isinstance(raw_chg, str):
                raw_chg = float(raw_chg)
            chg24h = raw_chg * 100 if abs(raw_chg) < 10 else float(raw_chg)
            # volume24 is total USDT volume in 24h
            vol_usd = float(t.get("volume24", 0))
            if vol_usd < min_vol_24h:
                continue
            # MEXC doesn't provide open/high/low in ticker endpoint? Use defaults
            open24h = last / (1 + chg24h / 100) if chg24h != -100 else 0
            high24h = float(t.get("high24h", last))
            low24h = float(t.get("low24h", last))
            sym = symbol.replace("_", "").replace("-", "")
            ticker_map[symbol] = {
                "sym": sym,
                "last": last,
                "chg24h": chg24h,
                "vol_usd_24h": vol_usd,
                "high24h": high24h,
                "low24h": low24h,
                "open24h": open24h,
                "oi": 0,
                "funding_rate": 0.0,
                "source_exchange": "mexc",
            }
        except (ValueError, TypeError, KeyError):
            continue

    logger.info(f"MEXC tickers: {len(ticker_map)} 个(成交量≥{min_vol_24h/1000:.0f}K USDT)")
    return ticker_map


def merge_ticker_maps(maps: list) -> dict:
    """合并多个交易所的ticker map，同币种保留成交量最大的那个。
    
    Args:
        maps: [(str exchange_name, dict ticker_map), ...]
    Returns:
        合并后的 {sym: info} 字典，新增 source_exchange 字段
    """
    merged = {}
    for exchange_name, tm in maps:
        for inst_id, info in tm.items():
            sym = info["sym"]
            if sym in merged:
                # Keep the one with higher volume
                if info["vol_usd_24h"] > merged[sym]["vol_usd_24h"]:
                    merged[sym] = info
            else:
                merged[sym] = info
    logger.info(f"合并后共 {len(merged)} 个唯一币种")
    return merged


def fetch_5m_klines(inst_ids: list, limit: int = 4) -> dict:
    """批量获取5m K线。返回 {inst_id: klines}。"""
    kline_cache = {}
    for inst_id in inst_ids:
        for attempt in range(MAX_RETRY + 1):
            try:
                resp = requests.get(
                    f"{OKX_API}/api/v5/market/candles",
                    params={"instId": inst_id, "bar": "5m", "limit": limit},
                    timeout=8,
                )
                if resp.status_code == 200:
                    kd = resp.json()
                    if kd.get("code") == "0":
                        klines = kd.get("data", [])
                        if len(klines) >= 2:
                            kline_cache[inst_id] = klines
                        break
            except Exception:
                if attempt < MAX_RETRY:
                    _time.sleep(0.5)
                continue
    logger.info(f"5m K线: {len(kline_cache)}/{len(inst_ids)} 成功")
    return kline_cache


# ════════════════════════════════════════════════════════════
#  2. 检测逻辑
# ════════════════════════════════════════════════════════════


def detect_breakouts(
    ticker_map: dict,
    kline_cache: dict,
    coin_exchanges: dict = None,  # {sym: [exchange_name, ...]}
) -> list:
    """
    三条件起涨检测。

    参数:
        ticker_map: {inst_id: info}
        kline_cache: {inst_id: klines}
        coin_exchanges: {sym: [exchange_name, ...]} 跨所信息
    返回:
        [alert_dict, ...]
    """
    alerts = []

    for inst_id, info in ticker_map.items():
        klines = kline_cache.get(inst_id)
        if not klines or len(klines) < 2:
            continue

        try:
            # 解析K线: [ts, o, h, l, c, vol, vol_ccy, ...]
            c0 = klines[0]   # 最新(最近)K线
            c1 = klines[1]   # 前一根
            open0 = float(c0[1])
            high0 = float(c0[2])
            low0 = float(c0[3])
            close0 = float(c0[4])
            vol0 = float(c0[5])  # 张数
            vol_usdt_5m = vol0 * close0

            open1 = float(c1[1])
            close1 = float(c1[4])
            vol1 = float(c1[5])
            vol1_usdt = vol1 * close1

            # 如果有3根K线，取前两根均量对比
            vol_prev_avg = 0
            if len(klines) >= 3:
                vols = [float(k[5]) * float(k[4]) for k in klines[1:4]]
                vol_prev_avg = sum(vols) / len(vols) if vols else 0
            else:
                vol_prev_avg = vol1_usdt

            sym = info["sym"]
            last = info["last"]
            chg24h = info["chg24h"]
            low24h = info["low24h"]
            vol_usd_24h = info["vol_usd_24h"]
            oi = info["oi"]
            funding_rate = info["funding_rate"]

            # ── v2.2 跨所共振判断 + 三价格层级 ──
            exchanges = coin_exchanges.get(sym, []) if coin_exchanges else []
            is_resonance = len(exchanges) >= 2

            # ── v2.2 三价格层级量门槛 ──
            is_micro_price = last < MICRO_PRICE_THRESHOLD if last > 0 else False
            is_low_price = (last < LOW_PRICE_THRESHOLD and not is_micro_price) if last > 0 else False
            is_normal_price = not is_low_price and not is_micro_price

            if is_micro_price:
                min_vol = SURGE_MIN_VOL_MICRO       # 5K USDT
            elif is_low_price:
                min_vol = SURGE_MIN_VOL_LOW_PRICE   # 20K USDT
            else:
                min_vol = SURGE_MIN_VOL              # 200K USDT

            # 共振币成交量门槛降低50%
            if is_resonance:
                min_vol = max(min_vol // 2, SURGE_MIN_VOL_MICRO)

            # ── 5m K线涨跌幅 ──
            chg_5m_pct = ((close0 - open0) / open0 * 100) if open0 else 0
            chg_1m_from_prev = ((close0 - close1) / close1 * 100) if close1 else 0

            # ── v2.2 成交量中位数基线（用前3根中位数替代单根对比）──
            vol_baseline = vol1_usdt  # fallback
            if len(klines) >= 4:
                vol_list = [float(klines[j][5]) * float(klines[j][4]) for j in range(1, 4)]
                vol_baseline = sorted(vol_list)[len(vol_list)//2] if vol_list else vol1_usdt
            vol_ratio = vol_usdt_5m / vol_baseline if vol_baseline > 0 else 0

            # ── 距24h低点/高点 ──
            dist_from_low24_pct = ((close0 - low24h) / low24h * 100) if low24h > 0 else 999
            high24h = info.get("high24h", 0)
            dist_from_high24_pct = ((high24h - close0) / high24h * 100) if high24h > 0 else 999

            # ── 上下影线计算 ──
            lower_wick_pct = (min(open0, close0) - low0) / low0 * 100 if low0 > 0 else 0
            upper_wick_pct = (high0 - max(open0, close0)) / max(open0, close0) * 100 if max(open0, close0) > 0 else 0
            wick_range_pct = ((high0 - low0) / low0 * 100) if low0 > 0 else 0

            # ── 资金费率风控 ──
            if abs(funding_rate) > MAX_FUNDING_RATE:
                continue

            # ── v2.2 已检测维度 ──
            triggered_conditions = []

            # ═══════════════════════════════════════════════════════════
            # 条件F: 暴力拉升（新增 — 极端行情捕获器，不设量门槛）
            # ═══════════════════════════════════════════════════════════
            if chg_5m_pct >= SURGE_VIOLENT_MIN_PCT and vol_usdt_5m >= SURGE_VIOLENT_MIN_VOL:
                triggered_conditions.append({
                    "type": "F",
                    "label": "暴力拉升",
                    "detail": f"单根5m涨幅{chg_5m_pct:+.2f}%, 成交{_fmt_vol(vol_usdt_5m)}",
                })

            # ═══════════════════════════════════════════════════════════
            # 条件A: 常规起涨（v2.2 阈值降至1.5%）
            # ═══════════════════════════════════════════════════════════
            chg_threshold_a = SURGE_5M_MIN_PCT
            vol_threshold_a = min_vol
            if is_resonance:
                chg_threshold_a = 1.0
            if chg_5m_pct >= chg_threshold_a and vol_usdt_5m >= vol_threshold_a:
                if not any(c["type"] in ("A", "F") for c in triggered_conditions):
                    triggered_conditions.append({
                        "type": "A",
                        "label": "常规起涨",
                        "detail": f"单根5m涨幅{chg_5m_pct:+.2f}%, 成交量{_fmt_vol(vol_usdt_5m)}",
                    })

            # ═══════════════════════════════════════════════════════════
            # 条件B: 放量拉升（v2.2 量比降至2.5x, 涨幅降至1.0%）
            # ═══════════════════════════════════════════════════════════
            vol_ratio_b = VOL_SPIKE_MIN_RATIO
            if is_resonance:
                vol_ratio_b = 1.5
            if vol_ratio >= vol_ratio_b and chg_1m_from_prev >= VOL_SPIKE_MIN_CHG and vol_usdt_5m >= min_vol:
                triggered_conditions.append({
                    "type": "B",
                    "label": "放量拉升",
                    "detail": f"量比{vol_ratio:.1f}x, 涨幅{chg_1m_from_prev:+.2f}%",
                })

            # ═══════════════════════════════════════════════════════════
            # 条件C: 低位启动（v2.2 距低点放宽至5%, 量比降至3x, 涨幅降至1.5%）
            # ═══════════════════════════════════════════════════════════
            vol_ratio_c = LOW_STARTUP_VOL_RATIO
            if is_resonance:
                vol_ratio_c = 2.0
            if (dist_from_low24_pct <= LOW_STARTUP_NEAR_LOW24_PCT
                and vol_ratio >= vol_ratio_c
                and chg_5m_pct >= LOW_STARTUP_MIN_CHG
                and vol_usdt_5m >= min_vol):
                triggered_conditions.append({
                    "type": "C",
                    "label": "低位启动",
                    "detail": f"距24h低点{dist_from_low24_pct:.1f}%, 量比{vol_ratio:.1f}x, 涨幅{chg_5m_pct:+.2f}%",
                })

            # ═══════════════════════════════════════════════════════════
            # 条件D: 累计拉升（v2.2 紧缩化: 2根K线, 每根≥0.5%, 最新根放量）
            # ═══════════════════════════════════════════════════════════
            if len(klines) >= CUMULATIVE_RAMP_MIN_CANDLES + 1:
                ramp_check = True
                ramp_details = []
                for i in range(CUMULATIVE_RAMP_MIN_CANDLES):
                    ci = klines[i]
                    ci_next = klines[i+1] if i+1 < len(klines) else None
                    if ci_next:
                        close_ci = float(ci[4])
                        close_next = float(ci_next[4])
                        vol_next = float(ci_next[5]) * close_next
                        chg = ((close_next - close_ci) / close_ci * 100) if close_ci else 0
                        ramp_details.append((chg, vol_next))
                        if chg < CUMULATIVE_RAMP_MIN_CHG or vol_next < CUMULATIVE_RAMP_MIN_VOL:
                            ramp_check = False
                            break
                # v2.2: 用最新根/前一根量比替代逐根递增检查
                if ramp_check and vol_ratio < 1.0:
                    ramp_check = False  # 最新根不能缩量
                if ramp_check:
                    detail_strs = [f"{chg:+.2f}%/{_fmt_vol(vol)}" for chg, vol in ramp_details]
                    triggered_conditions.append({
                        "type": "D",
                        "label": "累计拉升",
                        "detail": " | ".join(detail_strs),
                    })
                    logger.info(f"  累计拉升检测命中: ${sym} {ramp_details}")

            # ═══════════════════════════════════════════════════════════
            # 条件E: 极端振幅（v2.2 振幅降至6%, 拆分上下影线）
            # ═══════════════════════════════════════════════════════════
            if wick_range_pct >= EXTREME_WICK_MIN_RANGE and vol_usdt_5m >= min_vol // EXTREME_WICK_MIN_VOL_DIVISOR:
                e_subtype = "E"
                e_label = "极端振幅"
                e_parts = [f"振幅{wick_range_pct:.1f}%"]

                # 看涨下影线(E1)
                if lower_wick_pct >= LOWER_WICK_MIN_PCT:
                    e_subtype = "E1"
                    e_label = "长下影线"
                    e_parts.append(f"下影{lower_wick_pct:.1f}%看涨")
                # 看跌上影线(E2)
                if upper_wick_pct >= UPPER_WICK_MIN_PCT:
                    if e_subtype == "E1":
                        e_subtype = "E3"
                        e_label = "宽幅震荡"
                        e_parts[-1] = f"下影{lower_wick_pct:.1f}%/上影{upper_wick_pct:.1f}%"
                    else:
                        e_subtype = "E2"
                        e_label = "长上影线"
                        e_parts.append(f"上影{upper_wick_pct:.1f}%看跌")

                e_parts.append(f"成交{_fmt_vol(vol_usdt_5m)}")
                triggered_conditions.append({
                    "type": e_subtype,
                    "label": e_label,
                    "detail": f"振幅{wick_range_pct:.1f}% | 成交{_fmt_vol(vol_usdt_5m)} | "
                              f"下影{lower_wick_pct:.1f}%/上影{upper_wick_pct:.1f}%",
                })

            # ═══════════════════════════════════════════════════════════
            # 条件G: 横盘蓄力（新增 — 前3根缩量横盘+最新根放量突破）
            # ═══════════════════════════════════════════════════════════
            if len(klines) >= ACCUMULATION_BARS + 1:
                accum_check = True
                accum_vols = []
                for i in range(1, ACCUMULATION_BARS + 1):  # klines[1], klines[2], klines[3]
                    ci_chg = abs((float(klines[i][4]) - float(klines[i][1])) / float(klines[i][1]) * 100) if float(klines[i][1]) > 0 else 999
                    ci_vol = float(klines[i][5]) * float(klines[i][4])
                    accum_vols.append(ci_vol)
                    if ci_chg > ACCUMULATION_MAX_CHG:
                        accum_check = False
                        break
                if accum_check and len(accum_vols) >= 2:
                    accum_avg_vol = sum(accum_vols) / len(accum_vols)
                    # 最新K线放量>均量*1.5 且 阳线上涨
                    if vol_usdt_5m >= accum_avg_vol * ACCUMULATION_MIN_VOL_RATIO and chg_5m_pct > 0:
                        triggered_conditions.append({
                            "type": "G",
                            "label": "蓄力突破",
                            "detail": f"前{ACCUMULATION_BARS}根横盘(|chg|<{ACCUMULATION_MAX_CHG}%), "
                                      f"最新放量{vol_usdt_5m/accum_avg_vol:.1f}x突破",
                        })

            # ═══════════════════════════════════════════════════════════
            # 条件H: 突破前高（新增 — 价格接近并突破24h高点+量确认）
            # ═══════════════════════════════════════════════════════════
            if (dist_from_high24_pct <= BREAKOUT_24H_DIST
                and chg_5m_pct > 0
                and vol_ratio >= BREAKOUT_MIN_VOL_RATIO
                and vol_usdt_5m >= min_vol // 2):
                triggered_conditions.append({
                    "type": "H",
                    "label": "突破前高",
                    "detail": f"距24h高点{dist_from_high24_pct:.1f}%, 量比{vol_ratio:.1f}x突破",
                })

            # ═══════════════════════════════════════════════════════════
            # 任一条件满足 → 生成告警
            # ═══════════════════════════════════════════════════════════
            if not triggered_conditions:
                continue

            condition_types = [c["type"] for c in triggered_conditions]
            condition_labels = [c["label"] for c in triggered_conditions]

            # ── v2.2 评分系统 ──
            score = len(triggered_conditions) * 12  # 基础分从15降到12

            has_a = any(c["type"] == "A" for c in triggered_conditions)
            has_b = any(c["type"] == "B" for c in triggered_conditions)
            has_c = any(c["type"] == "C" for c in triggered_conditions)
            has_d = any(c["type"] == "D" for c in triggered_conditions)
            has_e = any(c["type"].startswith("E") for c in triggered_conditions)
            has_f = any(c["type"] == "F" for c in triggered_conditions)
            has_g = any(c["type"] == "G" for c in triggered_conditions)
            has_h = any(c["type"] == "H" for c in triggered_conditions)

            # 条件类型加成
            if has_d: score += 10
            if has_e: score += 8
            if has_f: score += 25   # 暴力拉升=最强信号
            if has_g: score += 15   # 横盘蓄力突破=强信号
            if has_h: score += 12   # 突破前高

            # ── v2.2 条件组合协同加分 ──
            combo_bonus = 0
            if has_a and has_b: combo_bonus += 10     # 暴涨+放量=量价齐升
            if has_a and has_c: combo_bonus += 8       # 暴涨+低位启动=底部起爆
            if has_a and has_h: combo_bonus += 10      # 暴涨+突破前高=强突破
            if has_d and has_e: combo_bonus += 12      # 累计+wick=新币起爆
            if has_b and has_c and has_d: combo_bonus += 15  # 三重确认
            if has_g and has_h: combo_bonus += 10      # 蓄力+突破=即将爆发
            if has_f: combo_bonus += 5                 # F本身强再加
            score += combo_bonus

            # ── v2.2 量比趋势加分 ──
            if len(klines) >= 3:
                v0 = float(klines[0][5]) * float(klines[0][4])
                v1 = float(klines[1][5]) * float(klines[1][4])
                v2 = float(klines[2][5]) * float(klines[2][4])
                if v0 > v1 > v2: score += 8    # 连续3根放量
                elif v0 > v1: score += 4        # 连续2根放量

            # ── 涨幅加成 ──
            score += min(chg_5m_pct * 2, 15)

            # ── 爆量加成 ──
            score += 8 if vol_ratio >= 8 else 0
            score += 5 if vol_ratio >= 5 else 0

            # ── v2.2 24h涨跌幅细化 ──
            if chg24h <= 5: score += 10
            elif chg24h <= 15: score += 5
            elif chg24h <= 30: score += 0
            elif chg24h <= 50: score -= 5
            else: score -= 10

            # ── 共振加成 ──
            score += 12 if is_resonance else 0

            # ── v2.2 微市值α信号加成 ──
            if is_micro_price and chg_5m_pct > 0: score += 8

            score = max(0, min(120, score))  # 满分提到120

            # ── v2.2 优先级判定 ──
            if has_f:
                if score >= 60: priority = "🔴 暴力起爆"
                else: priority = "🟡 极端行情"
            elif has_g:
                if score >= 60: priority = "🔴 蓄力突破"
                else: priority = "🟢 蓄力观察"
            elif has_d or has_e:
                if score >= 50: priority = "🔵 潜力挖掘"
                elif score >= 30: priority = "🟢 早期观察"
                else: priority = "🟢 观察级"
            elif score >= 70: priority = "🔴 高优先级"
            elif score >= 40: priority = "🟡 中优先级"
            else: priority = "🟢 观察级"

            # 操作建议
            advice = _get_advice(triggered_conditions, chg_5m_pct, vol_ratio, is_low_price, is_micro_price)

            alerts.append({
                "sym": sym,
                "price": last,
                "chg_5m_pct": round(chg_5m_pct, 2),
                "chg_24h_pct": round(chg24h, 2),
                "vol_usdt_5m": round(vol_usdt_5m, 0),
                "vol_ratio": round(vol_ratio, 1),
                "dist_from_low24_pct": round(dist_from_low24_pct, 1),
                "is_low_price": is_low_price,
                "is_micro_price": is_micro_price,
                "is_resonance": is_resonance,
                "exchanges": exchanges,
                "condition_types": condition_types,
                "condition_labels": condition_labels,
                "conditions": triggered_conditions,
                "priority": priority,
                "score": score,
                "combo_bonus": combo_bonus,
                "advice": advice,
                "funding_rate": funding_rate,
                "oi": oi,
                "vol_usd_24h": vol_usd_24h,
            })
        except (ValueError, TypeError, IndexError) as e:
            logger.debug(f"解析K线异常 {inst_id}: {e}")
            continue

    # 按综合评分降序排列
    alerts.sort(key=lambda x: -x["score"])
    logger.info(f"检测到 {len(alerts)} 个起涨点信号")
    for a in alerts:
        logger.info(f"  {a['priority']} ${a['sym']} | {','.join(a['condition_labels'])} | score={a['score']}")
    return alerts


def _get_advice(conditions: list, chg_5m: float, vol_ratio: float, is_low_price: bool, is_micro_price: bool = False) -> str:
    """v2.2 操作建议 — 根据触发条件生成。"""
    has_a = any(c["type"] == "A" for c in conditions)
    has_b = any(c["type"] == "B" for c in conditions)
    has_c = any(c["type"] == "C" for c in conditions)
    has_d = any(c["type"] == "D" for c in conditions)
    has_f = any(c["type"] == "F" for c in conditions)
    has_g = any(c["type"] == "G" for c in conditions)
    has_h = any(c["type"] == "H" for c in conditions)

    points = []
    if has_f:
        points.append("暴力拉升中，注意放量可持续性，等回调确认")
    elif has_a and chg_5m >= 3:
        points.append("大幅拉升，追高风险大，等回调再确认")
    elif has_a:
        points.append("起涨初期，确认量能持续后再入场")
    if has_b:
        points.append("放量确认，量价配合良好")
    if has_c:
        points.append("低位刚启动，空间较大")
    if has_d:
        points.append("温和放量爬坡，动能累积中")
    if has_g:
        points.append("横盘蓄力后放量突破，起爆前信号")
    if has_h:
        points.append("突破24h前高，关注是否有效站稳")
    if is_micro_price:
        points.append("微市值币波动极大，严格仓位管理")
    elif is_low_price:
        points.append("低价币波动大，注意仓位管理")

    if not points:
        return "观察等待"
    return " | ".join(points)


def _fmt_vol(vol: float) -> str:
    if vol >= 1_000_000:
        return f"{vol/1_000_000:.2f}M"
    elif vol >= 1000:
        return f"{vol/1000:.0f}K"
    return f"{vol:.0f}"


# ════════════════════════════════════════════════════════════
#  3. TG推送
# ════════════════════════════════════════════════════════════


def send_tg(text: str, dry_run: bool = False) -> bool:
    """发送到突破检测专用Bot。"""
    if dry_run:
        print(f"\n{'='*50}")
        print(text)
        print(f"{'='*50}")
        return True

    if not BREAKOUT_BOT_TOKEN:
        logger.warning("BREAKOUT_BOT_TOKEN 未配置")
        return False

    url = f"https://api.telegram.org/bot{BREAKOUT_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id": BREAKOUT_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }, timeout=15)
        if resp.status_code == 200:
            logger.info("TG 突破检测推送成功")
            return True
        else:
            logger.warning(f"TG 推送失败: {resp.status_code} {resp.text[:200]}")
            return False
    except requests.RequestException as e:
        logger.warning(f"TG 请求异常: {e}")
        return False


def format_alert(a: dict) -> str:
    """格式化起涨检测告警消息。"""
    cond_str = " | ".join(a["condition_labels"])
    cond_detail = "\n".join(f"  • {c['detail']}" for c in a["conditions"])

    # Cross-exchange info
    exchanges = a.get("exchanges", ["okx"])
    exchange_str = ", ".join(ex.upper() for ex in exchanges)
    exchange_bonus = " 📡多所共振" if len(exchanges) >= 2 else ""

    # 价格层级标记
    price_tier = "微市值" if a.get("is_micro_price") else ("低价" if a.get("is_low_price") else "常规")

    # 组合加成行
    combo_line = f"\n组合加成：`+{a.get('combo_bonus', 0)}`" if a.get("combo_bonus", 0) > 0 else ""

    msg = (
        f"🚀 *起涨点检测 · ${a['sym']}*{exchange_bonus}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"优先级：{a['priority']} (score={a['score']})\n"
        f"触发模式：{cond_str}\n"
        f"价格层级：{price_tier}{combo_line}\n"
        f"交易所：`{exchange_str}`\n\n"
        f"📊 *行情数据*\n"
        f"当前价格：`${a['price']:.4f}`\n"
        f"5m涨幅：`{a['chg_5m_pct']:+.2f}%`\n"
        f"24h涨幅：`{a['chg_24h_pct']:+.2f}%`\n"
        f"5m成交额：`{_fmt_vol(a['vol_usdt_5m'])}` (量比 {a['vol_ratio']:.1f}x)\n"
        f"距24h低点：`{a['dist_from_low24_pct']:.1f}%`\n"
        f"24h成交额：`{_fmt_vol(a['vol_usd_24h'])}`\n\n"
        f"🔍 *触发细节*\n"
        f"{cond_detail}\n\n"
        f"💡 *建议*\n"
        f"{a['advice']}\n\n"
        f"⚠️ *风控*\n"
        f"资金费率：`{a['funding_rate']*100:.4f}%`\n"
        f"价格层级：{price_tier}\n\n"
        f"⏰ {datetime.now(timezone(timedelta(hours=8))).strftime('%m-%d %H:%M')} UTC+8"
    )
    return msg


# ════════════════════════════════════════════════════════════
#  4. 主逻辑
# ════════════════════════════════════════════════════════════


def main():
    import argparse
    parser = argparse.ArgumentParser(description="独立起涨点检测通道 v2.2")
    parser.add_argument("--dry", action="store_true", help="预览不发")
    parser.add_argument("--min-vol", type=float, default=None,
                        help="24h最低成交量USDT (default: auto, normal=200K, new=50K)")
    parser.add_argument("--report-low-score", action="store_true",
                        help="查看低分起涨信号池")
    args = parser.parse_args()

    # ── 低分信号池报表模式 ──
    if args.report_low_score:
        _report_low_score_breakouts()
        return

    t0 = _time.monotonic()
    ts = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M UTC+8")

    logger.info("🚀 独立起涨点检测通道 v2.1 (复盘优化版)")
    logger.info(f"阈值: 条件A: 5m涨≥{SURGE_5M_MIN_PCT}%+成交≥{_fmt_vol(SURGE_MIN_VOL)}"
                f" | 条件B: 量比≥{VOL_SPIKE_MIN_RATIO}x+涨≥{VOL_SPIKE_MIN_CHG}%"
                f" | 条件C: 距低点≤{LOW_STARTUP_NEAR_LOW24_PCT}%+量比≥{LOW_STARTUP_VOL_RATIO}x+涨≥{LOW_STARTUP_MIN_CHG}%")
    logger.info(f"新: 条件D(累计拉升) | 条件E(极端振幅) | 新币动态量门槛")
    logger.info(f"低价币阈值: $<{LOW_PRICE_THRESHOLD} 成交量门槛降至{_fmt_vol(SURGE_MIN_VOL_LOW_PRICE)}")

    # ── Step 1: 拉取全市场tickers (OKX + Bitget + Gate + MEXC) ──
    # 对新币用50K低量门槛，成熟币用200K（或用户指定的值）
    norm_vol = args.min_vol if args.min_vol is not None else MIN_VOL_USDT_24H
    okx_map = fetch_okx_tickers(norm_vol)
    bitget_map = fetch_bitget_tickers(norm_vol)
    gate_map = fetch_gate_tickers(norm_vol)
    mexc_map = fetch_mexc_tickers(norm_vol)

    # ── 新币动态量门槛: 加载之前见过的币 ──
    new_coin_seen = {}
    if os.path.exists(NEW_COIN_SEEN_PATH):
        try:
            with open(NEW_COIN_SEEN_PATH) as f:
                new_coin_seen = json.load(f)
        except (json.JSONDecodeError, IOError):
            new_coin_seen = {}
    logger.info(f"已知币池: {len(new_coin_seen)} 个已见过")

    # 再拉一批低价量的新币（仅对首次见到的币放宽阈值）
    new_coin_tickers = {}
    if norm_vol > MIN_VOL_USDT_24H_NEW:
        # 新币用50K门槛再拉一次所有交易所
        new_okx = fetch_okx_tickers(MIN_VOL_USDT_24H_NEW)
        new_bitget = fetch_bitget_tickers(MIN_VOL_USDT_24H_NEW)
        new_gate = fetch_gate_tickers(MIN_VOL_USDT_24H_NEW)
        new_mexc = fetch_mexc_tickers(MIN_VOL_USDT_24H_NEW)
        new_all = merge_ticker_maps([
            ("okx", new_okx), ("bitget", new_bitget),
            ("gate", new_gate), ("mexc", new_mexc),
        ])
        # 只保留新币: 之前没见过的
        # 快速查询: 哪些币已经在200K扫描池中
        seen_in_200k = set()
        for tm in [okx_map, bitget_map, gate_map, mexc_map]:
            seen_in_200k.update(v["sym"] for v in tm.values())
        for sym, info in new_all.items():
            if sym not in seen_in_200k and sym not in new_coin_seen:
                new_coin_tickers[sym] = info
        logger.info(f"新币动态量门槛: {len(new_coin_tickers)} 个首次见到的增量币(量>{_fmt_vol(MIN_VOL_USDT_24H_NEW)})")

    # Merge: keep the highest volume exchange per coin
    ticker_map = merge_ticker_maps([
        ("okx", okx_map),
        ("bitget", bitget_map),
        ("gate", gate_map),
        ("mexc", mexc_map),
    ])

    # Also build a per-coin exchange list for cross-exchange detection
    coin_exchanges = {}  # {sym: [exchange_name, ...]}
    for exchange_name, tm in [("okx", okx_map), ("bitget", bitget_map), ("gate", gate_map), ("mexc", mexc_map)]:
        for inst_id, info in tm.items():
            sym = info["sym"]
            if sym not in coin_exchanges:
                coin_exchanges[sym] = []
            if exchange_name not in coin_exchanges[sym]:
                coin_exchanges[sym].append(exchange_name)
    if not ticker_map:
        logger.warning("无ticker数据，退出")
        return

    # ── Step 2: 预筛选候选币 ──
    # Path A: normal coins meeting normal volume threshold
    candidate_syms = set()
    for inst_id, info in ticker_map.items():
        if info["vol_usd_24h"] >= norm_vol:
            candidate_syms.add(info["sym"])
    # Path B: coins with |chg24h| >= 3% regardless of volume
    for inst_id, info in ticker_map.items():
        if info["sym"] not in candidate_syms and abs(info["chg24h"]) >= 3.0:
            candidate_syms.add(info["sym"])
    # Path C: NEW coins (first seen) with lower 50K threshold
    for sym, info in new_coin_tickers.items():
        candidate_syms.add(sym)
    # Merge new coin info into ticker_map
    for sym, info in new_coin_tickers.items():
        if sym not in ticker_map:
            ticker_map[sym] = info

    # ── 市值过滤 ──
    mcap_cache = {}
    if os.path.exists(MCAP_CACHE_PATH):
        try:
            with open(MCAP_CACHE_PATH) as f:
                mcap_cache = json.load(f)
        except (json.JSONDecodeError, IOError):
            mcap_cache = {}

    if mcap_cache:
        pre_mcap = len(candidate_syms)
        mcap_filtered = set()
        for sym in candidate_syms:
            base = sym.replace("USDT", "")
            raw = mcap_cache.get(base)
            if raw is not None:
                mcap_val = raw.get("mcap") if isinstance(raw, dict) else raw
            else:
                mcap_val = None
            if mcap_val is not None and MCAP_MIN <= mcap_val <= MCAP_MAX:
                mcap_filtered.add(sym)
            elif mcap_val is not None:
                pass  # 市值超范围，过滤掉
            else:
                mcap_filtered.add(sym)  # 无市值数据放行
        candidate_syms = mcap_filtered
        logger.info(f"市值过滤: {pre_mcap} → {len(candidate_syms)} (需${MCAP_MIN/1e6:.0f}M-${MCAP_MAX/1e6:.0f}M)")
    else:
        logger.warning(f"市值缓存 {MCAP_CACHE_PATH} 不存在，跳过市值过滤")

    candidates = {s: ticker_map[s] for s in candidate_syms if s in ticker_map}
    logger.info(f"预筛选候选: {len(candidates)}/{len(ticker_map)} 个 (含{len(new_coin_tickers)}新币)")

    # Fetch 5m K线 from OKX
    sym_to_okx_id = {}
    for inst_id, info in okx_map.items():
        sym_to_okx_id[info["sym"]] = inst_id
    # Also try to get inst_ids from new_coin_tickers if they happen to be on OKX
    for sym, info in new_coin_tickers.items():
        if info.get("source_exchange") == "okx":
            for okx_inst_id, okx_info in new_okx.items():
                if okx_info["sym"] == sym:
                    sym_to_okx_id[sym] = okx_inst_id

    okx_inst_ids = []
    for sym in candidate_syms:
        if sym in sym_to_okx_id:
            okx_inst_ids.append(sym_to_okx_id[sym])

    kline_cache = fetch_5m_klines(okx_inst_ids, limit=4)

    # ── Step 3: 检测 ──
    alerts = detect_breakouts(candidates, kline_cache, coin_exchanges)

    # ── 更新新币seen缓存 ──
    # 把本次所有扫描到的币都记录一下（包括正常的）
    now_ts = _time.time()
    seen_updated = False
    for inst_id, info in ticker_map.items():
        sym = info["sym"]
        if sym not in new_coin_seen:
            new_coin_seen[sym] = now_ts
            seen_updated = True
    for sym in new_coin_tickers:
        if sym not in new_coin_seen:
            new_coin_seen[sym] = now_ts
            seen_updated = True
    if seen_updated:
        # Limit to 2000 entries
        if len(new_coin_seen) > 2000:
            sorted_seen = sorted(new_coin_seen.items(), key=lambda x: x[1], reverse=True)[:2000]
            new_coin_seen = dict(sorted_seen)
        os.makedirs(os.path.dirname(NEW_COIN_SEEN_PATH), exist_ok=True)
        with open(NEW_COIN_SEEN_PATH, "w") as f:
            json.dump(new_coin_seen, f)

    # ── 保存低分信号到独立池 ──
    low_score_alerts = [a for a in alerts if a["score"] < LOW_SCORE_THRESHOLD]
    if low_score_alerts:
        _save_low_score_breakouts(low_score_alerts, ts)
        logger.info(f"低分信号池: 新增 {len(low_score_alerts)} 个(score<{LOW_SCORE_THRESHOLD})")

    # ── Step 4: 推送去重 ──
    dedup_cache_path = os.path.join(os.path.dirname(__file__), "data", "breakout_dedup.json")
    dedup_cache = {}
    if os.path.exists(dedup_cache_path):
        try:
            with open(dedup_cache_path) as f:
                dedup_cache = json.load(f)
        except (json.JSONDecodeError, IOError):
            dedup_cache = {}

    dedup_window = 900  # 15 minutes

    filtered_alerts = []
    for a in alerts:
        sym = a["sym"]
        last_ts = dedup_cache.get(sym, 0)
        if last_ts and (now_ts - last_ts) < dedup_window:
            logger.info(f"  去重跳过: ${sym} (上次推送{int(now_ts-last_ts)}s前)")
            continue
        dedup_cache[sym] = now_ts
        filtered_alerts.append(a)

    if len(dedup_cache) > 500:
        sorted_cache = sorted(dedup_cache.items(), key=lambda x: x[1], reverse=True)[:500]
        dedup_cache = dict(sorted_cache)
    os.makedirs(os.path.dirname(dedup_cache_path), exist_ok=True)
    with open(dedup_cache_path, "w") as f:
        json.dump(dedup_cache, f)

    # ── Step 5: 推送 ──
    pushed = 0
    for a in filtered_alerts:
        msg = format_alert(a)
        send_tg(msg, args.dry)
        pushed += 1
        logger.info(f"  推送 #{pushed}: ${a['sym']} ({a['priority']}, score={a['score']})")

    # ── 无信号时确认（v2.2:每次推送，不再跳过）──
    if not alerts or not filtered_alerts:
        msg = (
            f"📊 *起涨点检测 · 空扫描*\n"
            f"{ts}\n\n"
            f"扫描 {len(candidates)} 个币种(流通市值${MCAP_MIN/1e6:.0f}M-${MCAP_MAX/1e6:.0f}M)，"
            f"未检测到符合条件的起涨信号。\n"
            f"低分信号池: {len(low_score_alerts)}个(score<{LOW_SCORE_THRESHOLD})"
        )
        send_tg(msg, args.dry)
        logger.info("无突破信号(发送空扫描确认)")

    cost = _time.monotonic() - t0
    logger.info(f"✅ 完成 ({cost:.1f}s) | 扫描={len(ticker_map)} | 检测={len(alerts)} | 推送={pushed} | 新币={len(new_coin_tickers)} | 低分池={len(low_score_alerts)}")
    print(f"\n运行耗时 {cost:.1f}s | 扫描 {len(ticker_map)} 标的 | 检测 {len(alerts)} 个 | 推送 {pushed} 条")


def _save_low_score_breakouts(alerts: list, ts_str: str):
    """将低分起涨信号保存到独立文件。"""
    records = []
    for a in alerts:
        records.append({
            "ts": ts_str,
            "sym": a["sym"],
            "score": a["score"],
            "conditions": a["condition_types"],
            "price": a["price"],
            "chg_5m": a["chg_5m_pct"],
            "chg_24h": a["chg_24h_pct"],
            "vol_ratio": a["vol_ratio"],
            "vol_5m": a["vol_usdt_5m"],
            "vol_24h": a["vol_usd_24h"],
            "is_micro_price": a.get("is_micro_price", False),
            "price_tier": "微市值" if a.get("is_micro_price") else ("低价" if a.get("is_low_price") else "常规"),
        })

    # Append to existing (keep max 200)
    existing = []
    if os.path.exists(LOW_SCORE_BREAKOUTS_PATH):
        try:
            with open(LOW_SCORE_BREAKOUTS_PATH) as f:
                existing = json.load(f)
        except (json.JSONDecodeError, IOError):
            existing = []

    existing.extend(records)
    if len(existing) > 200:
        existing = existing[-200:]

    os.makedirs(os.path.dirname(LOW_SCORE_BREAKOUTS_PATH), exist_ok=True)
    with open(LOW_SCORE_BREAKOUTS_PATH, "w") as f:
        json.dump(existing, f, indent=2)


def _report_low_score_breakouts():
    """打印低分信号池报表。"""
    if not os.path.exists(LOW_SCORE_BREAKOUTS_PATH):
        print("低分信号池为空")
        return
    try:
        with open(LOW_SCORE_BREAKOUTS_PATH) as f:
            records = json.load(f)
    except (json.JSONDecodeError, IOError):
        print("低分信号池读取失败")
        return

    if not records:
        print("低分信号池为空")
        return

    print(f"\n{'='*60}")
    print(f"📊 低分起涨信号池 (score<{LOW_SCORE_THRESHOLD})")
    print(f"{'='*60}")
    print(f"共 {len(records)} 条记录\n")

    # 按币种统计出现频率
    from collections import Counter
    sym_counts = Counter(r["sym"] for r in records)
    top_repeats = sym_counts.most_common(15)

    print("🔁 高频出现币种 (可能即将爆发):")
    for sym, cnt in top_repeats:
        if cnt >= 2:
            print(f"  {sym:15s} ×{cnt}次")

    print(f"\n📋 最近20条:")
    for r in records[-20:]:
        conds = ",".join(r.get("conditions", []))
        print(f"  {r['ts'][:16]:16s} ${r['sym']:12s} score={r['score']:3d} "
              f"5m={r.get('chg_5m',0):+.2f}% vol={_fmt_vol(r.get('vol_5m',0)):>8s} "
              f"cond=[{conds}]")

    print(f"\n{'='*60}")


if __name__ == "__main__":
    main()
