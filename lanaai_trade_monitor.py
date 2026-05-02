#!/usr/bin/env python3
"""
lanaai_trade_monitor.py — 监控币安 Square 实盘交易持仓变动

功能：
  从 lanaai 的币安 Square 个人主页解析所有带实盘交易的帖子，
  对比上一轮快照，发现新开仓或平仓时推送到 Telegram。

数据来源（按优先级）：
  Mode 1: 从 stdin 读 HTML（由外层脚本传入 browser snapshot）
  Mode 2: 从 args --snapshot-file 读预存的 HTML
  Mode 3: 直接从页面文本正则解析（预设的 snapshot 文本）
  Mode 4: 无输入时读本地的 state (缓存的上一轮数据) 并返回空快照

用法:
  python3 lanaai_trade_monitor.py                          # 从当前页面文本解析 + 检查变化
  python3 lanaai_trade_monitor.py --test                   # 测试模式，打印当前持仓
  python3 lanaai_trade_monitor.py --snapshot-file ./page.txt  # 从文件读取页面文本

快照文件格式:
  ~/.lanaai_monitor/snapshot.json    # 上一轮持仓快照
  ~/.lanaai_monitor/seen_trades.json  # 已推送的交易 ID（防重复）
"""

import re
import json
import sys
import os
import time
import hashlib
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("lanaai_monitor")

# ── 配置 ──────────────────────────────────────────────────────────────────────
USERNAME = "lanaai"
STATE_DIR = Path.home() / ".lanaai_monitor"
SNAPSHOT_FILE = STATE_DIR / "snapshot.json"
SEEN_TRADES_FILE = STATE_DIR / "seen_trades.json"

# ── TG Bot（从环境变量读取）────────────────────────────────────────────────────
# 自动从 Hermes 配置文件读取 TG 凭据
_HERMES_ENV_PATH = Path("/root/hermes-agent/.env")

def _load_hermes_tg_config() -> tuple:
    """从 Hermes 的 .env 文件读取 TG bot token 和 chat_id"""
    token = os.environ.get("LANAAI_TG_BOT_TOKEN", "")
    chat_id = os.environ.get("LANAAI_TG_CHAT_ID", "")
    
    if not token and _HERMES_ENV_PATH.exists():
        try:
            for line in _HERMES_ENV_PATH.read_text().splitlines():
                line = line.strip()
                if line.startswith("TELEGRAM_BOT_TOKEN="):
                    token = line.split("=", 1)[1].strip().strip('"').strip("'")
                elif line.startswith("TELEGRAM_ALLOWED_USERS="):
                    chat_id = line.split("=", 1)[1].strip().strip('"').strip("'")
        except Exception:
            pass
    
    return token, chat_id

TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID = _load_hermes_tg_config()

# ── 解析模式常量 ──────────────────────────────────────────────────────────────
# 页面文本中的实盘数据格式（从 Hermes browser snapshot 确认）：
#   "CHIPUSDT Perp Opening Long Unrealized PNL +2,769.82USDT"
#   "SWARMSUSDT Perp Closed PNL -556.60USDT"
#   "B" 前缀表示已平仓（B = 实盘平仓标记）

TRADE_PATTERN = re.compile(
    r"""
    ([A-Z0-9]{2,}USDT)       # symbol (CHIPUSDT, SWARMSUSDT...)
    \s*Perp\s*               # 合约类型
    (Opening|Closed)          # 状态
    (?:\s+(Long|Short))?      # 方向（可选，平仓时可能不出现）
    (?:\s+Unrealized)?        # 未实现（可选）
    \s*PNL\s*                # PNL 标记
    ([+-]?[\d,]+\.\d+)       # 盈亏金额
    USDT                      # 单位
    """,
    re.VERBOSE,
)

# 辅助匹配：带 B 标记（平仓）
CLOSED_B_PATTERN = re.compile(
    r"""
    \bB\b\s+                  # 平仓标记 B
    ([A-Z0-9]{2,}USDT)       # symbol
    """,
    re.VERBOSE,
)


def parse_trades_from_text(text: str) -> list[dict]:
    """
    从纯文本中解析所有实盘交易条目。
    
    Returns:
        [{"symbol": "CHIPUSDT", "status": "Opening"/"Closed",
          "side": "Long"/"Short"/None, "pnl": 2769.82,
          "trade_id": str, "is_closed": bool}, ...]
    """
    trades = []
    for m in TRADE_PATTERN.finditer(text):
        symbol = m.group(1)
        status = m.group(2)
        side = m.group(3)  # 可能为 None
        pnl_str = m.group(4).replace(",", "")
        try:
            pnl = float(pnl_str)
        except ValueError:
            pnl = 0.0
        
        # 检查 symbol 前面是否有 B 标记（平仓确认）
        is_closed = status == "Closed"
        
        # 生成唯一 trade_id: symbol + pnl + 前后文 hash
        ctx_before = text[max(0, m.start() - 40):m.start()]
        trade_id = hashlib.md5(
            f"{symbol}:{pnl}:{ctx_before}".encode()
        ).hexdigest()[:16]
        
        trades.append({
            "symbol": symbol,
            "status": status,
            "side": side,
            "pnl": pnl,
            "is_closed": is_closed,
            "trade_id": trade_id,
            "timestamp": int(time.time()),
        })
    
    # 去重（同一 symbol 同方向同 pnl 的多条只保留第一条）
    seen = set()
    deduped = []
    for t in trades:
        key = (t["symbol"], t["status"], round(t["pnl"], 1))
        if key not in seen:
            seen.add(key)
            deduped.append(t)
    
    return deduped


def build_current_snapshot(trades: list[dict]) -> dict:
    """
    从交易列表构建当前持仓快照。
    只保留 Opening 状态（当前持仓）。
    """
    positions = {}
    for t in trades:
        if t["status"] == "Opening":
            positions[t["symbol"]] = {
                "side": t.get("side"),
                "pnl": t["pnl"],
                "last_seen": t["timestamp"],
                "trade_id": t["trade_id"],
            }
    return {"positions": positions, "timestamp": int(time.time())}


def load_snapshot() -> dict:
    """加载上一轮快照"""
    if SNAPSHOT_FILE.exists():
        try:
            data = json.loads(SNAPSHOT_FILE.read_text())
            logger.info(f"已加载上一轮快照: {len(data.get('positions', {}))} 个持仓")
            return data
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"快照文件损坏，重新创建: {e}")
    return {"positions": {}, "timestamp": 0}


def save_snapshot(snapshot: dict):
    """保存当前快照"""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_FILE.write_text(json.dumps(snapshot, indent=2))
    logger.info(f"快照已保存: {len(snapshot.get('positions', {}))} 个持仓")


def load_seen_trades() -> set:
    """加载已推送的交易 ID 集合"""
    if SEEN_TRADES_FILE.exists():
        try:
            return set(json.loads(SEEN_TRADES_FILE.read_text()))
        except (json.JSONDecodeError, TypeError):
            pass
    return set()


def save_seen_trades(seen: set):
    """保存已推送的交易 ID"""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    SEEN_TRADES_FILE.write_text(json.dumps(list(seen)))


def detect_changes(
    current: dict,
    previous: dict,
    seen_trades: set,
) -> list[dict]:
    """
    对比快照，检测新建仓和平仓事件。
    
    Returns:
        [{"type": "open"/"close", "symbol": str, "side": str,
          "pnl": float, "trade_id": str}, ...]
    """
    events = []
    prev_positions = previous.get("positions", {})
    curr_positions = current.get("positions", {})
    
    # 新建仓：在 current 但不在 previous
    for sym, pos in curr_positions.items():
        if sym not in prev_positions and pos["trade_id"] not in seen_trades:
            events.append({
                "type": "open",
                "symbol": sym,
                "side": pos.get("side", "Unknown"),
                "pnl": pos["pnl"],
                "trade_id": pos["trade_id"],
            })
    
    # 平仓：在 previous 但不在 current
    for sym, pos in prev_positions.items():
        if sym not in curr_positions and pos["trade_id"] not in seen_trades:
            events.append({
                "type": "close",
                "symbol": sym,
                "side": pos.get("side", "Unknown"),
                "pnl": pos["pnl"],
                "trade_id": pos["trade_id"],
            })
    
    return events


def format_tg_message(event: dict) -> str:
    """格式化 TG 推送消息"""
    symbol = event["symbol"].replace("USDT", "")
    side = event.get("side", "")
    pnl = event.get("pnl", 0)
    emoji = "🟢" if event["type"] == "open" else "🔴"
    side_str = f" {side}" if side and side != "Unknown" else ""
    
    if event["type"] == "open":
        pnl_str = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"
        return (
            f"{emoji} <b>lanaai 开仓</b>\n"
            f"#{symbol}{side_str}\n"
            f"🏦 未实现盈亏: {pnl_str}"
        )
    else:
        pnl_str = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"
        return (
            f"{emoji} <b>lanaai 平仓</b>\n"
            f"#{symbol}{side_str}\n"
            f"💸 盈亏: {pnl_str}"
        )


def send_tg_message(message: str) -> bool:
    """发送 TG 消息"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("TG 未配置 (LANAAI_TG_BOT_TOKEN / LANAAI_TG_CHAT_ID)")
        return False
    
    import urllib.request
    import urllib.parse
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    
    try:
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if result.get("ok"):
                logger.info("TG 消息发送成功")
                return True
            else:
                logger.warning(f"TG 发送失败: {result}")
                return False
    except Exception as e:
        logger.warning(f"TG 发送异常: {e}")
        return False


def print_trades(trades: list[dict]):
    """打印交易列表（调试/测试用）"""
    if not trades:
        print("⚠️  未发现任何实盘交易数据")
        return
    
    print(f"\n{'='*60}")
    print(f"📊 lanaai 实盘交易数据 ({len(trades)} 条)")
    print(f"{'='*60}")
    
    opening = [t for t in trades if t["status"] == "Opening"]
    closed = [t for t in trades if t["status"] == "Closed"]
    
    if opening:
        print(f"\n🟢 当前持仓 ({len(opening)}):")
        for t in opening:
            side_str = f" {t.get('side', '')}" if t.get('side') else ""
            pnl_str = f"+${t['pnl']:,.2f}" if t['pnl'] >= 0 else f"-${abs(t['pnl']):,.2f}"
            print(f"  {t['symbol'].replace('USDT',''):>10}{side_str:>8}  {pnl_str}")
    
    if closed:
        print(f"\n🔴 已平仓 ({len(closed)}):")
        for t in closed:
            side_str = f" {t.get('side', '')}" if t.get('side') else ""
            pnl_str = f"+${t['pnl']:,.2f}" if t['pnl'] >= 0 else f"-${abs(t['pnl']):,.2f}"
            print(f"  {t['symbol'].replace('USDT',''):>10}{side_str:>8}  {pnl_str}")
    
    print()


def main():
    # ── 获取输入 ──────────────────────────────────────────────────
    input_text = None
    
    if "--snapshot-file" in sys.argv:
        idx = sys.argv.index("--snapshot-file")
        if idx + 1 < len(sys.argv):
            filepath = sys.argv[idx + 1]
            input_text = Path(filepath).read_text(encoding="utf-8")
            logger.info(f"从文件读取: {filepath} ({len(input_text)} chars)")
    
    elif not sys.stdin.isatty():
        input_text = sys.stdin.read()
        logger.info(f"从 stdin 读取: {len(input_text)} chars")
    
    # ── 解析交易数据 ──────────────────────────────────────────────
    trades = []
    if input_text:
        trades = parse_trades_from_text(input_text)
        logger.info(f"解析到 {len(trades)} 条交易记录")
    else:
        logger.info("无输入数据，尝试从已有快照恢复...")
    
    # ── 测试模式 ──────────────────────────────────────────────────
    if "--test" in sys.argv:
        print_trades(trades)
        return
    
    # ── 构建快照 ──────────────────────────────────────────────────
    if trades:
        current_snapshot = build_current_snapshot(trades)
    else:
        # 无新数据时保持空快照，触发平仓检测
        current_snapshot = {"positions": {}, "timestamp": int(time.time())}
    
    previous_snapshot = load_snapshot()
    seen_trades = load_seen_trades()
    
    # ── 检测变化 ──────────────────────────────────────────────────
    events = detect_changes(current_snapshot, previous_snapshot, seen_trades)
    
    # ── 处理事件 ──────────────────────────────────────────────────
    if not events:
        logger.info("无新交易事件")
    else:
        logger.info(f"检测到 {len(events)} 个新事件:")
        
        for event in events:
            # 推送 TG
            msg = format_tg_message(event)
            print(f"\n{'─'*40}")
            print(msg)
            print(f"{'─'*40}")
            
            success = send_tg_message(msg)
            if success:
                seen_trades.add(event["trade_id"])
                logger.info(f"已推送: {event['type']} {event['symbol']}")
            else:
                logger.warning(f"推送失败: {event['type']} {event['symbol']}")
        
        # 保存已推送记录
        save_seen_trades(seen_trades)
    
    # ── 保存快照（即使无变化也保存，保持时间戳更新）─────────────
    if trades:
        save_snapshot(current_snapshot)
    else:
        # 保持上一轮快照的时间戳
        save_snapshot(previous_snapshot)


if __name__ == "__main__":
    main()
