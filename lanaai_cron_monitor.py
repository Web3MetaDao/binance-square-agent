#!/usr/bin/env python3
"""
lanaai_cron_monitor.py — 独立版 lanaai 持仓监控
===============================================
自包含脚本，供 cron job 使用。不需要 Hermes browser 工具。
用 agent-browser CLI 获取币安 Square 页面数据。

用法:
  python3 lanaai_cron_monitor.py          # 正常监控模式
  python3 lanaai_cron_monitor.py --test   # 测试模式，只打印不推送
  python3 lanaai_cron_monitor.py --snapshot-file <path>  # 从文件读取

输出:
  正常模式：检测到事件则推送 TG，否则打印 [SILENT]
  测试模式：打印当前持仓数据
"""

import json, os, re, hashlib, subprocess, sys, time, logging, urllib.request, urllib.parse
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("lanaai_cron")

# ── 配置 ──────────────────────────────────────────────────────
USERNAME = "lanaai"
STATE_DIR = Path.home() / ".lanaai_monitor"
SNAPSHOT_FILE = STATE_DIR / "snapshot.json"
SEEN_TRADES_FILE = STATE_DIR / "seen_trades.json"
AGENT_BROWSER = "/usr/bin/agent-browser"
PAGE_URL = "https://www.binance.com/en/square/profile/lanaai"
_HERMES_ENV = Path("/root/hermes-agent/.env")


def _load_tg_config() -> tuple:
    token = os.environ.get("LANAAI_TG_BOT_TOKEN", "8081319730:AAGeakKWvN4pHpd3DN5pkJFhKZKMGKAwFvM")
    chat_id = os.environ.get("LANAAI_TG_CHAT_ID", "1077054086")
    if not token and _HERMES_ENV.exists():
        for line in _HERMES_ENV.read_text().splitlines():
            s = line.strip()
            if s.startswith("TELEGRAM_BOT_TOKEN=***"):
                token = s.split("=", 1)[1].strip().strip("\"'")
            elif s.startswith("TELEGRAM_ALLOWED_USERS="):
                chat_id = s.split("=", 1)[1].strip().strip("\"'")
    return token, chat_id


TG_TOKEN, TG_CHAT = _load_tg_config()


# ── 页面抓取 ──────────────────────────────────────────────────

def fetch_page_text() -> Optional[str]:
    """用 agent-browser 打开币安 Square 页面并获取 innerText"""
    try:
        logger.info(f"打开页面 ...")
        subprocess.run([AGENT_BROWSER, "close"], capture_output=True, timeout=5)
        time.sleep(1)
        subprocess.run([AGENT_BROWSER, "open", PAGE_URL], capture_output=True, timeout=30)
        time.sleep(6)  # 等 JS 渲染

        # 滚动以触发懒加载
        for _ in range(3):
            subprocess.run([AGENT_BROWSER, "eval", "window.scrollTo(0, document.body.scrollHeight)"], capture_output=True, timeout=10)
            time.sleep(1.5)

        r = subprocess.run([AGENT_BROWSER, "eval", "document.body.innerText"], capture_output=True, text=True, timeout=15)
        subprocess.run([AGENT_BROWSER, "close"], capture_output=True, timeout=5)

        if r.returncode != 0:
            logger.warning(f"eval 失败: {r.stderr[:200]}")
            return None

        text = r.stdout.strip()
        if text.startswith('"') and text.endswith('"'):
            try:
                text = json.loads(text)
            except json.JSONDecodeError:
                pass

        # Normalize 转义换行
        if "\\n" in text and "\\\\n" not in text:
            text = text.replace("\\n", "\n")

        logger.info(f"获取到页面文本: {len(text)} chars")
        return text
    except Exception as e:
        logger.warning(f"获取页面失败: {e}")
        return None


# ── 交易解析 ──────────────────────────────────────────────────

def parse_trades(text: str) -> list[dict]:
    """从页面文本中解析实盘交易数据"""
    if not text:
        return []

    pat = re.compile(
        r"([\w\u4e00-\u9fff]{2,20}USDT)\s*\n\s*Perp\s*\n\s*"
        r"(?:B\s*\n\s*)?"
        r"(Opening|Closed)\s*(Long\s*)?\s*\n\s*"
        r"(?:Unrealized\s*)?"
        r"PNL\s*\n\s*"
        r"([+-]?[\d,]+(?:\.\d+)?)USDT"
    )

    trades = []
    for m in pat.finditer(text):
        symbol, status, side_str, pnl_str = m.group(1), m.group(2), m.group(3), m.group(4).replace(",", "")
        try:
            pnl = float(pnl_str)
        except ValueError:
            pnl = 0.0
        side = side_str.strip() if side_str else None
        tid = hashlib.md5(f"{symbol}:{status}:{pnl}".encode()).hexdigest()[:16]
        trades.append({
            "symbol": symbol,
            "status": "Opening" if status == "Opening" else "Closed",
            "side": side, "pnl": pnl,
            "trade_id": tid,
            "timestamp": int(time.time()),
        })

    # 去重
    seen = set()
    deduped = []
    for t in trades:
        key = (t["symbol"], t["status"], round(t["pnl"], 1))
        if key not in seen:
            seen.add(key)
            deduped.append(t)
    return deduped


def build_snapshot(trades: list[dict]) -> dict:
    positions = {}
    for t in trades:
        if t["status"] == "Opening":
            positions[t["symbol"]] = {"side": t["side"], "pnl": t["pnl"], "last_seen": t["timestamp"], "trade_id": t["trade_id"]}
    return {"positions": positions, "timestamp": int(time.time())}


def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return default


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


def detect_events(current: dict, previous: dict, seen: set) -> list[dict]:
    events = []
    prev_p = previous.get("positions", {})
    curr_p = current.get("positions", {})
    for sym, pos in curr_p.items():
        if sym not in prev_p and pos["trade_id"] not in seen:
            events.append({"type": "open", "symbol": sym, "side": pos.get("side", "Unknown"), "pnl": pos["pnl"], "trade_id": pos["trade_id"]})
    for sym, pos in prev_p.items():
        if sym not in curr_p and pos["trade_id"] not in seen:
            events.append({"type": "close", "symbol": sym, "side": pos.get("side", "Unknown"), "pnl": pos["pnl"], "trade_id": pos["trade_id"]})
    return events


def fmt_msg(event: dict) -> str:
    sym = event["symbol"].replace("USDT", "")
    side = event.get("side", "")
    emoji = "🟢" if event["type"] == "open" else "🔴"
    side_str = f" {side}" if side and side != "Unknown" else ""
    pnl_str = f"+${event['pnl']:,.2f}" if event['pnl'] >= 0 else f"-${abs(event['pnl']):,.2f}"
    lbl = "开仓" if event["type"] == "open" else "平仓"
    icon = "🏦 未实现盈亏" if event["type"] == "open" else "💸 盈亏"
    return f"{emoji} <b>lanaai {lbl}</b>\n#{sym}{side_str}\n{icon}: {pnl_str}"


def send_tg(msg: str) -> bool:
    if not TG_TOKEN or not TG_CHAT:
        logger.warning("TG 未配置")
        return False
    data = urllib.parse.urlencode({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": "true"}).encode()
    try:
        req = urllib.request.Request(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            ok = json.loads(resp.read()).get("ok")
            if ok:
                logger.info("TG 消息发送成功")
            else:
                logger.warning("TG 发送返回失败")
            return bool(ok)
    except Exception as e:
        logger.warning(f"TG 发送异常: {e}")
        return False


def print_trades(trades: list[dict]):
    if not trades:
        print("⚠️  未发现实盘交易")
        return
    opening = [t for t in trades if t["status"] == "Opening"]
    closed = [t for t in trades if t["status"] == "Closed"]
    print(f"\n📊 lanaai 实盘交易 ({len(trades)} 条)")
    if opening:
        print(f"\n🟢 持仓 ({len(opening)}):")
        for t in opening:
            p = f"+${t['pnl']:,.2f}" if t['pnl'] >= 0 else f"-${abs(t['pnl']):,.2f}"
            s = f" {t['side']}" if t['side'] else ""
            print(f"  {t['symbol'].replace('USDT',''):>10}{s:>6}  {p}")
    if closed:
        print(f"\n🔴 已平仓 ({len(closed)}):")
        for t in closed:
            p = f"+${t['pnl']:,.2f}" if t['pnl'] >= 0 else f"-${abs(t['pnl']):,.2f}"
            print(f"  {t['symbol'].replace('USDT',''):>10}  {p}")
    print()


def main():
    # ── 获取输入 ──
    if "--snapshot-file" in sys.argv:
        idx = sys.argv.index("--snapshot-file")
        fp = sys.argv[idx + 1]
        text = Path(fp).read_text(encoding="utf-8")
        if "\\n" in text and "\\\\n" not in text:
            text = text.replace("\\n", "\n")
        trades = parse_trades(text)
        logger.info(f"从文件读取: {len(text)} chars → {len(trades)} 条")
    else:
        logger.info("获取 lanaai 页面数据...")
        text = fetch_page_text()
        if not text:
            logger.error("页面获取失败")
            sys.exit(1)
        trades = parse_trades(text)

    logger.info(f"解析到 {len(trades)} 条交易记录")

    if "--test" in sys.argv:
        print_trades(trades)
        return

    # ── 差分检测 ──
    cur = build_snapshot(trades) if trades else {"positions": {}, "timestamp": int(time.time())}
    prev = load_json(SNAPSHOT_FILE, {"positions": {}, "timestamp": 0})
    seen = set(load_json(SEEN_TRADES_FILE, []))

    events = detect_events(cur, prev, seen)

    if not events:
        logger.info("无新交易事件")
        print("[SILENT]")
    else:
        logger.info(f"检测到 {len(events)} 个事件:")
        for ev in events:
            msg = fmt_msg(ev)
            print(f"\n{'─'*40}\n{msg}\n{'─'*40}")
            if send_tg(msg):
                seen.add(ev["trade_id"])
        save_json(SEEN_TRADES_FILE, list(seen))

    if trades:
        save_json(SNAPSHOT_FILE, cur)
    else:
        save_json(SNAPSHOT_FILE, prev)


if __name__ == "__main__":
    main()
