#!/usr/bin/env python3
"""
lanaai_daemon.py — lanaai 持仓监控常驻守护进程
=================================================
用 Playwright 常驻 headless 浏览器，每 20 秒拉一次币安 Square 页面，
检测到新建仓/平仓立即推送到 Telegram。

延迟: ~10-15 秒（页面渲染 + 检测 + 推送）

用法:
  python3 lanaai_daemon.py                     # 前台运行
  nohup python3 lanaai_daemon.py &             # 后台运行
  python3 lanaai_daemon.py --test              # 抓一次页面显示
  python3 lanaai_daemon.py --check-browser     # 检查 Playwright 浏览器状态

环境变量 (可选，自动从 Hermes .env 回退):
  LANAAI_TG_BOT_TOKEN
  LANAAI_TG_CHAT_ID
"""

import json, os, re, hashlib, sys, time, logging, urllib.request, urllib.parse
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("lanaai_daemon")

# ── 配置 ──────────────────────────────────────────────────────────────────────
USERNAME = "lanaai"
PAGE_URL = "https://www.binance.com/en/square/profile/lanaai"
STATE_DIR = Path.home() / ".lanaai_monitor"
SNAPSHOT_FILE = STATE_DIR / "snapshot.json"
SEEN_TRADES_FILE = STATE_DIR / "seen_trades.json"
POLL_INTERVAL = 20          # 每 20 秒轮询一次
PAGE_LOAD_TIMEOUT = 12000   # 页面加载超时 12s
PAGE_WAIT_MS = 4000         # 等待 JS 渲染
_HERMES_ENV = Path("/root/hermes-agent/.env")

# ── TG 配置 ───────────────────────────────────────────────────────────────────
def _load_tg_config() -> tuple:
    token = os.environ.get("LANAAI_TG_BOT_TOKEN", "")
    chat_id = os.environ.get("LANAAI_TG_CHAT_ID", "")
    if not token and _HERMES_ENV.exists():
        for line in _HERMES_ENV.read_text().splitlines():
            s = line.strip()
            if s.startswith("TELEGRAM_BOT_TOKEN="):
                token = s.split("=", 1)[1].strip().strip("\"'")
            elif s.startswith("TELEGRAM_ALLOWED_USERS="):
                chat_id = s.split("=", 1)[1].strip().strip("\"'")
    return token, chat_id

TG_TOKEN, TG_CHAT = _load_tg_config()

# ── 交易解析 ──────────────────────────────────────────────────────────────────

TRADE_PAT = re.compile(
    r"([A-Z0-9]{2,20}USDT)\s*\n\s*Perp\s*\n\s*"
    r"(?:B\s*\n\s*)?"
    r"(Opening|Closed)\s*(?:Long\s*)?\s*\n\s*"
    r"(?:Unrealized\s*)?"
    r"PNL\s*\n\s*"
    r"([+-]?[\d,]+(?:\.\d+)?)USDT"
)


def parse_trades(text: str) -> list[dict]:
    if not text:
        return []
    trades = []
    for m in TRADE_PAT.finditer(text):
        symbol, status, pnl_str = m.group(1), m.group(2), m.group(3).replace(",", "")
        try:
            pnl = float(pnl_str)
        except ValueError:
            pnl = 0.0
        ctx = text[max(0, m.start() - 60):m.start()]
        side = "Long" if "Long" in ctx else None
        tid = hashlib.md5(f"{symbol}:{pnl}:{ctx[-30:]}".encode()).hexdigest()[:16]
        trades.append({
            "symbol": symbol, "status": status, "side": side, "pnl": pnl,
            "trade_id": tid, "timestamp": int(time.time()),
        })
    # dedup
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
            positions[t["symbol"]] = {
                "side": t["side"], "pnl": t["pnl"],
                "last_seen": t["timestamp"], "trade_id": t["trade_id"],
            }
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
            events.append({
                "type": "open", "symbol": sym, "side": pos.get("side", "Unknown"),
                "pnl": pos["pnl"], "trade_id": pos["trade_id"],
            })
    for sym, pos in prev_p.items():
        if sym not in curr_p and pos["trade_id"] not in seen:
            events.append({
                "type": "close", "symbol": sym, "side": pos.get("side", "Unknown"),
                "pnl": pos["pnl"], "trade_id": pos["trade_id"],
            })
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
    data = urllib.parse.urlencode({
        "chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    try:
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data=data, method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            ok = json.loads(resp.read()).get("ok")
            if ok:
                logger.info(f"TG 推送成功: {event['type']} {event['symbol']}")
            return bool(ok)
    except Exception as e:
        logger.warning(f"TG 发送异常: {e}")
        return False


# ── Playwright 引擎 ───────────────────────────────────────────────────────────

class PlaywrightEngine:
    """封装 Playwright headless 浏览器，支持页面刷新和文本提取。"""

    def __init__(self):
        self._browser = None
        self._page = None
        self._init_ok = False

    def init(self) -> bool:
        """启动浏览器并打开页面"""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error("Playwright not installed: pip install playwright")
            return False

        try:
            self._pw = sync_playwright().start()
            # Try chromium first, fall back to firefox
            try:
                btype = "chromium"
                self._browser = self._pw.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
                )
            except Exception as e:
                logger.warning(f"chromium 启动失败 ({e}), 尝试 firefox...")
                btype = "firefox"
                self._browser = self._pw.firefox.launch(headless=True)

            self._page = self._browser.new_page(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 4000},
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )
            self._goto_page()
            self._init_ok = True
            logger.info(f"Playwright 浏览器已启动 ({btype})")
            return True
        except Exception as e:
            logger.error(f"Playwright 初始化失败: {e}")
            if self._pw:
                try: self._pw.stop()
                except: pass
            return False

    def _goto_page(self, retries=2):
        """打开或刷新页面"""
        for attempt in range(retries + 1):
            try:
                self._page.goto(PAGE_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="domcontentloaded")
                self._page.wait_for_timeout(PAGE_WAIT_MS)
                # scroll 触发懒加载
                self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                self._page.wait_for_timeout(1000)
                self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                self._page.wait_for_timeout(1000)
                return True
            except Exception as e:
                logger.warning(f"页面加载尝试 {attempt+1}/{retries+1} 失败: {e}")
                if attempt < retries:
                    time.sleep(2)
        return False

    def refresh(self) -> str | None:
        """刷新页面（重新加载）并返回 innerText"""
        if not self._page:
            return None
        try:
            ok = self._goto_page()
            if not ok:
                return None
            text = self._page.evaluate("document.body.innerText")
            if text is None:
                return None
            text = str(text)
            logger.debug(f"页面文本: {len(text)} chars")
            return text
        except Exception as e:
            logger.warning(f"页面刷新失败: {e}")
            return None

    def close(self):
        """关闭浏览器"""
        if self._browser:
            try: self._browser.close()
            except: pass
        if self._pw:
            try: self._pw.stop()
            except: pass
        self._init_ok = False


# ── 主循环 ────────────────────────────────────────────────────────────────────

def run_once(engine: PlaywrightEngine, test_mode: bool = False):
    """单次抓取+检测"""
    text = engine.refresh()
    if not text:
        logger.warning("页面获取失败, 等待下次轮询")
        return

    trades = parse_trades(text)
    logger.info(f"解析到 {len(trades)} 条交易 (含已平仓)")

    if test_mode:
        _print_trades(trades)
        return

    cur = build_snapshot(trades) if trades else {"positions": {}, "timestamp": int(time.time())}
    prev = load_json(SNAPSHOT_FILE, {"positions": {}, "timestamp": 0})
    seen = set(load_json(SEEN_TRADES_FILE, []))

    events = detect_events(cur, prev, seen)

    if not events:
        logger.info("无新交易事件")
    else:
        logger.info(f"检测到 {len(events)} 个事件:")
        for ev in events:
            msg = fmt_msg(ev)
            logger.info(f"  发送: {ev['type']} {ev['symbol']} PnL=${ev['pnl']:,.2f}")
            if send_tg(msg):
                seen.add(ev["trade_id"])

    # 保存状态
    if events:
        save_json(SEEN_TRADES_FILE, list(seen))
    if trades:
        save_json(SNAPSHOT_FILE, cur)


def _print_trades(trades: list[dict]):
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


def check_browser():
    """检查 Playwright 浏览器安装状态"""
    try:
        import playwright
        print(f"Playwright: OK (imported)")
    except ImportError:
        print("❌ Playwright 未安装 (pip install playwright)")
        return False

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            try:
                b = p.chromium.launch(headless=True, args=["--no-sandbox"])
                print("✅ Chromium: OK")
                b.close()
            except Exception as e:
                print(f"❌ Chromium: {e}")
            try:
                b = p.firefox.launch(headless=True)
                print("✅ Firefox: OK")
                b.close()
            except Exception as e:
                print(f"❌ Firefox: {e}")
        print("\n至少需要一个浏览器引擎运行")
        return True
    except Exception as e:
        print(f"❌ Playwright 运行时错误: {e}")
        return False


def main():
    if "--check-browser" in sys.argv:
        check_browser()
        return

    test_mode = "--test" in sys.argv

    # 检查浏览器（测试模式时跳过状态打印）
    if not test_mode:
        check_browser()

    if not TG_TOKEN or not TG_CHAT:
        logger.error("TG 未配置，无法推送")
        if not test_mode:
            sys.exit(1)

    engine = PlaywrightEngine()
    if not engine.init():
        logger.error("Playwright 引擎启动失败")
        sys.exit(1)

    try:
        if test_mode:
            run_once(engine, test_mode=True)
        else:
            logger.info(f"开始常驻监控，每 {POLL_INTERVAL}s 轮询一次")
            run_once(engine)  # 立即跑第一次
            while True:
                time.sleep(POLL_INTERVAL)
                run_once(engine)
    except KeyboardInterrupt:
        logger.info("守护进程退出")
    finally:
        engine.close()


if __name__ == "__main__":
    main()
