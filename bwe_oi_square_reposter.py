#!/usr/bin/env python3
"""
BWE OI 异动 → 币安广场自动转发脚本
======================================
用 agent-browser 从 TG 频道 @BWE_OI_Price_monitor 抓 OI 异动消息，
解析关键数据，发到币安 Square。

用法:
  python3 bwe_oi_square_reposter.py          # 正常模式
  python3 bwe_oi_square_reposter.py --test   # 仅打印，不发帖
  python3 bwe_oi_square_reposter.py --force  # 忽略去重，强制发帖

输出:
  正常模式 → 检测到新消息则发帖，否则 SILENT
"""

import json, os, re, subprocess, sys, time, hashlib, urllib.request, urllib.parse, logging
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bwe_oi_reposter")

# ── 配置 ──────────────────────────────────────────────────────
STATE_DIR = Path.home() / ".bwe_oi_monitor"
SENT_FILE = STATE_DIR / "sent_ids.json"
AGENT_BROWSER = "/usr/bin/agent-browser"
PAGE_URL = "https://t.me/s/BWE_OI_Price_monitor"
# 项目 root
REPO_DIR = Path(__file__).resolve().parent

# ── TG 频道抓取 ──────────────────────────────────────────────


def fetch_recent_messages(max_scrolls: int = 5) -> str:
    """用 agent-browser 打开 TG 频道 web preview 并获取文本"""
    try:
        logger.info("打开 TG 频道...")
        # Clean up any lingering sessions first
        subprocess.run([AGENT_BROWSER, "close"], capture_output=True, timeout=5)
        time.sleep(1)

        r = subprocess.run(
            [AGENT_BROWSER, "open", PAGE_URL],
            capture_output=True, timeout=30,
        )
        if r.returncode != 0:
            logger.warning(f"open 失败: {r.stderr[:200]}")
            return ""
        time.sleep(3)

        # Scroll down a few times to load more messages
        for i in range(max_scrolls):
            r = subprocess.run(
                [AGENT_BROWSER, "eval", "window.scrollTo(0, document.body.scrollHeight)"],
                capture_output=True, timeout=10,
            )
            time.sleep(1.5)

        r = subprocess.run(
            [AGENT_BROWSER, "eval", "document.body.innerText"],
            capture_output=True, text=True, timeout=15,
        )
        subprocess.run([AGENT_BROWSER, "close"], capture_output=True, timeout=5)

        if r.returncode != 0:
            logger.warning(f"eval 失败: {r.stderr[:200]}")
            return ""

        text = r.stdout.strip()
        if text.startswith('"') and text.endswith('"'):
            try:
                text = json.loads(text)
            except json.JSONDecodeError:
                pass

        # Normalize escaped newlines
        if "\\n" in text and "\\\\n" not in text:
            text = text.replace("\\n", "\n")

        logger.info(f"获取到页面文本: {len(text)} chars")
        return text
    except Exception as e:
        logger.warning(f"获取页面失败: {e}")
        return ""


# ── OI 消息解析 ──────────────────────────────────────────────

def parse_oi_messages(text: str) -> list[dict]:
    """从页面文本中解析 BWE OI 异动消息"""
    if not text:
        return []

    messages = []

    # Pattern: 币种 OI/Price 异动标准格式
    # e.g.:
    # 🇨🇳 🟢 ZKJUSDT 币安未平仓合约量 +26.9%
    # 过去 3600 秒价格上涨 23.8%
    # ...
    # 24H Price Change: +44.1%
    # 💰 市值 ZKJ MarketCap: 9M
    #
    # There's a dual CN/EN format. We match the CN section primarily.

    # Split on emoji-separated blocks (each message starts with 🇨🇳 or 🇺🇸)
    # First, normalize the text by joining broken lines
    blocks = re.split(r"(?=[\U0001F1E8\U0001F1F3])", text)  # split on 🇨
    # Better approach: split on the message boundary marker
    # Messages start with "🇨🇳" or "🇺🇸" for country flags

    # Use a line-based approach to extract OI messages
    lines = text.split("\n")
    i = 0
    current_msg_lines = []
    in_msg = False

    while i < len(lines):
        line = lines[i].strip()
        if not line:
            if in_msg:
                current_msg_lines.append(line)
            i += 1
            continue

        # Check for message start: flag emoji followed by symbols
        if re.match(r"[\U0001F1E8\U0001F1F3][\U0001F1F3\U0001F1FA]?\s*(?:🟢|🔴|🟡|🟣)\s*[A-Z0-9]{2,20}USDT", line):
            if in_msg and current_msg_lines:
                msg_text = "\n".join(current_msg_lines).strip()
                parsed = _parse_single_oi_message(msg_text)
                if parsed:
                    messages.append(parsed)
            current_msg_lines = [line]
            in_msg = True
        elif in_msg:
            current_msg_lines.append(line)
        i += 1

    # Last message
    if in_msg and current_msg_lines:
        msg_text = "\n".join(current_msg_lines).strip()
        parsed = _parse_single_oi_message(msg_text)
        if parsed:
            messages.append(parsed)

    # Deduplicate by content hash
    seen = set()
    deduped = []
    for m in messages:
        key = (m["symbol"], m["oi_change_pct"], m["price_change_pct"], m.get("timestamp", ""))
        if key not in seen:
            seen.add(key)
            deduped.append(m)

    logger.info(f"解析到 {len(deduped)} 条 OI 异动消息")
    return deduped


def _parse_single_oi_message(text: str) -> Optional[dict]:
    """解析单条 OI 消息"""
    if not text:
        return None

    # Extract symbol
    sym_m = re.search(r"([A-Z0-9]{2,20})USDT", text)
    if not sym_m:
        return None
    symbol = sym_m.group(1) + "USDT"

    # Extract OI change %
    oi_m = re.search(
        r"未平仓合约量\s*(?:增长|增长为|)\s*([+-]?\d+\.?\d*)%",
        text,
    )
    if not oi_m:
        # Try "openinterest" (EN section)
        oi_m = re.search(
            r"openinterest\s*[+-]\d+\.?\d*%",
            text,
            re.IGNORECASE,
        )
        if oi_m:
            oi_str = re.search(r"([+-]\d+\.?\d*)%", oi_m.group())
            oi_pct = float(oi_str.group(1)) if oi_str else 0.0
        else:
            oi_pct = 0.0
    else:
        oi_str = oi_m.group(1)
        # Handle "增长 28.2%" -> strip the word
        oi_clean = re.sub(r"[^\d+\-.]", "", oi_str)
        try:
            oi_pct = float(oi_clean)
        except ValueError:
            oi_pct = 0.0

    # Extract price change %
    price_m = re.search(
        r"(?:价格|Price)\s*(?:上涨|下跌|在过去|在 past|)\s*(?:上涨|下跌|)\s*[+-]?\d+\.?\d*%",
        text,
        re.IGNORECASE,
    )
    if not price_m:
        price_pct = 0.0
    else:
        price_str = re.search(r"([+-]?\d+\.?\d*)%", price_m.group())
        price_pct = float(price_str.group(1)) if price_str else 0.0

    # Normalize sign: all BWE messages are green (🟢), price is always positive
    # when paired with OI increase. They show "上涨 23.8%" not "-23.8%"
    if price_pct > 0 and "下跌" in text[:text.find(symbol) + 20] if symbol in text else False:
        price_pct = -price_pct

    # Extract OI value
    oi_val_m = re.search(
        r"未平仓合约量[：:]\s*\$?(\d+\.?\d*)\s*(万|亿|万|M|B)?",
        text,
    )
    if not oi_val_m:
        oi_val_m = re.search(r"OI[：:]\s*\$?(\d+\.?\d*)\s*(M|B)?", text)
    oi_value = 0.0
    oi_unit = ""
    if oi_val_m:
        oi_value = float(oi_val_m.group(1))
        oi_unit = oi_val_m.group(2) or ""

    # Market cap
    mc_m = re.search(
        r"市值\s+\w+\s+MarketCap[：:]\s*\$?(\d+\.?\d*)\s*(M|亿|万)?",
        text,
    )
    market_cap = 0.0
    if mc_m:
        market_cap = float(mc_m.group(1))

    # 24h price change
    chg24_m = re.search(
        r"24[hH]\s*(?:价格变动|Price\s*Change)[：:]\s*([+-]?\d+\.?\d*)%",
        text,
    )
    chg24 = 0.0
    if chg24_m:
        chg24 = float(chg24_m.group(1))

    # Content hash for dedup
    content_hash = hashlib.md5(text.encode()).hexdigest()[:16]

    return {
        "symbol": symbol,
        "oi_change_pct": oi_pct,
        "price_change_pct": price_pct,
        "oi_value": oi_value,
        "oi_unit": oi_unit,
        "market_cap": market_cap,
        "change_24h_pct": chg24,
        "direction": "up" if price_pct >= 0 else "down",
        "content_hash": content_hash,
        "timestamp": int(time.time()),
        "_raw": text,
    }


# ── Square 帖子生成 ──────────────────────────────────────────


def make_square_post(data: dict) -> str:
    """将 OI 异动数据转为币安广场帖子"""
    symbol = data["symbol"].replace("USDT", "")
    oi_pct = data["oi_change_pct"]
    px_pct = data["price_change_pct"]
    oi_val = data["oi_value"]
    oi_unit = data["oi_unit"]
    chg24 = data["change_24h_pct"]
    mc = data["market_cap"]

    direction_emoji = "🟢" if data["direction"] == "up" else "🔴"
    arrow = "📈" if px_pct >= 0 else "📉"

    # Build OI value string
    oi_str = f"${oi_val:.1f}{oi_unit}" if oi_val > 0 and oi_unit else (f"${oi_val:.1f}" if oi_val > 0 else "")

    lines = [
        f"{direction_emoji} ${symbol} OI 出现异动",
        "",
        f"• 未平仓合约量：+{oi_pct:.1f}%（{oi_str}）" if oi_pct >= 0 else f"• 未平仓合约量：{oi_pct:.1f}%（{oi_str}）",
        f"{arrow} 价格（1h）：{'%+.1f' % px_pct if px_pct >= 0 else '%.1f' % px_pct}%",
        f"• 24h 价格变动：{'%+.1f' % chg24 if chg24 >= 0 else '%.1f' % chg24}%",
    ]
    if mc > 0:
        lines.append(f"• 市值：${mc:.0f}M")

    lines.extend([
        "",
        f"$BNB $BSB",
        f"#OI异动 #${symbol} #合约数据 #交易信号",
        "",
        "⚠️ 数据来源：TradingView/BWE OI Monitor\n仅供信息参考，非投资建议。",
    ])

    return "\\n".join(lines)


# ── 状态管理 ────────────────────────────────────────────────


def load_sent_ids() -> set[str]:
    if SENT_FILE.exists():
        try:
            return set(json.loads(SENT_FILE.read_text()))
        except Exception:
            pass
    return set()


def save_sent_ids(ids: set[str]):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    SENT_FILE.write_text(json.dumps(sorted(ids), indent=2))


# ── 主流程 ──────────────────────────────────────────────────


def main():
    logger.info("BWE OI 异动 → Square 转发 启动")

    force = "--force" in sys.argv

    # 1. 获取页面
    text = fetch_recent_messages(max_scrolls=4)
    if not text:
        logger.error("页面获取失败")
        sys.exit(1)

    # 2. 解析消息
    messages = parse_oi_messages(text)
    if not messages:
        logger.info("未解析到 OI 异动消息")
        print("[SILENT] no messages parsed")
        return

    # 3. 差分发帖
    sent_ids = load_sent_ids()
    new_messages = [m for m in messages if m["content_hash"] not in sent_ids]

    if not new_messages:
        logger.info(f"无新 OI 异动消息（已有 {len(sent_ids)} 条已发送）")
        print("[SILENT] nothing new")
        return

    logger.info(f"发现 {len(new_messages)} 条新 OI 异动")

    if "--test" in sys.argv:
        print(f"\n{'='*50}")
        print(f"📊 BWE OI 异动 — 测试模式（{len(new_messages)} 条新消息）")
        print(f"{'='*50}")
        for m in new_messages:
            print(f"\n{'─'*40}")
            print(f"  币种: {m['symbol']}")
            print(f"  OI变化: +{m['oi_change_pct']:.1f}%")
            print(f"  价格变化: {'%+.1f' % m['price_change_pct']}%")
            print(f"  24h变化: {'%+.1f' % m['change_24h_pct']}%")
            print(f"  OI值: ${m['oi_value']:.1f}{m['oi_unit']}")
            print(f"  市值: ${m['market_cap']:.0f}M")
            print(f"\n📝 预备帖子:")
            print(make_square_post(m))
            print(f"{'─'*40}")
        return

    # 4. 记录新消息（不再发帖，只监控记录）
    posted_count = 0
    for msg in new_messages:
        post_content = make_square_post(msg)
        symbol_short = msg["symbol"].replace("USDT", "")
        msg_desc = f"{symbol_short} OI {'%+.1f' % msg['oi_change_pct']}%"
        logger.info(f"新 OI 异动: {msg_desc}")
        logger.info(f"  帖子内容预览:\\n{post_content[:200]}...")
        
        sent_ids.add(msg["content_hash"])
        posted_count += 1

    save_sent_ids(sent_ids)
    logger.info(f"本轮完成: {posted_count} 条新 OI 异动（仅TG记录，不再发Square）")
    posted_str = ", ".join(
        f"{m['symbol'].replace('USDT','')} OI+{m['oi_change_pct']:.1f}%"
        for m in new_messages[:posted_count]
    ) if posted_count > 0 else "无"
    print(f"[MONITORED] {posted_str}" if posted_count > 0 else "[SILENT] no new messages")


if __name__ == "__main__":
    import logging
    main()
