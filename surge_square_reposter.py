#!/usr/bin/env python3
"""
surge_square_reposter.py — 策略信号 → 币安广场转发

从 surge_scanner_v2.py 读取 A/B 级信号的最新 JSON 输出，
转换为适合币安广场的中文内容帖并推送。
推送到 @MetaFreddy 频道（-1002826667582）及币安广场。

仅推送 A 级（最优）信号，防噪音。

运行: python3 surge_square_reposter.py
"""
import json
import os
import sys
import time
import logging
from datetime import datetime, timezone, timedelta, date
from pathlib import Path

# ── 路径 ───────────────────────────────────────────────────
REPO_DIR = Path("/root/binance-square-agent")
DATA_DIR = REPO_DIR / "data"
SENT_FILE = DATA_DIR / "surge_square_sent.json"
SIGNAL_CACHE = DATA_DIR / "surge_signals_cache.json"

# ── 日志 ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("surge_square")


def load_surge_results() -> list[dict]:
    """从 surge_scanner_v2.py 的缓存文件加载信号"""
    if not SIGNAL_CACHE.exists():
        logger.warning("信号缓存不存在: %s", SIGNAL_CACHE)
        return []
    try:
        data = json.loads(SIGNAL_CACHE.read_text())
        if isinstance(data, list):
            return data
        # 可能是 dict 包着 results
        return data.get("results", data.get("signals", []))
    except Exception as e:
        logger.warning("解析信号缓存失败: %s", e)
        return []


def load_sent() -> set[str]:
    if SENT_FILE.exists():
        try:
            return set(json.loads(SENT_FILE.read_text()))
        except Exception:
            pass
    return set()


def save_sent(ids: set[str]):
    SENT_FILE.parent.mkdir(parents=True, exist_ok=True)
    SENT_FILE.write_text(json.dumps(sorted(ids), ensure_ascii=False, indent=2))


def format_square_post(r: dict) -> str:
    """将 A/B 级信号格式化为币安广场帖"""
    sym_raw = r.get("sym", "UNKNOWN")
    sym = sym_raw.replace("USDT", "")
    price = r.get("price", 0)
    chg = r.get("chg24h", 0)
    grade = r.get("grade", "D")
    score = r.get("score", 0)
    vol = r.get("vol_24h", 0)
    cx = r.get("cross_exchange", 0)
    ctf = r.get("cross_timeframe", 0)
    signals = r.get("signals", [])
    patterns = r.get("patterns", [])
    entry_advice = r.get("entry_advice", "")
    exit_advice = r.get("exit_advice", "")
    rapid = r.get("rapid", False)

    # 成交量格式化
    vol_str = f"{vol / 1e6:.1f}M" if vol >= 1e6 else f"{vol / 1000:.0f}K" if vol >= 1000 else f"{vol:.0f}"

    chg_emoji = "🟢" if chg >= 0 else "🔴"

    lines = []

    # ── 标题 ──
    if rapid:
        header = f"🚀 ${sym} 快速突破信号 — 三所策略扫描"
    else:
        header = f"📊 ${sym} {'A' if grade == 'A' else 'B'}级策略信号 — 三所三时间框扫描"
    lines.append(header)
    lines.append("")

    # ── 核心数据 ──
    lines.append(f"💵 现价: ${price:.6f}  {chg_emoji}24h: {chg:+.2f}%")
    lines.append(f"📊 24h成交量: {vol_str}  |  评分: {score}/100")
    lines.append(f"🔄 覆盖 {cx} 所 / {ctf} 时间框")
    lines.append("")

    # ── 信号列表 ──
    if signals:
        lines.append("🎯 检测到的信号：")
        for s in signals[:5]:
            s_clean = s.replace("*", "").replace("_", " ")
            lines.append(f"  • {s_clean}")
        lines.append("")

    # ── 形态 ──
    if patterns:
        lines.append("📐 K线形态：")
        for p in patterns[:3]:
            p_clean = p.replace("*", "")
            lines.append(f"  • {p_clean}")
        lines.append("")

    # ── 交易建议 ──
    if entry_advice:
        entry_clean = entry_advice.replace("*", "").replace("_", " ")
        lines.append(f"✅ 入场参考：{entry_clean}")
    if exit_advice:
        exit_clean = exit_advice.replace("*", "").replace("_", " ")
        lines.append(f"❌ 止损参考：{exit_clean}")
    lines.append("")

    # ── 尾部 ──
    beijing = datetime.now(timezone(timedelta(hours=8)))
    lines.append(f"🤖 策略扫描 v3 · {beijing.strftime('%m-%d %H:%M')} UTC+8")
    lines.append("")
    lines.append("$BSB #策略信号 #合约交易 #技术分析")

    return "\n".join(lines)


def send_tg(text: str, bot_token: str, chat_id: str):
    """发送到 TG 频道"""
    import requests
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        if resp.status_code == 200:
            return True
        logger.warning("TG发送失败 %d: %s", resp.status_code, resp.text[:200])
    except Exception as e:
        logger.warning("TG异常: %s", e)
    return False


# ── 主流程 ──


def main():
    from dotenv import load_dotenv
    dotenv_path = REPO_DIR / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path)

    tg_token = os.environ.get("TG_BOT_TOKEN", "")
    tg_channel = os.environ.get("SQUARE_CHANNEL_ID", "-1002826667582")

    results = load_surge_results()
    if not results:
        logger.warning("无信号数据")
        # 尝试直接运行扫描先？
        return

    # 过滤 A/B 级信号
    grade_signals = {
        "A": [r for r in results if r.get("grade") == "A"],
        "B": [r for r in results if r.get("grade") == "B"],
    }

    if not any(grade_signals.values()):
        logger.info("无 A/B 级信号，跳过推送")
        return

    for grade in ("A", "B"):
        logger.info(f"找到 {len(grade_signals[grade])} 个 {grade} 级信号")

    sent = load_sent()
    today_key = date.today().isoformat()

    posted = 0
    for grade in ("A", "B"):
        for r in grade_signals[grade]:
            sym = r.get("sym", "")
            signal_id = f"{sym}_{today_key}"

            if signal_id in sent:
                logger.info(f"跳过今日已推送: {sym}")
                continue

            post = format_square_post(r)

            # TG 频道记录（不再推送币安广场）
            if tg_token and tg_channel:
                send_tg(f"📢 策略信号\n\n{post}", tg_token, tg_channel)
                sent.add(signal_id)
                save_sent(sent)
                posted += 1
                logger.info(f"✅ {sym} ({grade}级) TG推送成功")
                time.sleep(1)

    logger.info(f"本轮推送完成，共推送 {posted} 个信号到TG")


if __name__ == "__main__":
    main()
