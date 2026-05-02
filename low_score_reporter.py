#!/usr/bin/env python3
"""
low_score_reporter.py — 低分信号汇报机器人
============================================
读取 low_score_signals.json，格式化推送至 Telegram 频道。
支持 --dry 预览模式。

完整格式示例：
━━━━ ⚡ 低频信号池 ═══════════════
🕐 2026-05-01 23:00 UTC
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🟢 UBUSDT  +88%  $0.1318  [OKX+GATE+MEXC]
  评分: 35(C) | 成交量: 320K
  信号: EMA9/26多头, MA88支撑, 空头轧空...

🔴 BUSDT  +119%  $0.2889  [GATE+MEXC]
  评分: 23(D) | 成交量: 3.6M
  信号: 成交量6x, OBV突破...

用法:
  python3 low_score_reporter.py              # 正常推送
  python3 low_score_reporter.py --dry        # 预览不发
  python3 low_score_reporter.py --top 20     # 最多推送20条 (默认15)
  python3 low_score_reporter.py --min-score 20  # 最低分数过滤
  python3 low_score_reporter.py --reset      # 推送后清空缓存（防止重复推送）
"""

import json
import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

import requests

# ── 路径 ───────────────────────────────────────────────────
DATA_DIR = Path("/root/binance-square-agent/data")
LOW_SCORE_CACHE = DATA_DIR / "low_score_signals.json"

# ── Telegram 配置 (从 surge_scanner_v2.py 复用) ────────────
# 可用独立的 bot / chat id，若不设置则复用 surge scanner 的
LOWSCORE_BOT_TOKEN = os.environ.get(
    "LOWSCORE_BOT_TOKEN",
    "875782...b7V4",  # fallback 到 scanner 的 token
)
LOWSCORE_CHAT_ID = os.environ.get("LOWSCORE_CHAT_ID", "1077054086")

# ── 日志 ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("low_score_reporter")


def load_json(path: Path) -> list[dict]:
    """加载 JSON 文件"""
    if not path.exists():
        logger.warning("文件不存在: %s", path)
        return []
    try:
        data = json.loads(path.read_text())
        if isinstance(data, list):
            return data
        return data.get("results", data.get("signals", []))
    except Exception as e:
        logger.warning("解析 %s 失败: %s", path, e)
        return []


def save_json(path: Path, data: list[dict]) -> None:
    """安全写入 JSON 文件"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, default=str)
    )


def format_exchanges(r: dict) -> str:
    """格式化交易所列表，如 [OKX+GATE]"""
    # 直接检查 cross_exchange 字段
    cx = r.get("cross_exchange", 0)
    if cx >= 2:
        # 如果有多个交易所，构造简短标签
        exchanges = []
        # 尝试从 signals_bear/signals 中推断交易所 (通常没有)
        # 改用已知的跨所标签
        return f"[{cx} exchanges]"
    elif cx == 1:
        return "[single]"
    return ""


def format_signal_card(r: dict) -> str:
    """格式化单个信号卡片"""
    sym_raw = r.get("sym", "UNKNOWN")
    sym_short = sym_raw.replace("USDT", "")
    price = r.get("price", 0)
    chg = r.get("chg24h", 0)
    score = r.get("score", 0)
    grade = r.get("grade", "D")
    vol = r.get("vol_24h", 0)
    signals = r.get("signals", [])
    patterns = r.get("patterns", [])

    # 成交量格式化
    if vol >= 1_000_000:
        vol_str = f"{vol/1_000_000:.1f}M"
    elif vol >= 1000:
        vol_str = f"{vol/1000:.0f}K"
    else:
        vol_str = f"{vol:.0f}"

    # 等级图标
    grade_icon = {"A": "🟢", "B": "🟢", "C": "🟠", "D": "⚪"}.get(grade, "⚪")

    # 涨幅图标
    chg_icon = "🟢" if chg >= 0 else "🔴"

    # 交易所信息
    exch_info = format_exchanges(r)
    rapid_tag = " 🚀" if r.get("rapid") else ""

    # 构建卡片
    lines = []

    # ── 第一行: 币种 + 涨幅 + 价格 + 交易所 ──
    lines.append(
        f"{grade_icon} *${sym_short}*{rapid_tag}  {chg_icon}`{chg:+.2f}%`  "
        f"`${price:.4f}`{f'  {exch_info}' if exch_info else ''}"
    )

    # ── 第二行: 评分 + 成交量 ──
    lines.append(
        f"  评分: `{score}({grade})` | 成交量: `{vol_str}`"
    )

    # ── 第三行: 信号摘要（前5个） ──
    if signals:
        # 去重，保留前5个有意义的
        seen = set()
        unique_sigs = []
        for s in signals:
            if s not in seen:
                seen.add(s)
                unique_sigs.append(s)
        sig_summary = ", ".join(unique_sigs[:5])
        if len(unique_sigs) > 5:
            sig_summary += f"... (+{len(unique_sigs) - 5})"
        lines.append(f"  信号: `{sig_summary}`")

    # ── 第四行: 模式识别 ──
    if patterns:
        pat_str = ", ".join(patterns)
        lines.append(f"  模式: `{pat_str}`")

    # ── 入场建议（如有） ──
    entry = r.get("entry_advice", "")
    if entry:
        lines.append(f"  💡 {entry}")

    return "\n".join(lines)


def format_summary(signals: list[dict]) -> str:
    """格式化顶部摘要统计"""
    total = len(signals)
    grade_counts = defaultdict(int)
    for r in signals:
        grade_counts[r.get("grade", "D")] += 1

    avg_score = sum(r.get("score", 0) for r in signals) / total if total else 0
    max_chg = max((r.get("chg24h", 0) for r in signals), default=0)
    min_chg = min((r.get("chg24h", 0) for r in signals), default=0)

    lines = []
    lines.append("━━━━ ⚡ 低频信号池 ═══════════════")
    lines.append(
        f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC"
    )
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")

    # 统计行
    parts = []
    if grade_counts.get("C", 0):
        parts.append(f"🟠C={grade_counts['C']}")
    if grade_counts.get("D", 0):
        parts.append(f"⚪D={grade_counts['D']}")
    parts.append(f"共{total}个信号")
    lines.append(f"📊 `{'  '.join(parts)}`")
    lines.append(
        f"📈 平均分: `{avg_score:.1f}`  |  涨幅区间: `{min_chg:+.1f}% ~ {max_chg:+.1f}%`"
    )
    lines.append("")

    return "\n".join(lines)


def build_full_message(signals: list[dict], top_n: int = 15) -> str:
    """
    构建完整推送消息：标题 + 统计 + 每币卡片
    """
    # 按 score 降序排列
    sorted_sigs = sorted(signals, key=lambda r: -r.get("score", 0))

    # 只取 top N
    display = sorted_sigs[:top_n]

    lines = []

    # ── 标题统计 ──
    lines.append(format_summary(sorted_sigs))

    # ── 每币卡片 ──
    for i, r in enumerate(display, 1):
        card = format_signal_card(r)
        lines.append(card)
        lines.append("")

    # ── 尾部 ──
    if len(sorted_sigs) > top_n:
        lines.append(f"... 还有 {len(sorted_sigs) - top_n} 个信号未显示")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    ts = datetime.now(timezone.utc).strftime("%m-%d %H:%M UTC")
    lines.append(f"🤖 Low Score Pool · {ts}")

    return "\n".join(lines)


def send_tg(text: str, dry: bool = False) -> bool:
    """发送消息到 Telegram，dry 模式只打印"""
    if dry:
        print(f"\n{'='*50}")
        print("[DRY MODE — 以下为推送预览]")
        print(f"{'='*50}")
        print(text)
        print(f"{'='*50}")
        return True

    token = LOWSCORE_BOT_TOKEN
    chat = LOWSCORE_CHAT_ID
    if not token or not chat:
        logger.warning("LOWSCORE_BOT_TOKEN 或 LOWSCORE_CHAT_ID 未设置，无法推送")
        return False

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning("TG push failed: %d %s", resp.status_code, resp.text[:200])
            return False
        logger.info("✅ TG 推送成功")
        return True
    except Exception as e:
        logger.warning("TG exception: %s", e)
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="低分信号汇报 — 推送 score < 50 的信号到 Telegram"
    )
    parser.add_argument("--dry", action="store_true", help="预览模式，不发送 TG")
    parser.add_argument(
        "--top", type=int, default=15,
        help="最多推送信号数 (默认 15)"
    )
    parser.add_argument(
        "--min-score", type=int, default=0,
        help="最低分数过滤 (默认 0，全部推送)"
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="推送后清空 low_score_signals.json"
    )
    parser.add_argument(
        "--source", type=str, default=str(LOW_SCORE_CACHE),
        help=f"信号源文件路径 (默认: {LOW_SCORE_CACHE})"
    )
    args = parser.parse_args()

    source_path = Path(args.source)

    # 1. 读取缓存
    signals = load_json(source_path)
    if not signals:
        logger.info("没有低分信号需要汇报")
        return

    logger.info("读取到 %d 条低分信号", len(signals))

    # 2. 按时间排序（虽然有去重，但保留最近的表现）
    # 实际上我们按 score 排，因为低分池关注全量

    # 3. 最低分数过滤
    if args.min_score > 0:
        signals = [r for r in signals if r.get("score", 0) >= args.min_score]
        logger.info("最低分数过滤 (>=%d) 后: %d 条", args.min_score, len(signals))
        if not signals:
            logger.info("过滤后无信号，跳过")
            return

    # 4. 构建推送消息
    message = build_full_message(signals, top_n=args.top)

    # 5. 发送
    if len(message) > 4000:
        logger.warning(
            "消息过长 (%d chars)，将截断为前 4000 字符",
            len(message),
        )
        message = message[:4000] + "\n\n... (截断)"

    success = send_tg(message, dry=args.dry)

    # 6. 推送成功后可选清空缓存
    if success and args.reset and not args.dry:
        save_json(source_path, [])
        logger.info("✅ 已清空低分信号缓存")


if __name__ == "__main__":
    main()
