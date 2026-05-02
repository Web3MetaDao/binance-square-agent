#!/usr/bin/env python3
"""
low_score_collector.py — 低分信号收集通道
=========================================
从 surge_signals_cache.json 中提取所有 score < 50（C级以下）的信号，
存入 low_score_signals.json 独立缓存，支持增量更新（按sym去重）。

用法:
  python3 low_score_collector.py              # 正常收集
  python3 low_score_collector.py --dry        # 预览数量，不写入
  python3 low_score_collector.py --source FILE  # 指定源文件
"""

import json
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone

# ── 路径 ───────────────────────────────────────────────────
DATA_DIR = Path("/root/binance-square-agent/data")
SOURCE_CACHE = DATA_DIR / "surge_signals_cache.json"
LOW_SCORE_CACHE = DATA_DIR / "low_score_signals.json"

# ── 过滤阈值 ──────────────────────────────────────────────
MAX_SCORE = 50  # 只收集 score < 50 的信号

# ── 日志 ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("low_score_collector")


def load_json(path: Path) -> list[dict]:
    """加载 JSON 文件，非存在或无效返回空列表"""
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


def dedup_by_sym(signals: list[dict]) -> list[dict]:
    """
    按 sym 去重，保留 score 最高的记录。
    如果 score 相同，保留 chg24h 绝对值更大的。
    """
    best: dict[str, dict] = {}
    for s in signals:
        sym = s.get("sym", "")
        if not sym:
            continue
        existing = best.get(sym)
        if existing is None:
            best[sym] = s
        else:
            # 取 score 更高的
            if s.get("score", 0) > existing.get("score", 0):
                best[sym] = s
            elif s.get("score", 0) == existing.get("score", 0):
                # score 相同，取涨幅更大的
                if abs(s.get("chg24h", 0)) > abs(existing.get("chg24h", 0)):
                    best[sym] = s
    return list(best.values())


def collect_low_score_signals(
    source_path: Path = SOURCE_CACHE,
    output_path: Path = LOW_SCORE_CACHE,
    dry: bool = False,
) -> list[dict]:
    """
    主逻辑：从源文件提取低分信号，合并到已有缓存，去重后保存。

    Returns:
        list[dict]: 筛选出的低分信号列表
    """
    # 1. 读取源信号
    source_signals = load_json(source_path)
    if not source_signals:
        logger.warning("源信号为空，无法收集")
        return []

    total_source = len(source_signals)
    logger.info("源信号总数: %d", total_source)

    # 2. 筛选 score < MAX_SCORE
    low_score = [s for s in source_signals if s.get("score", 999) < MAX_SCORE]
    logger.info(
        "低分信号 (score < %d): %d 个 (占比 %.1f%%)",
        MAX_SCORE, len(low_score),
        len(low_score) / total_source * 100 if total_source else 0,
    )

    if not low_score:
        logger.info("本轮没有新的低分信号")
        return []

    # 3. 加载已有缓存，合并
    existing = load_json(output_path)
    logger.info("现有低分缓存: %d 条", len(existing))

    # 合并：已有记录 + 新记录
    merged = existing + low_score

    # 4. 去重（按sym保留score最高的）
    deduped = dedup_by_sym(merged)
    deduped.sort(key=lambda r: -r.get("score", 0))

    logger.info(
        "去重后: %d 条 (新增 %d, 移除重复 %d)",
        len(deduped),
        len(deduped) - len(existing) if len(deduped) > len(existing) else len(low_score),
        len(merged) - len(deduped),
    )

    # 5. 保存或预览
    if dry:
        logger.info("🧪 DRY MODE — 不写入文件")
        print(f"\n{'='*50}")
        print(f"低分信号收集预览 (score < {MAX_SCORE})")
        print(f"源信号: {total_source} | 低分: {len(low_score)} | 缓存总计: {len(deduped)}")
        print(f"{'='*50}")
        for r in deduped[:10]:
            sym = r.get("sym", "?")
            score = r.get("score", 0)
            grade = r.get("grade", "D")
            chg = r.get("chg24h", 0)
            print(f"  {grade} {sym}: score={score} chg={chg:+.2f}%")
        if len(deduped) > 10:
            print(f"  ... 还有 {len(deduped) - 10} 条")
        print(f"{'='*50}")
    else:
        save_json(output_path, deduped)
        logger.info("✅ 已保存 %d 条低分信号到 %s", len(deduped), output_path)

    return deduped


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="低分信号收集通道 — 提取 score < 50 的信号存入独立缓存"
    )
    parser.add_argument("--dry", action="store_true", help="预览模式，不写入文件")
    parser.add_argument(
        "--source", type=str, default=str(SOURCE_CACHE),
        help=f"源信号文件路径 (默认: {SOURCE_CACHE})"
    )
    args = parser.parse_args()

    source_path = Path(args.source)
    collect_low_score_signals(
        source_path=source_path,
        dry=args.dry,
    )


if __name__ == "__main__":
    main()
