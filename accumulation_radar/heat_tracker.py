"""热度追踪模块 — 首次上榜检测。

记录热度币的第一次出现时间，实现"首次上榜检测"功能。

灵感来自 ConnectFarm1 的"热度做多雷达"，功能类似但更精简。

数据存储路径：{DATA_DIR}/heat_history.json
"""

import json
import os
from datetime import datetime, timedelta, timezone

from .config import DATA_DIR, logger

# ── 文件路径 ──────────────────────────────────────
HEAT_HISTORY_PATH = os.path.join(DATA_DIR, "heat_history.json")

# ── 时间常量 ──────────────────────────────────────
# 北京时间（UTC+8）
BJT = timezone(timedelta(hours=8))

# 重新标记为首次上榜的过期天数
REFRESH_DAYS = 7

# 自动清理的过期天数
CLEANUP_DAYS = 14


def _now_bjt() -> str:
    """返回当前北京时间字符串，格式：YYYY-MM-DD HH:MM"""
    return datetime.now(BJT).strftime("%Y-%m-%d %H:%M")


def _parse_bjt(time_str: str) -> datetime | None:
    """解析北京时间字符串，返回 datetime 对象。"""
    try:
        return datetime.strptime(time_str, "%Y-%m-%d %H:%M").replace(tzinfo=BJT)
    except (ValueError, TypeError):
        return None


# ═══════════════════════════════════════════════════
#  文件读写
# ═══════════════════════════════════════════════════


def load_heat_history() -> dict:
    """加载热度历史文件。

    如果文件不存在或损坏，返回空字典。

    Returns:
        {
            "BTC": {
                "first_seen": "2026-04-27 14:30",
                "last_seen": "2026-04-28 10:00",
                "sources": ["cg_trending", "vol_surge"],
                "heat_score": 65
            },
            ...
        }
    """
    if not os.path.exists(HEAT_HISTORY_PATH):
        logger.info("热度历史文件不存在，返回空字典")
        return {}

    try:
        with open(HEAT_HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            logger.warning("热度历史文件格式异常，重置为空字典")
            return {}
        logger.info(f"已加载 {len(data)} 个币的热度历史")
        return data
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"加载热度历史文件失败 ({e})，返回空字典")
        return {}


def save_heat_history(history: dict):
    """保存热度历史。

    保存前自动清理超过 CLEANUP_DAYS 天的历史记录。

    Args:
        history: 热度历史字典
    """
    # 保存前自动清理
    history = cleanup_old(history, days=CLEANUP_DAYS)

    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(HEAT_HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        logger.info(f"热度历史已保存 ({len(history)} 个币)")
    except OSError as e:
        logger.error(f"保存热度历史失败: {e}")


# ═══════════════════════════════════════════════════
#  核心逻辑
# ═══════════════════════════════════════════════════


def update_heat(coin: str, heat_map: dict, coin_data_entry: dict) -> dict:
    """更新单个币的热度记录。

    Args:
        coin: 币种名称（无 USDT 后缀，如 "BTC"）
        heat_map: {coin_name: heat_score} 汇总热度字典
        coin_data_entry: coin_data 中该币的数据条目（用于提取 sources）

    Returns:
        更新后的该币记录
    """
    history = load_heat_history()
    now_str = _now_bjt()

    # 获取当前热度值
    current_heat = heat_map.get(coin, 0)

    # 解析 sources：从 coin_data_entry 中提取各维度标记
    sources = []
    if coin_data_entry.get("cg_trending"):
        sources.append("cg_trending")
    if coin_data_entry.get("vol_surge"):
        sources.append("vol_surge")

    existing = history.get(coin)

    if existing:
        # 已存在：更新 last_seen 和 heat_score（取最高值）
        existing["last_seen"] = now_str
        existing["heat_score"] = max(existing.get("heat_score", 0), current_heat)
        # 合并 sources（去重）
        existing_sources = set(existing.get("sources", []))
        existing_sources.update(sources)
        existing["sources"] = sorted(existing_sources)
        record = existing
    else:
        # 首次出现
        record = {
            "first_seen": now_str,
            "last_seen": now_str,
            "sources": sorted(sources),
            "heat_score": current_heat,
        }

    history[coin] = record
    save_heat_history(history)
    return record


def detect_new_entries(coin_data: dict, heat_map: dict) -> list[dict]:
    """检测首次上榜的币。

    逻辑：
    1. 从 heat_map 中找所有有热度的币
    2. 对每个币，检查 heat_history 中是否存在
    3. 如果不存在 → 首次上榜
    4. 如果存在但超过 REFRESH_DAYS 天没更新 → 重新标记为首次
    5. 更新 history（写入 last_seen + 热度）
    6. 保存

    Args:
        coin_data: build_coin_data 返回的完整数据 {sym: {coin, ...}}
        heat_map: {coin_name: heat_score}，从 fetch_heat_data 和策略评分中汇总的热度

    Returns:
        按 heat_score 降序排列的列表：
        [{"coin": "BTC", "sources": [...], "first_seen": "...", "heat_score": 65}, ...]
    """
    history = load_heat_history()
    now_str = _now_bjt()
    now_dt = datetime.now(BJT)

    new_entries = []

    for coin, heat_score in heat_map.items():
        if heat_score <= 0:
            continue

        # 从 coin_data 中查找对应条目（sym 为 coin + "USDT"）
        sym = f"{coin}USDT"
        entry = coin_data.get(sym, {})

        # 解析 sources
        sources = []
        if entry.get("cg_trending"):
            sources.append("cg_trending")
        if entry.get("vol_surge"):
            sources.append("vol_surge")

        existing = history.get(coin)
        is_new = False

        if existing:
            # 检查是否超过 REFRESH_DAYS 天没更新
            last_seen = _parse_bjt(existing.get("last_seen", ""))
            if last_seen is not None:
                days_since = (now_dt - last_seen).days
                if days_since >= REFRESH_DAYS:
                    # 重新标记为首次上榜
                    is_new = True
                    existing["first_seen"] = now_str
                    logger.info(
                        f"♻️ {coin} 已 {days_since} 天未出现，重新标记为首次上榜"
                    )
            # 更新 last_seen 和 heat_score
            existing["last_seen"] = now_str
            existing["heat_score"] = max(existing.get("heat_score", 0), heat_score)
            # 合并 sources
            existing_sources = set(existing.get("sources", []))
            existing_sources.update(sources)
            existing["sources"] = sorted(existing_sources)
            record = existing
        else:
            # 全新首次上榜
            is_new = True
            record = {
                "first_seen": now_str,
                "last_seen": now_str,
                "sources": sorted(sources),
                "heat_score": heat_score,
            }

        history[coin] = record

        if is_new:
            new_entries.append({
                "coin": coin,
                "sources": record["sources"],
                "first_seen": record["first_seen"],
                "heat_score": record["heat_score"],
            })

    # 保存更新后的历史
    save_heat_history(history)

    # 按 heat_score 降序
    new_entries.sort(key=lambda x: x["heat_score"], reverse=True)

    if new_entries:
        coins_str = ", ".join(
            f"{e['coin']}({e['heat_score']})" for e in new_entries
        )
        logger.info(f"🆕 检测到 {len(new_entries)} 个首次上榜币: {coins_str}")
    else:
        logger.info("✅ 没有新的首次上榜币")

    return new_entries


# ═══════════════════════════════════════════════════
#  维护
# ═══════════════════════════════════════════════════


def cleanup_old(history: dict, days: int = 14) -> dict:
    """清理超过指定天数的历史记录。

    清理条件：last_seen 距今超过 days 天。

    Args:
        history: 热度历史字典
        days: 过期天数阈值（默认 14 天）

    Returns:
        清理后的历史字典
    """
    now_dt = datetime.now(BJT)
    cutoff = now_dt - timedelta(days=days)
    removed = []

    keys_to_delete = []
    for coin, record in history.items():
        last_seen = _parse_bjt(record.get("last_seen", ""))
        if last_seen is not None and last_seen < cutoff:
            keys_to_delete.append(coin)
            removed.append(coin)

    for key in keys_to_delete:
        del history[key]

    if removed:
        logger.info(f"🧹 清理了 {len(removed)} 个过期热度记录: {', '.join(removed)}")
    else:
        logger.debug(f"清理检查: 无过期记录（阈值 {days} 天）")

    return history
