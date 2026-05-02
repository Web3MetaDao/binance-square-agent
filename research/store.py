# ─────────────────────────────────────────────────────────
# strategy_library  — 全量化交易策略知识库（SQLite）
# 包含：原始策略、融合方案、回测结果、实盘部署状态
# ─────────────────────────────────────────────────────────

import sqlite3
import json
import os
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional


_DB_DIR = Path(__file__).resolve().parent.parent / "learned"
_DB_DIR.mkdir(parents=True, exist_ok=True)
_STRATEGY_DB = str(_DB_DIR / "strategies.db")


# ─── Schema ──────────────────────────────────────────────────

SCHEMA_SQL = """
-- 数据源注册
CREATE TABLE IF NOT EXISTS source_registry (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL UNIQUE,
    type         TEXT NOT NULL,       -- github / arxiv / rss / kaggle / blog
    url          TEXT,
    last_fetch   TEXT,                -- ISO-8601
    fetch_count  INTEGER DEFAULT 0,
    total_strategies INTEGER DEFAULT 0,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 机构策略库（解析后的结构化策略）
CREATE TABLE IF NOT EXISTS strategy_library (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id         INTEGER REFERENCES source_registry(id),
    strategy_name     TEXT NOT NULL,
    author_institution TEXT,
    core_indicators   TEXT,           -- JSON array
    entry_conditions  TEXT,           -- JSON array
    exit_conditions   TEXT,           -- JSON array
    risk_management   TEXT,
    backtest_results  TEXT,           -- JSON
    innovation_points TEXT,           -- JSON array
    applicable_markets TEXT,          -- JSON array
    raw_source_url    TEXT,
    discovered_at     TEXT NOT NULL DEFAULT (datetime('now')),
    tags              TEXT            -- JSON array: momentum, mean_reversion, volatility, ml, ..
);

-- 策略融合优化（Hermes生成的优化方案）
CREATE TABLE IF NOT EXISTS strategy_fusion (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    base_strategy_id   INTEGER NOT NULL REFERENCES strategy_library(id),
    fusion_parent_ids  TEXT,          -- JSON array of parent strategy IDs
    fusion_prompt      TEXT,
    hermes_output      TEXT,          -- 原始JSON输出
    code_extracted     TEXT,          -- 提取的Python代码
    optimized_params   TEXT,          -- JSON: 优化后参数
    created_at         TEXT NOT NULL DEFAULT (datetime('now')),
    status             TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','backtesting','approved','rejected','deployed'))
);

-- 回测结果缓存
CREATE TABLE IF NOT EXISTS backtest_cache (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_fusion_id INTEGER NOT NULL REFERENCES strategy_fusion(id),
    test_type          TEXT NOT NULL   -- insample / outsample / pressure / slippage / monte_carlo
        CHECK (test_type IN ('insample','outsample','pressure','slippage','monte_carlo')),
    sharpe_ratio       REAL,
    max_drawdown       REAL,
    win_rate           REAL,
    profit_factor      REAL,
    total_trades       INTEGER,
    net_profit         REAL,
    full_result        TEXT,          -- JSON 详细结果
    params_used        TEXT,          -- JSON 参数快照
    tested_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 实盘部署控制
CREATE TABLE IF NOT EXISTS deploy_control (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_fusion_id  INTEGER NOT NULL UNIQUE REFERENCES strategy_fusion(id),
    approved            INTEGER DEFAULT 0,
    insample_pass       INTEGER DEFAULT 0,
    outsample_pass      INTEGER DEFAULT 0,
    pressure_pass       INTEGER DEFAULT 0,
    slippage_pass       INTEGER DEFAULT 0,
    monte_carlo_pass    INTEGER DEFAULT 0,
    deployed_at         TEXT,
    circuit_break_count INTEGER DEFAULT 0,
    stopped             INTEGER DEFAULT 0,
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 监控日志
CREATE TABLE IF NOT EXISTS monitor_log (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_fusion_id INTEGER REFERENCES strategy_fusion(id),
    event_type         TEXT NOT NULL,  -- daily_loss / consecutive_loss / deviation / error
    detail             TEXT,
    created_at         TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_strategy_library_source ON strategy_library(source_id);
CREATE INDEX IF NOT EXISTS idx_strategy_library_tags   ON strategy_library(tags);
CREATE INDEX IF NOT EXISTS idx_strategy_fusion_status   ON strategy_fusion(status);
CREATE INDEX IF NOT EXISTS idx_backtest_fusion          ON backtest_cache(strategy_fusion_id);
CREATE INDEX IF NOT EXISTS idx_monitor_fusion           ON monitor_log(strategy_fusion_id);
"""


# ─── Connection helpers ──────────────────────────────────────


class _ReleaseLock:
    """Context manager that releases a threading.Lock on exit."""

    def __init__(self, lock: threading.Lock):
        self._lock = lock

    def __enter__(self):
        return None

    def __exit__(self, *exc):
        self._lock.release()
        return False


# ─── Connection ──────────────────────────────────────────────

class StrategyStore:
    """SQLite 策略库 — threadsafe 连接复用"""

    def __init__(self, db_path: Optional[str] = None):
        self._db_path = db_path or _STRATEGY_DB
        self._lock = threading.Lock()
        self._conn_singleton: Optional[sqlite3.Connection] = None
        self._init_db()

    # ── connection ──────────────────────────────────────────

    def _conn(self) -> sqlite3.Connection:
        """Get the singleton connection (caller must hold _lock when using it)."""
        if self._conn_singleton is None:
            c = sqlite3.connect(self._db_path, check_same_thread=False,
                                isolation_level=None, timeout=10)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL")
            # Note: PRAGMA foreign_keys=ON disables autocommit, so we
            # explicitly commit after each write operation below.
            c.execute("PRAGMA foreign_keys=ON")
            self._conn_singleton = c
        return self._conn_singleton

    def _with_lock(self):
        """Context manager that acquires/releases the threading lock.

        Usage::

            with self._with_lock():
                c = self._conn()
                c.execute(...)
        """
        self._lock.acquire()
        return _ReleaseLock(self._lock)

    def _init_db(self):
        with self._with_lock():
            c = self._conn()
            c.executescript(SCHEMA_SQL)
            # 基于 raw_source_url 的去重索引 — 跳过空字符串
            c.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_library_url "
                "ON strategy_library(raw_source_url) "
                "WHERE raw_source_url != ''"
            )

    # ── Source Registry ──────────────────────────────────

    def upsert_source(self, name: str, s_type: str, url: str = "") -> int:
        with self._with_lock():
            c = self._conn()
            c.execute(
                "INSERT INTO source_registry (name, type, url) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET "
                "type=excluded.type, url=excluded.url",
                (name, s_type, url),
            )
            c.commit()
            # SQLite lastrowid is unreliable for ON CONFLICT UPDATE:
            # it may return the previous INSERT's autoincrement value.
            # Always re-query to get the correct ID.
            row = c.execute(
                "SELECT id FROM source_registry WHERE name = ?", (name,)
            ).fetchone()
            return row["id"] if row else 0

    def update_source_fetch(self, name: str, n_strategies: int):
        with self._with_lock():
            c = self._conn()
            c.execute(
                """UPDATE source_registry SET
                       last_fetch = datetime('now'),
                       fetch_count = fetch_count + 1,
                       total_strategies = total_strategies + ?
                   WHERE name = ?""",
                (n_strategies, name),
            )
            c.commit()

    def get_source(self, name: str) -> Optional[dict]:
        with self._with_lock():
            c = self._conn()
            r = c.execute(
                "SELECT * FROM source_registry WHERE name = ?", (name,)
            ).fetchone()
            return dict(r) if r else None

    def list_sources(self) -> list[dict]:
        with self._with_lock():
            c = self._conn()
            return [dict(r) for r in c.execute(
                "SELECT * FROM source_registry ORDER BY last_fetch DESC"
            ).fetchall()]

    # ── Strategy Library ─────────────────────────────────

    def insert_strategy(self, source_id: int, data: dict) -> Optional[int]:
        """插入策略，如果已存在则返回 None 表示重复。

        去重规则：
        - raw_source_url 不为空 → 按 URL 去重
        - raw_source_url 为空 → 按 strategy_name + source_id 去重
        """
        url = data.get("raw_source_url", "")
        with self._with_lock():
            c = self._conn()
            if url:
                existing = c.execute(
                    "SELECT id FROM strategy_library WHERE raw_source_url = ?",
                    (url,)
                ).fetchone()
            else:
                # arxiv/blog 等无唯一 URL 的来源：按策略名+source_id 去重
                name = data.get("strategy_name", "")
                existing = c.execute(
                    "SELECT id FROM strategy_library "
                    "WHERE strategy_name = ? AND source_id = ?",
                    (name, source_id)
                ).fetchone()
            if existing:
                return None
            c.execute(
                """INSERT INTO strategy_library
                   (source_id, strategy_name, author_institution,
                    core_indicators, entry_conditions, exit_conditions,
                    risk_management, backtest_results, innovation_points,
                    applicable_markets, raw_source_url, tags)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    source_id,
                    data.get("strategy_name", ""),
                    data.get("author_institution", ""),
                    json.dumps(data.get("core_indicators", []), ensure_ascii=False),
                    json.dumps(data.get("entry_conditions", []), ensure_ascii=False),
                    json.dumps(data.get("exit_conditions", []), ensure_ascii=False),
                    data.get("risk_management", ""),
                    json.dumps(data.get("backtest_results", {}), ensure_ascii=False),
                    json.dumps(data.get("innovation_points", []), ensure_ascii=False),
                    json.dumps(data.get("applicable_markets", []), ensure_ascii=False),
                    url,
                    json.dumps(data.get("tags", []), ensure_ascii=False),
                ),
            )
            c.commit()
            return c.execute("SELECT last_insert_rowid()").fetchone()[0]

    def list_strategies(self, tag: Optional[str] = None,
                        limit: int = 50, offset: int = 0) -> list[dict]:
        with self._with_lock():
            c = self._conn()
            if tag:
                rows = c.execute(
                    """SELECT * FROM strategy_library
                       WHERE tags LIKE ? ORDER BY discovered_at DESC
                       LIMIT ? OFFSET ?""",
                    (f"%{tag}%", limit, offset),
                ).fetchall()
            else:
                rows = c.execute(
                    "SELECT * FROM strategy_library ORDER BY discovered_at DESC LIMIT ? OFFSET ?",
                    (limit, offset),
                ).fetchall()
            return [dict(r) for r in rows]

    def count_strategies(self) -> int:
        with self._with_lock():
            c = self._conn()
            return c.execute("SELECT COUNT(*) FROM strategy_library").fetchone()[0]

    def get_recent_strategies(self, hours: int = 24) -> list[dict]:
        with self._with_lock():
            c = self._conn()
            rows = c.execute(
                """SELECT * FROM strategy_library
                   WHERE discovered_at >= datetime('now', ?)
                   ORDER BY discovered_at DESC""",
                (f"-{hours} hours",),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_strategy_by_name(self, name: str) -> Optional[dict]:
        with self._with_lock():
            c = self._conn()
            row = c.execute(
                "SELECT * FROM strategy_library WHERE strategy_name = ?",
                (name,),
            ).fetchone()
            return dict(row) if row else None

    # ── Strategy Fusion ──────────────────────────────────

    def insert_fusion(self, base_id: int, parent_ids: list[int],
                      prompt: str, hermes_output: str,
                      code_extracted: str = "",
                      optimized_params: Optional[dict] = None) -> int:
        with self._with_lock():
            c = self._conn()
            cur = c.execute(
                """INSERT INTO strategy_fusion
                   (base_strategy_id, fusion_parent_ids, fusion_prompt,
                    hermes_output, code_extracted, optimized_params)
                   VALUES (?,?,?,?,?,?)""",
                (
                    base_id,
                    json.dumps(parent_ids),
                    prompt,
                    hermes_output,
                    code_extracted,
                    json.dumps(optimized_params or {}),
                ),
            )
            c.commit()
            return cur.lastrowid

    def update_fusion_status(self, fusion_id: int, status: str):
        with self._with_lock():
            c = self._conn()
            c.execute(
                "UPDATE strategy_fusion SET status = ? WHERE id = ?",
                (fusion_id, status),
            )
            c.commit()

    def init_deploy_control(self, fusion_id: int):
        with self._with_lock():
            c = self._conn()
            c.execute(
                "INSERT OR IGNORE INTO deploy_control (strategy_fusion_id) VALUES (?)",
                (fusion_id,),
            )
            c.commit()

    def update_fusion_code(self, fusion_id: int, code: str,
                           params: Optional[dict] = None):
        with self._with_lock():
            c = self._conn()
            c.execute(
                "UPDATE strategy_fusion SET code_extracted = ?, optimized_params = ? WHERE id = ?",
                (code, json.dumps(params or {}), fusion_id),
            )
            c.commit()

    def get_pending_fusions(self) -> list[dict]:
        with self._with_lock():
            c = self._conn()
            rows = c.execute(
                """SELECT * FROM strategy_fusion
                   WHERE status = 'pending' ORDER BY created_at ASC"""
            ).fetchall()
            return [dict(r) for r in rows]

    def get_backtesting_fusions(self) -> list[dict]:
        with self._with_lock():
            c = self._conn()
            rows = c.execute(
                """SELECT * FROM strategy_fusion
                   WHERE status = 'backtesting' ORDER BY created_at ASC"""
            ).fetchall()
            return [dict(r) for r in rows]

    # ── Backtest Cache ───────────────────────────────────

    def insert_backtest(self, fusion_id: int, test_type: str,
                        result: dict, params: Optional[dict] = None) -> int:
        with self._with_lock():
            c = self._conn()
            cur = c.execute(
                """INSERT INTO backtest_cache
                   (strategy_fusion_id, test_type, sharpe_ratio, max_drawdown,
                    win_rate, profit_factor, total_trades, net_profit,
                    full_result, params_used)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    fusion_id, test_type,
                    result.get("sharpe_ratio"),
                    result.get("max_drawdown"),
                    result.get("win_rate"),
                    result.get("profit_factor"),
                    result.get("total_trades"),
                    result.get("net_profit"),
                    json.dumps(result, ensure_ascii=False),
                    json.dumps(params or {}),
                ),
            )
            c.commit()
            return cur.lastrowid

    def get_backtest_summary(self, fusion_id: int) -> dict:
        """返回 {test_type: {sharpe, max_dd, ...}}"""
        with self._with_lock():
            c = self._conn()
            rows = c.execute(
                "SELECT * FROM backtest_cache WHERE strategy_fusion_id = ?",
                (fusion_id,),
            ).fetchall()
            return {r["test_type"]: dict(r) for r in rows}

    # ── Deploy Control ───────────────────────────────────

    def update_deploy_test(self, fusion_id: int, test_type: str, passed: bool):
        col_map = {
            "insample": "insample_pass",
            "outsample": "outsample_pass",
            "pressure": "pressure_pass",
            "slippage": "slippage_pass",
            "monte_carlo": "monte_carlo_pass",
        }
        col = col_map.get(test_type)
        if not col:
            return
        with self._with_lock():
            c = self._conn()
            c.execute(
                f"UPDATE deploy_control SET {col} = ?, updated_at = datetime('now') "
                "WHERE strategy_fusion_id = ?",
                (1 if passed else 0, fusion_id),
            )
            c.commit()

    def check_deploy_ready(self, fusion_id: int) -> bool:
        with self._with_lock():
            c = self._conn()
            r = c.execute(
                """SELECT insample_pass, outsample_pass, pressure_pass,
                          slippage_pass, monte_carlo_pass
                   FROM deploy_control WHERE strategy_fusion_id = ?""",
                (fusion_id,),
            ).fetchone()
            if not r:
                return False
            return all([r["insample_pass"], r["outsample_pass"],
                        r["pressure_pass"], r["slippage_pass"],
                        r["monte_carlo_pass"]])

    def approve_deployment(self, fusion_id: int):
        with self._with_lock():
            c = self._conn()
            c.execute(
                "UPDATE deploy_control SET approved = 1, "
                "deployed_at = datetime('now'), updated_at = datetime('now') "
                "WHERE strategy_fusion_id = ?",
                (fusion_id,),
            )
            c.commit()
            self.update_fusion_status(fusion_id, "deployed")

    def reject_deployment(self, fusion_id: int):
        with self._with_lock():
            c = self._conn()
            c.execute(
                "UPDATE deploy_control SET approved = 0, updated_at = datetime('now') "
                "WHERE strategy_fusion_id = ?",
                (fusion_id,),
            )
            c.commit()
            self.update_fusion_status(fusion_id, "rejected")

    # ── Monitor Log ──────────────────────────────────────

    def log_event(self, fusion_id: int, event_type: str, detail: str):
        with self._with_lock():
            c = self._conn()
            c.execute(
                "INSERT INTO monitor_log (strategy_fusion_id, event_type, detail) "
                "VALUES (?, ?, ?)",
                (fusion_id, event_type, detail),
            )
            c.commit()

    def get_recent_events(self, hours: int = 24) -> list[dict]:
        with self._with_lock():
            c = self._conn()
            rows = c.execute(
                "SELECT * FROM monitor_log WHERE created_at >= datetime('now', ?) "
                "ORDER BY created_at DESC",
                (f"-{hours} hours",),
            ).fetchall()
            return [dict(r) for r in rows]

    # ── Dashboard ────────────────────────────────────────

    def dashboard(self) -> dict:
        with self._with_lock():
            c = self._conn()
            total = c.execute("SELECT COUNT(*) FROM strategy_library").fetchone()[0]
            by_status = {
                r["status"]: r["cnt"] for r in c.execute(
                    "SELECT status, COUNT(*) as cnt FROM strategy_fusion GROUP BY status"
                ).fetchall()
            }
            pending_bt = c.execute(
                "SELECT COUNT(*) FROM strategy_fusion WHERE status IN ('pending','backtesting')"
            ).fetchone()[0]
            deployed = c.execute(
                "SELECT COUNT(*) FROM deploy_control WHERE approved=1"
            ).fetchone()[0]
            recent_24h = c.execute(
                "SELECT COUNT(*) FROM strategy_library "
                "WHERE discovered_at >= datetime('now', '-24 hours')"
            ).fetchone()[0]
            # 今日事件
            events_24h = c.execute(
                "SELECT COUNT(*) FROM monitor_log "
                "WHERE created_at >= datetime('now', '-24 hours')"
            ).fetchone()[0]
            return {
                "total_strategies": total,
                "fusion_by_status": by_status,
                "pending_backtesting": pending_bt,
                "deployed": deployed,
                "new_last_24h": recent_24h,
                "events_last_24h": events_24h,
            }
