"""SQLite 数据库操作模块。

所有函数均不提交事务，由调用方控制 conn.commit()。
"""
import json
import os
import sqlite3

from .config import DB_PATH, DATA_DIR, logger


def get_db() -> sqlite3.Connection:
    """获取数据库连接并确保必要的表存在。

    - 自动创建 DATA_DIR 目录
    - 返回 row_factory=sqlite3.Row 的连接
    - 建表：watchlist, pool_map, swing_history
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS watchlist (
            id INTEGER PRIMARY KEY,
            symbol TEXT UNIQUE,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes TEXT
        );
        CREATE TABLE IF NOT EXISTS pool_map (
            id INTEGER PRIMARY KEY,
            symbol TEXT UNIQUE,
            pool_data TEXT,
            updated_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS swing_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,      -- 'surge' | 'dump'
            swing REAL NOT NULL,
            score REAL NOT NULL DEFAULT 0,
            px REAL NOT NULL DEFAULT 0,
            vol REAL NOT NULL DEFAULT 0,
            chg24h REAL NOT NULL DEFAULT 0,
            fr REAL NOT NULL DEFAULT 0,
            window TEXT NOT NULL DEFAULT '5m',
            scanned_total INTEGER NOT NULL DEFAULT 0,
            btc_chg24h REAL NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT (datetime('now', '+8 hours'))
        );
        CREATE INDEX IF NOT EXISTS idx_swing_history_symbol
            ON swing_history(symbol);
        CREATE INDEX IF NOT EXISTS idx_swing_history_created_at
            ON swing_history(created_at);
    """)
    return conn


def save_watchlist(conn: sqlite3.Connection, results: list[dict]) -> None:
    """将 pool 扫描结果写入 watchlist 和 pool_map。

    每个 symbol：
      - watchlist: INSERT OR IGNORE（不覆盖已有行）
      - pool_map: 覆盖 pool_data（json 序列化）
    """
    import datetime
    now = datetime.datetime.utcnow().isoformat()

    for r in results:
        symbol = r.get("symbol", r.get("coin", ""))
        if not symbol:
            continue

        conn.execute(
            "INSERT OR IGNORE INTO watchlist (symbol) VALUES (?)",
            (symbol,),
        )
        conn.execute(
            "INSERT OR REPLACE INTO pool_map (symbol, pool_data, updated_at) VALUES (?, ?, ?)",
            (symbol, json.dumps(r), now),
        )


def load_watchlist_symbols(conn: sqlite3.Connection) -> list[str]:
    """返回 watchlist 中所有 symbol，按添加时间倒序。"""
    rows = conn.execute(
        "SELECT symbol FROM watchlist ORDER BY added_at DESC"
    ).fetchall()
    return [row["symbol"] for row in rows]


def load_pool_map(conn: sqlite3.Connection) -> dict[str, dict]:
    """返回 {symbol: dict, ...} 格式的完整 pool 数据映射。"""
    rows = conn.execute("SELECT symbol, pool_data FROM pool_map").fetchall()
    result = {}
    for row in rows:
        try:
            result[row["symbol"]] = json.loads(row["pool_data"])
        except (json.JSONDecodeError, TypeError):
            continue
    return result


def delete_watchlist_symbol(conn: sqlite3.Connection, symbol: str) -> None:
    """从 watchlist 和 pool_map 中删除指定 symbol。"""
    conn.execute("DELETE FROM watchlist WHERE symbol = ?", (symbol,))
    conn.execute("DELETE FROM pool_map WHERE symbol = ?", (symbol,))


# ── Swing 异动历史 ──────────────────────────────────


def save_swing_results(conn: sqlite3.Connection, result: dict) -> int:
    """将 swing 异动结果写入 swing_history 表。

    Args:
        conn: 数据库连接
        result: score_opportunities 返回的 dict
            {surge: [...], dump: [...], btc_chg24h, candidates_scanned}

    Returns:
        写入的记录数
    """
    now = os.popen("date -u +'%Y-%m-%d %H:%M:%S'").read().strip()
    btc_chg24h = result.get("btc_chg24h", 0.0)
    scanned_total = result.get("candidates_scanned", 0)
    count = 0

    for entry in result.get("surge", []):
        conn.execute(
            """INSERT INTO swing_history
               (symbol, direction, swing, score, px, vol, chg24h, fr, window,
                scanned_total, btc_chg24h, created_at)
               VALUES (?, 'surge', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                entry.get("sym", ""),
                entry.get("swing", 0),
                entry.get("score", 0),
                entry.get("px", 0),
                entry.get("vol", 0),
                entry.get("chg24h", 0),
                entry.get("fr", 0),
                entry.get("window", "5m"),
                scanned_total,
                btc_chg24h,
                now,
            ),
        )
        count += 1

    for entry in result.get("dump", []):
        conn.execute(
            """INSERT INTO swing_history
               (symbol, direction, swing, score, px, vol, chg24h, fr, window,
                scanned_total, btc_chg24h, created_at)
               VALUES (?, 'dump', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                entry.get("sym", ""),
                entry.get("swing", 0),
                entry.get("score", 0),
                entry.get("px", 0),
                entry.get("vol", 0),
                entry.get("chg24h", 0),
                entry.get("fr", 0),
                entry.get("window", "5m"),
                scanned_total,
                btc_chg24h,
                now,
            ),
        )
        count += 1

    return count


def load_recent_swing_history(
    conn: sqlite3.Connection,
    hours: int = 24,
    limit: int = 50,
) -> list[dict]:
    """查询最近 N 小时的 swing 异动历史。

    Args:
        conn: 数据库连接
        hours: 回溯小时数
        limit: 最大返回条数

    Returns:
        [{"symbol", "direction", "swing", "score", "px", "vol",
          "chg24h", "fr", "window", "scanned_total", "btc_chg24h",
          "created_at"}, ...]
    """
    rows = conn.execute(
        """SELECT symbol, direction, swing, score, px, vol, chg24h, fr,
                  window, scanned_total, btc_chg24h, created_at
           FROM swing_history
           WHERE created_at >= datetime('now', ?)
           ORDER BY created_at DESC
           LIMIT ?""",
        (f'-{hours} hours', limit),
    ).fetchall()
    return [dict(r) for r in rows]


def get_swing_summary(conn: sqlite3.Connection, hours: int = 24) -> dict:
    """获取近期 swing 异动汇总统计。

    Returns:
        {"total_events": int, "unique_coins": int, "surge_count": int,
         "dump_count": int, "top_surge": [...], "top_dump": [...]}
    """
    total = conn.execute(
        "SELECT COUNT(*) as c FROM swing_history WHERE created_at >= datetime('now', ?)",
        (f'-{hours} hours',),
    ).fetchone()["c"]

    unique = conn.execute(
        "SELECT COUNT(DISTINCT symbol) as c FROM swing_history WHERE created_at >= datetime('now', ?)",
        (f'-{hours} hours',),
    ).fetchone()["c"]

    surge_count = conn.execute(
        "SELECT COUNT(*) as c FROM swing_history WHERE direction='surge' AND created_at >= datetime('now', ?)",
        (f'-{hours} hours',),
    ).fetchone()["c"]

    dump_count = conn.execute(
        "SELECT COUNT(*) as c FROM swing_history WHERE direction='dump' AND created_at >= datetime('now', ?)",
        (f'-{hours} hours',),
    ).fetchone()["c"]

    top_surge = [
        dict(r) for r in conn.execute(
            """SELECT symbol, swing, score, window, created_at
               FROM swing_history WHERE direction='surge'
               AND created_at >= datetime('now', ?)
               ORDER BY score DESC LIMIT 5""",
            (f'-{hours} hours',),
        ).fetchall()
    ]

    top_dump = [
        dict(r) for r in conn.execute(
            """SELECT symbol, swing, score, window, created_at
               FROM swing_history WHERE direction='dump'
               AND created_at >= datetime('now', ?)
               ORDER BY score DESC LIMIT 5""",
            (f'-{hours} hours',),
        ).fetchall()
    ]

    return {
        "total_events": total,
        "unique_coins": unique,
        "surge_count": surge_count,
        "dump_count": dump_count,
        "top_surge": top_surge,
        "top_dump": top_dump,
    }
