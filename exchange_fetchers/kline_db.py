"""
kline_db.py — SQLite-based kline cache for multi-exchange surge scanner.

Schema:
  klines(sym TEXT, exchange TEXT, timeframe TEXT, ts INT,
         open REAL, high REAL, low REAL, close REAL, volume REAL,
         PRIMARY KEY(sym, exchange, timeframe, ts))

Functions:
  get_latest_ts(sym, exchange, timeframe) -> int or 0
  get_klines(sym, exchange, timeframe, limit=130) -> dict or None
  store_klines(sym, exchange, timeframe, kline_dict) -> int (rows inserted)
  ensure_gap_klines(sym, exchange, timeframe, fetch_fn) -> dict or None
  close()
"""

import logging
import sqlite3
import threading
import time
from pathlib import Path

import numpy as np

logger = logging.getLogger("kline_db")

DB_DIR = Path("/root/binance-square-agent/data")
DB_PATH = DB_DIR / "kline_cache.db"


class KlineDB:
    """SQLite-backed kline cache with thread-safe writes."""

    def __init__(self, db_path: str | Path | None = None):
        self._db_path = Path(db_path) if db_path else DB_PATH
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    # ── Schema ────────────────────────────────────────────────

    def _init_schema(self):
        with self._lock:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS klines (
                    sym       TEXT NOT NULL,
                    exchange  TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    ts        INTEGER NOT NULL,
                    open      REAL NOT NULL,
                    high      REAL NOT NULL,
                    low       REAL NOT NULL,
                    close     REAL NOT NULL,
                    volume    REAL NOT NULL,
                    PRIMARY KEY (sym, exchange, timeframe, ts)
                )
            """)
            self._conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_klines_lookup
                ON klines(sym, exchange, timeframe, ts DESC)
            """)
            self._conn.commit()

    # ── Query helpers ─────────────────────────────────────────

    def get_latest_ts(self, sym: str, exchange: str, timeframe: str) -> int:
        """Return the latest timestamp for (sym, exchange, timeframe), or 0."""
        with self._lock:
            row = self._conn.execute(
                "SELECT ts FROM klines WHERE sym=? AND exchange=? AND timeframe=? ORDER BY ts DESC LIMIT 1",
                (sym, exchange, timeframe),
            ).fetchone()
        return row["ts"] if row else 0

    def get_klines(
        self, sym: str, exchange: str, timeframe: str, limit: int = 130
    ) -> dict | None:
        """Return the latest `limit` bars as numpy arrays, or None if no data.

        Returns dict with keys: open, high, low, close, volume (np.ndarray), times (list[int]).
        Bars are ordered oldest to newest.
        """
        with self._lock:
            rows = self._conn.execute(
                """SELECT ts, open, high, low, close, volume
                   FROM klines
                   WHERE sym=? AND exchange=? AND timeframe=?
                   ORDER BY ts ASC
                   LIMIT ?""",
                (sym, exchange, timeframe, limit),
            ).fetchall()

        if not rows:
            return None

        times = [r["ts"] for r in rows]
        opens = np.array([r["open"] for r in rows], dtype=np.float64)
        highs = np.array([r["high"] for r in rows], dtype=np.float64)
        lows = np.array([r["low"] for r in rows], dtype=np.float64)
        closes = np.array([r["close"] for r in rows], dtype=np.float64)
        volumes = np.array([r["volume"] for r in rows], dtype=np.float64)

        return {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
            "times": times,
        }

    def store_klines(
        self, sym: str, exchange: str, timeframe: str, kline_dict: dict
    ) -> int:
        """Insert new kline bars, skipping duplicates via OR IGNORE.

        Args:
            sym: Standard symbol (e.g. "BTCUSDT")
            exchange: Exchange name (e.g. "okx", "gate", "bitget")
            timeframe: "1H", "2H", or "4H" (common format — Gate will be converted)
            kline_dict: dict with keys open, high, low, close, volume, times

        Returns:
            Number of rows actually inserted (new rows).
        """
        times = kline_dict.get("times", [])
        opens = kline_dict.get("open", [])
        highs = kline_dict.get("high", [])
        lows = kline_dict.get("low", [])
        closes = kline_dict.get("close", [])
        volumes = kline_dict.get("volume", [])

        if not times:
            return 0

        count_before = 0
        rows_inserted = 0
        with self._lock:
            count_before = self._conn.execute(
                "SELECT COUNT(*) FROM klines WHERE sym=? AND exchange=? AND timeframe=?",
                (sym, exchange, timeframe),
            ).fetchone()[0]

            for i in range(len(times)):
                try:
                    self._conn.execute(
                        """INSERT OR IGNORE INTO klines
                           (sym, exchange, timeframe, ts, open, high, low, close, volume)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            sym,
                            exchange,
                            timeframe,
                            int(times[i]),
                            float(opens[i]),
                            float(highs[i]),
                            float(lows[i]),
                            float(closes[i]),
                            float(volumes[i]),
                        ),
                    )
                except (ValueError, IndexError, TypeError) as e:
                    logger.debug("store_klines skip row %d for %s: %s", i, sym, e)
                    continue

            self._conn.commit()

            count_after = self._conn.execute(
                "SELECT COUNT(*) FROM klines WHERE sym=? AND exchange=? AND timeframe=?",
                (sym, exchange, timeframe),
            ).fetchone()[0]
            rows_inserted = count_after - count_before

        return rows_inserted

    # ── Gap-filling helper ────────────────────────────────────

    def ensure_gap_klines(
        self,
        sym: str,
        exchange: str,
        timeframe: str,
        fetch_fn: callable,
        interval_seconds: int | None = None,
    ) -> dict | None:
        """Ensure kline data is fresh, merging API results with local cache.

        Strategy:
          1. Query latest_ts from DB for (sym, exchange, timeframe).
          2. If no data or last bar is > 2 periods old, call fetch_fn() for full bars.
          3. Store all bars from the fetch result that are NEWER than latest_ts.
          4. Return the complete merged 130 bars (from DB) as numpy arrays.

        Args:
            sym: Standard symbol (e.g. "BTCUSDT")
            exchange: Exchange name (e.g. "okx", "gate", "bitget")
            timeframe: Timeframe string (e.g. "1H", "2h", "4H")
            fetch_fn: Zero-arg callable that returns a kline dict or None
            interval_seconds: Period duration in seconds (for staleness check).
                              Auto-detected from timeframe if not provided.

        Returns:
            Kline dict (same format as fetch functions), or None if no data available.
        """
        # Normalize timeframe suffixes (Gate uses lowercase)
        tf_upper = timeframe.upper() if timeframe else timeframe

        # Auto-detect interval seconds
        if interval_seconds is None:
            if tf_upper == "1H" or tf_upper == "1h":
                interval_seconds = 3600
            elif tf_upper == "2H" or tf_upper == "2h":
                interval_seconds = 7200
            elif tf_upper == "4H" or tf_upper == "4h":
                interval_seconds = 14400
            else:
                interval_seconds = 3600  # fallback

        latest_ts = self.get_latest_ts(sym, exchange, timeframe)
        now_ts = int(time.time())

        # Check if data is stale: no data, or last bar is > 2 periods old
        needs_fetch = False
        if latest_ts == 0:
            needs_fetch = True
        elif (now_ts - latest_ts) > (interval_seconds * 2):
            needs_fetch = True

        if needs_fetch:
            result = fetch_fn()
            if result is not None:
                # Store all bars — the ones we already have will be ignored via OR IGNORE
                self.store_klines(sym, exchange, timeframe, result)
            else:
                logger.warning(
                    "ensure_gap_klines: fetch_fn returned None for %s/%s/%s",
                    sym, exchange, timeframe,
                )
                # If we have stale data, still return what we have
                if latest_ts == 0:
                    return None

        return self.get_klines(sym, exchange, timeframe, limit=130)

    # ── Cleanup ───────────────────────────────────────────────

    def close(self):
        """Close the database connection."""
        try:
            self._conn.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
