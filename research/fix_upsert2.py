    def upsert_source(self, name: str, s_type: str, url: str = "") -> int:
        with self._with_lock():
            c = self._conn()
            cur = c.execute(
                "INSERT INTO source_registry (name, type, url) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET "
                "type=excluded.type, url=excluded.url",
                (name, s_type, url),
            )
            c.commit()
            if cur.lastrowid == 0:
                row = c.execute(
                    "SELECT id FROM source_registry WHERE name = ?", (name,)
                ).fetchone()
                return row["id"] if row else 0
            return cur.lastrowid

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
