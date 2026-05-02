    def upsert_source(self, name: str, s_type: str, url: str = "") -> int:
        with self._with_lock():
            c = self._conn()
            cur = c.execute(
                """INSERT INTO source_registry (name, type, url)
                   VALUES (?, ?, ?)
                   ON CONFLICT(name) DO UPDATE SET
                       type=excluded.type, url=excluded.url""",
                (name, s_type, url),
            )
            # ON CONFLICT UPDATE returns lastrowid=0 in SQLite
            # Re-query to get the actual ID
            if cur.lastrowid == 0:
                row = c.execute(
                    "SELECT id FROM source_registry WHERE name = ?", (name,)
                ).fetchone()
                return row["id"] if row else 0
            return cur.lastrowid
