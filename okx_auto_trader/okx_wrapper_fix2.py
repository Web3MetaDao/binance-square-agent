    def get_smartmoney_overview(self, limit: int = 10) -> dict:
        """Multi-currency smart money overview (always current-hour data)."""
        return self._run(
            "smartmoney", "overview", "--lmtNum", str(limit),
            check_api_key=True
        )

    def get_smartmoney_signal(self, instId: str) -> dict:
        """Single-currency aggregated consensus signal (always current-hour data)."""
        return self._run(
            "smartmoney", "signal", "--instId", instId,
            check_api_key=True
        )

    def get_smartmoney_signal_history(self, instId: str,
                                      ts: Optional[int] = None) -> list:
        """Signal history timeline for trend analysis."""
        args = ["smartmoney", "signal-history", "--instId", instId]
        if ts is None:
            ts = int(time.time() * 1000)
        args.extend(["--ts", str(ts)])
        return self._run(*args, check_api_key=True)

    # ------------------------------------------------------------------
    # News & Sentiment (requires API key)
    # ------------------------------------------------------------------

    def get_news_latest(self, limit: int = 10) -> list:
        """Latest news."""
        return self._run("news", "latest", "--limit", str(limit),
                         check_api_key=True)