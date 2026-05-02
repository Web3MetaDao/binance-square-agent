#!/usr/bin/env python3
"""
liquidation_detector.py — Detects liquidation cascade events from kline data.

OKX/Gate/Bitget do not provide public liquidation websocket feeds, so we
infer liquidation cascades from price action and volume patterns.

Three detection modes:
    1. Single-candle liquidation: one candle drops >5% with volume >3x avg
    2. Cascade: 2+ consecutive candles each drop >2% with elevated volume
    3. Liquidation wall breakout: single candle range >8% with volume spike

Resilient by design: all operations are pure arithmetic, never crashes the caller.

Exports:
    LiquidationDetector — class with detect_liquidation_cascade() and mark_liquidation_signal()
"""

import logging
from typing import Optional

import numpy as np

logger = logging.getLogger("liquidation")


class LiquidationDetector:
    """
    Detect liquidation cascade events from kline data.

    Methods:
        detect_liquidation_cascade(kline_data) -> dict[str, dict]
            Analyze klines for each symbol and return liquidation metrics.

        mark_liquidation_signal(liq_data) -> dict[str, dict]
            Convert liquidation metrics into extras-friendly signal format.
    """

    # ── Detection thresholds ──────────────────────────────
    CASCADE_DROP_PCT = 2.0        # Per-candle drop threshold for cascade
    CASCADE_VOL_MULTIPLIER = 2.0  # Volume multiplier vs 3-bar avg for cascade
    SINGLE_DROP_PCT = 5.0         # Single-candle drop threshold for severe
    SINGLE_VOL_MULTIPLIER = 3.0   # Volume multiplier for single severe
    RANGE_DROP_PCT = 8.0          # High-low range threshold for wall breakout
    LOW_DROP_PCT = 3.0            # Lower threshold for "low" severity
    LOW_VOL_MULTIPLIER = 2.0      # Volume multiplier for low severity
    LOOKBACK = 5                  # Number of recent candles to analyze

    def detect_liquidation_cascade(self, kline_data: dict) -> dict[str, dict]:
        """
        Analyze kline data for each symbol and detect liquidation cascade patterns.

        Args:
            kline_data: dict[str, dict] where each value has arrays:
                {"open": [...], "high": [...], "low": [...], "close": [...],
                 "volume": [...], "times": [...]}

        Returns:
            {symbol: {
                "has_liquidation": bool,
                "liquidation_type": "cascade" | "single" | None,
                "liquidation_severity": "high" | "medium" | "low" | None,
                "total_drop_pct": float,
                "max_single_candle_drop": float,
                "volume_ratio": float,
            }}
        """
        if not kline_data:
            return {}

        result: dict[str, dict] = {}

        for sym, kline in kline_data.items():
            try:
                analysis = self._analyze_one(sym, kline)
                result[sym] = analysis
            except Exception as e:
                logger.debug("Liquidation analysis failed %s: %s", sym, e)
                result[sym] = {
                    "has_liquidation": False,
                    "liquidation_type": None,
                    "liquidation_severity": None,
                    "total_drop_pct": 0.0,
                    "max_single_candle_drop": 0.0,
                    "volume_ratio": 0.0,
                }

        liq_count = sum(1 for v in result.values() if v.get("has_liquidation"))
        if liq_count:
            logger.info("LiquidationDetector: %d/%d symbols flagged", liq_count, len(result))
        return result

    def _analyze_one(self, sym: str, kline: dict) -> dict:
        """
        Analyze a single symbol's kline data.

        Uses the last LOOKBACK candles for detection.
        """
        closes = np.array(kline.get("close", []), dtype=float)
        opens = np.array(kline.get("open", []), dtype=float)
        highs = np.array(kline.get("high", []), dtype=float)
        lows = np.array(kline.get("low", []), dtype=float)
        volumes = np.array(kline.get("volume", []), dtype=float)

        n_bars = len(closes)
        if n_bars < self.LOOKBACK + 3:
            return {
                "has_liquidation": False,
                "liquidation_type": None,
                "liquidation_severity": None,
                "total_drop_pct": 0.0,
                "max_single_candle_drop": 0.0,
                "volume_ratio": 0.0,
            }

        # Focus on the last LOOKBACK candles
        lookback = min(self.LOOKBACK, n_bars)
        recent_closes = closes[-lookback:]
        recent_opens = opens[-lookback:]
        recent_highs = highs[-lookback:]
        recent_lows = lows[-lookback:]
        recent_volumes = volumes[-lookback:]

        # Per-candle drop percentage: (close - open) / open * 100
        # Negative means drop
        candle_drops = []
        candle_ranges = []
        candle_vol_ratios = []

        for i in range(lookback):
            idx = n_bars - lookback + i
            o = recent_opens[i]
            c = recent_closes[i]
            h = recent_highs[i]
            l = recent_lows[i]
            v = recent_volumes[i]

            if o > 0:
                drop_pct = (c - o) / o * 100
            else:
                drop_pct = 0.0
            candle_drops.append(drop_pct)

            if l > 0:
                range_pct = (h - l) / l * 100
            else:
                range_pct = 0.0
            candle_ranges.append(range_pct)

            # Volume ratio vs average of 3 candles before this one
            if idx >= 3:
                avg_vol_3 = float(np.mean(volumes[idx - 3: idx]))
            else:
                avg_vol_3 = 0.0
            vol_ratio = v / avg_vol_3 if avg_vol_3 > 0 else 0.0
            candle_vol_ratios.append(vol_ratio)

        # ── Detection logic ───────────────────────────────
        has_liquidation = False
        liq_type = None
        liq_severity = None
        max_drop = min(candle_drops) if candle_drops else 0.0
        total_drop = 0.0
        max_vol_ratio = max(candle_vol_ratios) if candle_vol_ratios else 0.0

        # Total drop from start to end of lookback window
        if recent_opens[0] > 0:
            total_drop = (recent_closes[-1] - recent_opens[0]) / recent_opens[0] * 100

        # 1. Cascade detection: 2+ consecutive candles with drop > 2% AND vol > 2x avg
        consecutive_drops = 0
        for i in range(lookback):
            if candle_drops[i] < -self.CASCADE_DROP_PCT and candle_vol_ratios[i] >= self.CASCADE_VOL_MULTIPLIER:
                consecutive_drops += 1
            else:
                consecutive_drops = 0

            if consecutive_drops >= 2:
                has_liquidation = True
                liq_type = "cascade"
                liq_severity = "high" if max_drop < -self.SINGLE_DROP_PCT else "medium"
                break

        # 2. Single-candle severe liquidation: one candle drops > 5% AND vol > 3x avg
        if not has_liquidation:
            for i in range(lookback):
                if candle_drops[i] < -self.SINGLE_DROP_PCT and candle_vol_ratios[i] >= self.SINGLE_VOL_MULTIPLIER:
                    has_liquidation = True
                    liq_type = "single"
                    liq_severity = "high"
                    break

        # 3. Liquidation wall breakout: single candle range > 8% AND vol spike
        if not has_liquidation:
            for i in range(lookback):
                if candle_ranges[i] >= self.RANGE_DROP_PCT and candle_vol_ratios[i] >= self.SINGLE_VOL_MULTIPLIER:
                    has_liquidation = True
                    liq_type = "single"
                    liq_severity = "high"
                    break

        # 4. Low severity: single candle drop > 3% AND vol > 2x avg
        if not has_liquidation:
            for i in range(lookback):
                if candle_drops[i] < -self.LOW_DROP_PCT and candle_vol_ratios[i] >= self.LOW_VOL_MULTIPLIER:
                    has_liquidation = True
                    liq_type = "single"
                    liq_severity = "low"
                    break

        return {
            "has_liquidation": has_liquidation,
            "liquidation_type": liq_type,
            "liquidation_severity": liq_severity,
            "total_drop_pct": round(total_drop, 2),
            "max_single_candle_drop": round(max_drop, 2),
            "volume_ratio": round(max_vol_ratio, 2),
        }

    def mark_liquidation_signal(self, liq_data: dict) -> dict[str, dict]:
        """
        Convert raw liquidation analysis into the extras scoring format.

        Args:
            liq_data: output from detect_liquidation_cascade()

        Returns:
            {symbol: {
                "liquidation_cascade": "high" | "medium" | "low" | None,
                "liquidation_score": int,
            }}
        """
        result: dict[str, dict] = {}
        severity_score_map = {"high": 10, "medium": 6, "low": 3}

        for sym, analysis in liq_data.items():
            severity = analysis.get("liquidation_severity")
            score = severity_score_map.get(severity, 0) if severity else 0

            result[sym] = {
                "liquidation_cascade": severity,
                "liquidation_score": score,
            }

        return result
