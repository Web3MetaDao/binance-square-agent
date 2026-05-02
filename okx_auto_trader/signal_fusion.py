"""
Signal Fusion Engine — combines OKX market data, smart money signals, and
sentiment data into unified trading signals with confidence scoring.

Output: per-coin score in [-100, 100], where positive = bullish, negative = bearish.
"""

import time
from datetime import datetime
from typing import Any, Optional

from okx_wrapper import OKXWrapper

# Weights for scoring components
TECHNICAL_WEIGHT = 40   # Price action + volume + OI
SMART_MONEY_WEIGHT = 30  # Trader consensus + capital flow
SENTIMENT_WEIGHT = 30    # News sentiment + social buzz

# Resonance multiplier when all 3 sources agree
RESONANCE_MULTIPLIER = 1.5

# Conflict threshold — sources disagree beyond this point
CONFLICT_THRESHOLD = 15  # points difference


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

class SignalComponent:
    """Score breakdown for one data source."""

    def __init__(self, name: str, score: float = 0.0, weight: float = 0.0,
                 details: Optional[dict] = None):
        self.name = name
        self.score = score          # -100 to +100
        self.weight = weight        # contribution weight
        self.details = details or {}

    @property
    def weighted_score(self) -> float:
        return self.score * (self.weight / 100.0)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "score": round(self.score, 1),
            "weight": self.weight,
            "weighted_score": round(self.weighted_score, 1),
            "details": self.details,
        }


class FusedSignal:
    """Aggregated signal for one coin."""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.components: list[SignalComponent] = []
        self.timestamp = time.time()

    def add_component(self, comp: SignalComponent) -> None:
        self.components.append(comp)

    @property
    def total_score(self) -> float:
        """Weighted sum of all components, in [-100, 100]."""
        if not self.components:
            return 0.0
        s = sum(c.weighted_score for c in self.components)
        # Normalize by total assigned weight
        total_w = sum(c.weight for c in self.components)
        if total_w > 0:
            s = s * (100.0 / total_w)
        return max(-100.0, min(100.0, s))

    @property
    def confidence(self) -> float:
        """Confidence based on component count and agreement."""
        if len(self.components) < 2:
            return 0.3
        scores = [c.score for c in self.components]
        agreement = 1.0 - (max(scores) - min(scores)) / 200.0
        # Penalize for few sources
        source_bonus = min(len(self.components) / 3.0, 1.0)
        return agreement * (0.5 + 0.5 * source_bonus)

    @property
    def resonance(self) -> bool:
        """True if all components agree (all positive or all negative)."""
        if len(self.components) < 2:
            return False
        signs = [c.score >= 0 for c in self.components]
        return all(signs) or not any(signs)

    @property
    def has_conflict(self) -> bool:
        """True if components significantly disagree."""
        if len(self.components) < 2:
            return False
        scores = [c.score for c in self.components]
        return (max(scores) - min(scores)) > CONFLICT_THRESHOLD

    @property
    def action_suggestion(self) -> str:
        """Suggested action based on score and confidence."""
        score = self.total_score
        conf = self.confidence

        if score > 40 and conf > 0.6:
            return "strong_long"
        elif score > 20 and conf > 0.4:
            return "long"
        elif score > 5 and conf > 0.3:
            return "cautious_long"
        elif score < -40 and conf > 0.6:
            return "strong_short"
        elif score < -20 and conf > 0.4:
            return "short"
        elif score < -5 and conf > 0.3:
            return "cautious_short"
        else:
            return "neutral"

    def to_dict(self) -> dict:
        # Apply resonance bonus
        score = self.total_score
        if self.resonance and len(self.components) >= 2:
            score *= RESONANCE_MULTIPLIER
            score = max(-100.0, min(100.0, score))

        return {
            "symbol": self.symbol,
            "score": round(score, 1),
            "confidence": round(self.confidence, 2),
            "resonance": self.resonance,
            "conflict": self.has_conflict,
            "action": self.action_suggestion,
            "components": [c.to_dict() for c in self.components],
            "timestamp": self.timestamp,
        }


# ---------------------------------------------------------------------------
# Signal Fusion Engine
# ---------------------------------------------------------------------------

class SignalFusionEngine:
    """
    Combines market data, smart money, and sentiment into unified signals.

    Args:
        okx: OKXWrapper instance.
    """

    def __init__(self, okx: Optional[OKXWrapper] = None):
        self.okx = okx or OKXWrapper()
        self._last_signals: list[FusedSignal] = []

    # ------------------------------------------------------------------
    # Technical analysis scoring
    # ------------------------------------------------------------------

    def _score_technical(self, symbol: str, ticker: Optional[dict] = None,
                         oi_data: Optional[list] = None) -> SignalComponent:
        """
        Score a coin based on technical factors.

        Components:
        - 24h price change (±15)
        - Volume anomaly (±10)
        - OI change (±10)
        - Optional: RSI (±5)
        """
        if ticker is None:
            try:
                ticker = self.okx.get_ticker(symbol)
            except Exception:
                ticker = {}

        total = 0.0
        details = {}

        # --- 24h price change ---
        chg_pct = float(ticker.get("chg24hPct", 0) or 0)
        if chg_pct > 10:
            total += 15       # Strong bull
        elif chg_pct > 5:
            total += 10
        elif chg_pct > 2:
            total += 5
        elif chg_pct > 0.5:
            total += 2
        elif chg_pct < -10:
            total -= 15       # Strong bear
        elif chg_pct < -5:
            total -= 10
        elif chg_pct < -2:
            total -= 5
        elif chg_pct < -0.5:
            total -= 2
        # else neutral

        details["chg24hPct"] = round(chg_pct, 2)

        # --- Volume anomaly (vs 24h avg) ---
        vol_24h = float(ticker.get("vol24h", 0) or 0)
        details["vol24h"] = int(vol_24h)

        # --- OI change ---
        oi_delta = 0.0
        if oi_data and isinstance(oi_data, list) and len(oi_data) > 0:
            # Latest OI bar's delta
            latest = oi_data[0]
            oi_delta = float(latest.get("oiDeltaPct", 0) or 0)
            if oi_delta > 30:
                total += 10    # Huge OI increase = accumulation
            elif oi_delta > 15:
                total += 7
            elif oi_delta > 8:
                total += 5
            elif oi_delta > 3:
                total += 3
            elif oi_delta < -30:
                total -= 10   # Massive OI drop = distribution
            elif oi_delta < -15:
                total -= 7
            elif oi_delta < -8:
                total -= 5
            elif oi_delta < -3:
                total -= 3
            details["oiDeltaPct"] = round(oi_delta, 2)

        # --- Funding rate check ---
        try:
            fr = self.okx.get_funding_rate(symbol)
            if isinstance(fr, dict):
                fr_rate = float(fr.get("fundingRate", 0) or 0) * 100
                details["fundingRate"] = round(fr_rate, 4)
                # Very high funding = crowded long = bearish contrarian signal
                if fr_rate > 0.1:
                    total -= 5
                elif fr_rate > 0.05:
                    total -= 3
                # Very negative funding = crowded short = bullish contrarian
                elif fr_rate < -0.1:
                    total += 5
                elif fr_rate < -0.05:
                    total += 3
        except Exception:
            pass

        clamped = max(-40.0, min(40.0, total))
        return SignalComponent(
            name="technical",
            score=clamped,
            weight=TECHNICAL_WEIGHT,
            details=details,
        )

    # ------------------------------------------------------------------
    # Smart money scoring
    # ------------------------------------------------------------------

    def _score_smartmoney(self, symbol: str) -> SignalComponent:
        """
        Score based on smart money consensus.

        Uses:
        - smartmoney signal (if available)
        - smartmoney overview (aggregated sentiment per coin)
        """
        total = 0.0
        details = {}

        try:
            signal = self.okx.get_smartmoney_signal()
            if isinstance(signal, dict):
                # Parse smart money signal structure
                sents = signal.get("sentiment", signal.get("data", {}))
                if isinstance(sents, list):
                    for entry in sents:
                        if symbol.upper() in str(entry.get("instId", "")).upper():
                            # Long/short ratio
                            lsr = float(entry.get("longShortRatio", 1) or 1)
                            if lsr > 1.5:
                                total += 10
                            elif lsr > 1.2:
                                total += 5
                            elif lsr < 0.6:
                                total -= 10
                            elif lsr < 0.8:
                                total -= 5
                            details["longShortRatio"] = round(lsr, 2)

                            # Capital flow direction
                            flow = entry.get("capitalFlowDirection", entry.get("flowDirection", ""))
                            if "in" in flow.lower() or "long" in flow.lower():
                                total += 10
                            elif "out" in flow.lower() or "short" in flow.lower():
                                total -= 10
                            details["capitalFlow"] = flow

                            # Position conviction
                            conviction = float(entry.get("conviction", entry.get("positionConviction", 0)) or 0)
                            details["conviction"] = round(conviction, 2)
                            break
                elif isinstance(sents, dict):
                    # Single coin signal
                    lsr = float(sents.get("longShortRatio", 1) or 1)
                    if lsr > 1.5:
                        total += 10
                    elif lsr > 1.2:
                        total += 5
                    elif lsr < 0.6:
                        total -= 10
                    elif lsr < 0.8:
                        total -= 5
                    details["longShortRatio"] = round(lsr, 2)
        except Exception:
            details["error"] = "smartmoney API not available"

        clamped = max(-30.0, min(30.0, total))
        return SignalComponent(
            name="smart_money",
            score=clamped,
            weight=SMART_MONEY_WEIGHT,
            details=details,
        )

    # ------------------------------------------------------------------
    # Sentiment scoring
    # ------------------------------------------------------------------

    def _score_sentiment(self, symbol: str) -> SignalComponent:
        """
        Score based on news sentiment and social buzz.

        Uses:
        - news coin-sentiment for individual coin sentiment
        - news sentiment-rank for relative ranking
        """
        total = 0.0
        details = {}
        base_symbol = symbol.replace("-SWAP", "").replace("-USDT", "")

        try:
            # Get per-coin sentiment
            coin_sent = self.okx.get_news_coin_sentiment(coins=[base_symbol])
            if isinstance(coin_sent, dict):
                coin_sent = [coin_sent]
            if isinstance(coin_sent, list):
                for entry in coin_sent:
                    score_val = float(entry.get("score", entry.get("sentimentScore", 0)) or 0)
                    if score_val > 50:
                        total += 10
                    elif score_val > 20:
                        total += 5
                    elif score_val > 0:
                        total += 2
                    elif score_val < -50:
                        total -= 10
                    elif score_val < -20:
                        total -= 5
                    elif score_val < 0:
                        total -= 2
                    details["sentimentScore"] = round(score_val, 1)

                    trend = entry.get("trend", entry.get("sentimentTrend", ""))
                    if "rising" in str(trend).lower() or "up" in str(trend).lower():
                        total += 5
                    elif "falling" in str(trend).lower() or "down" in str(trend).lower():
                        total -= 5
                    details["trend"] = trend
        except Exception:
            details["error"] = "coin sentiment API not available"

        # Try sentiment rank for relative strength
        try:
            rank_data = self.okx.get_news_sentiment_rank(sort_by="bullish", limit=30)
            if isinstance(rank_data, list):
                for i, entry in enumerate(rank_data):
                    coin = entry.get("coin", entry.get("instId", "")).upper()
                    if base_symbol.upper() in coin:
                        details["bullishRank"] = i + 1
                        if i < 5:
                            total += 10
                        elif i < 10:
                            total += 5
                        elif i < 20:
                            total += 2

            # Also check bearish side
            bearish_data = self.okx.get_news_sentiment_rank(sort_by="bearish", limit=30)
            if isinstance(bearish_data, list):
                for i, entry in enumerate(bearish_data):
                    coin = entry.get("coin", entry.get("instId", "")).upper()
                    if base_symbol.upper() in coin:
                        details["bearishRank"] = i + 1
                        if i < 5:
                            total -= 10
                        elif i < 10:
                            total -= 5
                        elif i < 20:
                            total -= 2
        except Exception:
            pass

        clamped = max(-30.0, min(30.0, total))
        return SignalComponent(
            name="sentiment",
            score=clamped,
            weight=SENTIMENT_WEIGHT,
            details=details,
        )

    # ------------------------------------------------------------------
    # Fusion
    # ------------------------------------------------------------------

    def fuse_signal(self, symbol: str,
                    ticker: Optional[dict] = None,
                    oi_data: Optional[list] = None) -> FusedSignal:
        """
        Generate a fused signal for one coin.

        Args:
            symbol: Instrument ID (e.g., 'BTC-USDT', 'ETH-USDT-SWAP').
            ticker: Pre-fetched ticker dict (optional, will fetch if None).
            oi_data: Pre-fetched OI data (optional).

        Returns:
            FusedSignal with full breakdown.
        """
        signal = FusedSignal(symbol)

        # Technical
        tech = self._score_technical(symbol, ticker, oi_data)
        signal.add_component(tech)

        # Smart Money
        sm = self._score_smartmoney(symbol)
        signal.add_component(sm)

        # Sentiment
        sent = self._score_sentiment(symbol)
        signal.add_component(sent)

        return signal

    def scan_top_setups(self, limit: int = 20,
                        min_vol_usd: float = 100_000) -> list[dict]:
        """
        Scan market for top trading setups.

        1. Get top movers from market filter
        2. Get OI change data
        3. Score each with full fusion
        4. Return ranked list

        Args:
            limit: Number of instruments to scan.
            min_vol_usd: Minimum 24h volume filter.

        Returns:
            List of signal dicts, sorted by |score| descending.
        """
        # Step 1: Top gainers (for long opportunities)
        gainers = self.okx.get_market_filter(
            instType="SWAP",
            sortBy="chg24hPct",
            sortOrder="desc",
            limit=limit,
            minVolUsd24h=min_vol_usd,
        )
        if not isinstance(gainers, list):
            gainers = []

        # Step 2: Top losers (for short opportunities)
        losers = self.okx.get_market_filter(
            instType="SWAP",
            sortBy="chg24hPct",
            sortOrder="asc",
            limit=limit,
            minVolUsd24h=min_vol_usd,
        )
        if not isinstance(losers, list):
            losers = []

        # Step 3: OI change data
        try:
            oi_changes = self.okx.get_oi_change(
                instType="SWAP",
                limit=limit,
                min_vol_usd_24h=min_vol_usd,
            )
        except Exception:
            oi_changes = []

        if not isinstance(oi_changes, list):
            oi_changes = []

        oi_index: dict[str, list] = {}
        for entry in oi_changes:
            inst = entry.get("instId", "")
            oi_index.setdefault(inst, []).append(entry)

        # Step 4: Fuse signals
        seen: set[str] = set()
        signals: list[FusedSignal] = []

        all_candidates = []
        all_candidates.extend(gainers or [])
        all_candidates.extend(losers or [])
        all_candidates.extend(oi_changes or [])

        for entry in all_candidates:
            if not isinstance(entry, dict):
                continue
            symbol = entry.get("instId", "")
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)

            try:
                oi = oi_index.get(symbol)
                signal = self.fuse_signal(symbol, ticker=entry, oi_data=oi)
                if signal.total_score != 0 or signal.confidence > 0.3:
                    signals.append(signal)
            except Exception:
                continue

        # Sort by |score| desc (strongest signals first)
        signals.sort(key=lambda s: abs(s.total_score), reverse=True)

        self._last_signals = signals
        return [s.to_dict() for s in signals]

    def get_conflict_signals(self) -> list[dict]:
        """Return signals where sources disagree."""
        return [s.to_dict() for s in self._last_signals if s.has_conflict]

    def get_resonance_signals(self) -> list[dict]:
        """Return signals where all sources agree."""
        return [s.to_dict() for s in self._last_signals if s.resonance]
