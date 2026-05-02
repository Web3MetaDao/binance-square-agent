"""
Strategy Engine — transforms fused signals into executable trading actions.

Strategies:
1. Breakout Follow (起涨点跟涨)
2. Smart Money Follow (聪明钱跟单)
3. OI Anomaly (OI异常放量)
4. Oversold Bounce (超跌反弹)
5. Sentiment Surge (情绪爆发)

Each strategy independently evaluates signals and produces Action proposals.
The DecisionEngine aggregates proposals and passes through RiskController.
"""

import time
from typing import Any, Optional

from okx_wrapper import OKXWrapper
from signal_fusion import SignalFusionEngine, FusedSignal
from risk_manager import RiskController


# ---------------------------------------------------------------------------
# Action model
# ---------------------------------------------------------------------------

class Action:
    """
    A proposed trading action from a strategy.
    """

    def __init__(
        self,
        action_type: str,       # "enter" | "exit" | "adjust"
        direction: str,         # "long" | "short" | "close"
        symbol: str,
        size_usd: float,
        entry_price: float,
        stop_loss_pct: float,
        take_profit_pct: float,
        strategy_source: str,
        confidence: float,
        reason: str = "",
    ):
        self.action_type = action_type
        self.direction = direction
        self.symbol = symbol
        self.size_usd = size_usd
        self.entry_price = entry_price
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.strategy_source = strategy_source
        self.confidence = confidence
        self.reason = reason
        self.timestamp = time.time()

    def to_dict(self) -> dict:
        return {
            "type": self.action_type,
            "direction": self.direction,
            "symbol": self.symbol,
            "size_usd": round(self.size_usd, 2),
            "entry_price": self.entry_price,
            "stop_loss_pct": self.stop_loss_pct,
            "take_profit_pct": self.take_profit_pct,
            "strategy": self.strategy_source,
            "confidence": round(self.confidence, 2),
            "reason": self.reason,
            "timestamp": self.timestamp,
        }


# ---------------------------------------------------------------------------
# Strategy base
# ---------------------------------------------------------------------------

class BaseStrategy:
    """Base class for all strategies."""

    def __init__(self, name: str):
        self.name = name

    def evaluate(self, signal: dict, risk: RiskController,
                 okx: OKXWrapper) -> list[Action]:
        """
        Evaluate a fused signal and return proposed actions.

        Args:
            signal: Fused signal dict from SignalFusionEngine.
            risk: RiskController for position checks.
            okx: OKXWrapper for additional data.

        Returns:
            List of Action proposals (empty list if no action warranted).
        """
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Strategy 1: Breakout Follow (起涨点跟涨)
# ---------------------------------------------------------------------------

class BreakoutFollowStrategy(BaseStrategy):
    """
    Follow breakouts: strong upward momentum + volume + smart money agreement.

    Triggers when:
    - Technical score > 25 (strong momentum)
    - Resonance: all sources agree bullish
    - Not already holding this coin
    """

    def __init__(self, min_score: float = 25.0):
        super().__init__("breakout_follow")
        self.min_score = min_score

    def evaluate(self, signal: dict, risk: RiskController,
                 okx: OKXWrapper) -> list[Action]:
        actions = []

        score = signal.get("score", 0)
        confidence = signal.get("confidence", 0)
        symbol = signal.get("symbol", "")
        resonance = signal.get("resonance", False)

        # Only evaluate bullish signals for breakout follow
        if score < self.min_score:
            return actions

        # Need at least moderate confidence
        if confidence < 0.4:
            return actions

        # Check if already holding this symbol
        open_positions = risk.get_open_positions()
        if any(p["symbol"] == symbol for p in open_positions):
            return actions

        # Check technical component details
        components = {c["name"]: c for c in signal.get("components", [])}
        tech = components.get("technical", {})
        details = tech.get("details", {})

        # Volume should be present
        vol_24h = details.get("vol24h", 0)
        if vol_24h < 100_000:
            return actions  # Too thin

        # Build reason
        reasons = []
        if resonance:
            reasons.append("三源共振")
        if details.get("chg24hPct", 0) > 5:
            reasons.append(f"涨幅{details['chg24hPct']:.1f}%")
        if details.get("oiDeltaPct", 0) > 10:
            reasons.append(f"OI+{details['oiDeltaPct']:.1f}%")

        # Determine position size
        size = risk.calculate_position_size()
        leverage = risk.calculate_leverage(size)
        sl_pct = risk.calculate_stop_loss(signal.get("entry_price", 0), "long")

        actions.append(Action(
            action_type="enter",
            direction="long",
            symbol=symbol,
            size_usd=size,
            entry_price=signal.get("entry_price", 0),
            stop_loss_pct=sl_pct,
            take_profit_pct=sl_pct * 2,  # 1:2 risk-reward
            strategy_source=self.name,
            confidence=confidence,
            reason=" | ".join(reasons),
        ))

        return actions


# ---------------------------------------------------------------------------
# Strategy 2: Smart Money Follow (聪明钱跟单)
# ---------------------------------------------------------------------------

class SmartMoneyFollowStrategy(BaseStrategy):
    """
    Follow smart money signals: trader consensus + capital inflow.

    Triggers when:
    - Smart Money score > 15
    - Resonance with at least one other source
    - Capital flow direction confirmed
    """

    def evaluate(self, signal: dict, risk: RiskController,
                 okx: OKXWrapper) -> list[Action]:
        actions = []
        symbol = signal.get("symbol", "")
        score = signal.get("score", 0)

        components = {c["name"]: c for c in signal.get("components", [])}
        sm = components.get("smart_money", {})
        details = sm.get("details", {})

        lsr = details.get("longShortRatio", 1)
        flow = details.get("capitalFlow", "")

        # Need smart money signal
        if abs(sm.get("score", 0)) < 15:
            return actions

        # Determine direction from smart money
        if sm["score"] < 0:
            direction = "short"
        else:
            direction = "long"

        # Check resonance - smart money alone is not enough
        resonance = signal.get("resonance", False)
        if not resonance and abs(score) < 30:
            return actions

        # Check if already holding
        open_positions = risk.get_open_positions()
        if any(p["symbol"] == symbol for p in open_positions):
            return actions

        # Size and risk
        size = risk.calculate_position_size()
        leverage = risk.calculate_leverage(size)
        sl_pct = risk.calculate_stop_loss(signal.get("entry_price", 0), "long" if direction == "long" else "short")

        reasons = [
            f"聪明钱{direction == 'long' and '做多' or '做空'}",
            f"多空比{lsr:.2f}" if lsr else "",
            f"资金{flow}" if flow else "",
        ]

        actions.append(Action(
            action_type="enter",
            direction=direction,
            symbol=symbol,
            size_usd=size,
            entry_price=signal.get("entry_price", 0),
            stop_loss_pct=sl_pct,
            take_profit_pct=sl_pct * 2,
            strategy_source=self.name,
            confidence=signal.get("confidence", 0.4),
            reason=" | ".join(filter(None, reasons)),
        ))

        return actions


# ---------------------------------------------------------------------------
# Strategy 3: OI Anomaly (OI异常放量)
# ---------------------------------------------------------------------------

class OIAnomalyStrategy(BaseStrategy):
    """
    Catch OI anomalies: large OI change + moderate price move.

    Triggers when:
    - |OI delta| > 20%
    - Price move < OI move (OI is leading, not following)
    - Technical score confirms direction
    """

    def evaluate(self, signal: dict, risk: RiskController,
                 okx: OKXWrapper) -> list[Action]:
        actions = []
        symbol = signal.get("symbol", "")

        components = {c["name"]: c for c in signal.get("components", [])}
        tech = components.get("technical", {})
        details = tech.get("details", {})

        oi_delta = details.get("oiDeltaPct", 0)
        chg_pct = details.get("chg24hPct", 0)

        # Need significant OI change
        if abs(oi_delta) < 20:
            return actions

        # OI increase + price up = genuine accumulation (long)
        # OI increase + price flat = potential accumulation (long)
        # OI decrease + price down = distribution (short)
        # OI decrease + price flat = potential distribution (short)

        if oi_delta > 20:
            # OI increasing
            if chg_pct > -3:  # Not deeply negative
                direction = "long"
            else:
                return actions  # OI up but price down - conflicting
        elif oi_delta < -20:
            # OI decreasing
            if chg_pct < 3:  # Not strongly positive
                direction = "short"
            else:
                return actions

        # Check if already holding
        open_positions = risk.get_open_positions()
        if any(p["symbol"] == symbol for p in open_positions):
            return actions

        size = risk.calculate_position_size()
        leverage = risk.calculate_leverage(size)
        sl_pct = risk.calculate_stop_loss(signal.get("entry_price", 0), direction)

        actions.append(Action(
            action_type="enter",
            direction=direction,
            symbol=symbol,
            size_usd=size,
            entry_price=signal.get("entry_price", 0),
            stop_loss_pct=sl_pct,
            take_profit_pct=sl_pct * 2,
            strategy_source=self.name,
            confidence=0.5,
            reason=f"OI异常{oi_delta:+.1f}% | 价格{chg_pct:+.1f}%",
        ))

        return actions


# ---------------------------------------------------------------------------
# Strategy 4: Oversold Bounce (超跌反弹)
# ---------------------------------------------------------------------------

class OversoldBounceStrategy(BaseStrategy):
    """
    Catch oversold bounces: big drop + sentiment fear + smart money accumulation.

    Triggers when:
    - Price dropped > 10% in 24h (negative technical score)
    - Sentiment deeply negative
    - BUT smart money starting to show long signals (divergence)
    """

    def evaluate(self, signal: dict, risk: RiskController,
                 okx: OKXWrapper) -> list[Action]:
        actions = []
        symbol = signal.get("symbol", "")

        components = {c["name"]: c for c in signal.get("components", [])}
        tech = components.get("technical", {})
        sm = components.get("smart_money", {})
        sent = components.get("sentiment", {})

        chg_pct = tech.get("details", {}).get("chg24hPct", 0)

        # Need a significant drop
        if chg_pct > -10:
            return actions

        # Sentiment should be negative (fear)
        sent_score = sent.get("score", 0)
        if sent_score > -10:
            return actions  # Not enough fear

        # Smart money should be turning bullish (divergence)
        sm_score = sm.get("score", 0)

        # DIRECT CONFLICT: price dropped 10%+ but smart money is long → bounce signal
        if sm_score < 5:
            return actions  # Smart money also bearish, no divergence

        # Check if already holding
        open_positions = risk.get_open_positions()
        if any(p["symbol"] == symbol for p in open_positions):
            return actions

        size = risk.calculate_position_size()
        leverage = risk.calculate_leverage(size)
        sl_pct = 5.0  # Tighter SL for bounce plays

        actions.append(Action(
            action_type="enter",
            direction="long",  # Always long for bounce
            symbol=symbol,
            size_usd=size,
            entry_price=signal.get("entry_price", 0),
            stop_loss_pct=sl_pct,
            take_profit_pct=10.0,  # Bigger target for bounce
            strategy_source=self.name,
            confidence=0.45,
            reason=f"超跌反弹{chg_pct:.1f}% | 聪明钱转多{sm_score:+.0f} | 情绪{sent_score:+.0f}",
        ))

        return actions


# ---------------------------------------------------------------------------
# Decision Engine
# ---------------------------------------------------------------------------

class DecisionEngine:
    """
    Aggregates strategy proposals, applies risk filters, and produces
    final executable actions.

    Flow:
    1. Get fused signals from SignalFusionEngine
    2. Each strategy evaluates independently
    3. Aggregate proposals, deduplicate, resolve conflicts
    4. Pass through RiskController
    5. Return final action list
    """

    def __init__(
        self,
        okx: Optional[OKXWrapper] = None,
        risk: Optional[RiskController] = None,
        signal_engine: Optional[SignalFusionEngine] = None,
    ):
        self.okx = okx or OKXWrapper()
        self.risk = risk or RiskController()
        self.signal_engine = signal_engine or SignalFusionEngine(self.okx)

        # Register strategies
        self.strategies: list[BaseStrategy] = [
            BreakoutFollowStrategy(),
            SmartMoneyFollowStrategy(),
            OIAnomalyStrategy(),
            OversoldBounceStrategy(),
        ]

        self._last_actions: list[Action] = []

    def run_cycle(self) -> list[dict]:
        """
        Run one complete decision cycle.

        1. Scan top setups via signal fusion
        2. Evaluate each signal against all strategies
        3. Filter through risk controller
        4. Return ranked actions

        Returns:
            List of action dicts, ranked by confidence descending.
        """
        # Check if trading is allowed
        allowed, reason = self.risk.can_trade()
        if not allowed:
            return [{
                "type": "blocked",
                "reason": reason,
                "timestamp": time.time(),
            }]

        # Get fused signals
        signals = self.signal_engine.scan_top_setups(limit=30)

        if not signals:
            return []

        # Each strategy evaluates all signals
        all_actions: list[Action] = []
        for strategy in self.strategies:
            for signal in signals:
                try:
                    actions = strategy.evaluate(signal, self.risk, self.okx)
                    all_actions.extend(actions)
                except Exception:
                    continue

        # Deduplicate by symbol + direction, keeping highest confidence
        seen: dict[tuple[str, str], Action] = {}
        for action in all_actions:
            key = (action.symbol, action.direction)
            if key not in seen or action.confidence > seen[key].confidence:
                seen[key] = action

        # Sort by confidence (highest first)
        actions = sorted(seen.values(), key=lambda a: a.confidence, reverse=True)

        # Limit to max 3 actions (matching MAX_POSITIONS)
        # But check if we have room
        open_count = len(self.risk.get_open_positions())
        room = max(0, 3 - open_count)
        enter_actions = [a for a in actions if a.action_type == "enter"]
        final_actions = enter_actions[:room]

        self._last_actions = final_actions
        return [a.to_dict() for a in final_actions]

    def check_positions(self) -> list[dict]:
        """
        Check all open positions against current market prices and update TP/SL.

        Returns:
            List of actions triggered (stop loss, take profit, trailing).
        """
        triggered: list[dict] = []
        positions = self.risk.get_open_positions()

        for pos in positions:
            try:
                ticker = self.okx.get_ticker(pos["symbol"])
                current_price = float(ticker.get("last", 0))

                result = self.risk.update_position_price(
                    pos["id"], current_price
                )

                if result is None:
                    # Position was closed
                    triggered.append({
                        "type": "position_closed",
                        "symbol": pos["symbol"],
                        "reason": pos.get("status", "unknown"),
                        "pnl_usd": pos.get("pnl_usd", 0),
                        "timestamp": time.time(),
                    })
                else:
                    # Check if trailing stop triggered update
                    if result.get("sl_note") == "trailing":
                        triggered.append({
                            "type": "trailing_stop_updated",
                            "symbol": pos["symbol"],
                            "new_sl_pct": result.get("stop_loss_pct"),
                            "timestamp": time.time(),
                        })
            except Exception:
                continue

        return triggered

    def get_status(self) -> dict:
        """Get full system status report."""
        state = self.risk.get_state_summary()
        state["open_positions_detail"] = self.risk.get_open_positions()
        state["recent_trades"] = self.risk.get_trade_history(limit=10)
        return state
