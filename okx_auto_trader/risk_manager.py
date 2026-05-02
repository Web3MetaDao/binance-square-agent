"""
Risk Manager & Position Manager — core risk control for 100U → 2000U system.

KEY RULES (100U initial, target 2000U):
  - Single position risk: 10-15% of current equity
  - Leverage: 3-5x (isolated margin only)
  - Max concurrent positions: 3
  - Max total exposure: 60% of equity
  - Daily loss limit: 20% → trigger pause
  - Consecutive loss limit: 3 → pause until next day
  - Total drawdown limit: 30% from peak → clear all + STOP
"""

import json
import os
import time
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional
from enum import Enum


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
STATE_FILE = os.path.join(DATA_DIR, "risk_state.json")
HISTORY_FILE = os.path.join(DATA_DIR, "trade_history.json")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INITIAL_EQUITY = 100.0
TARGET_EQUITY = 2000.0

MAX_POSITIONS = 3
MAX_EXPOSURE_PCT = 0.60        # max 60% of equity in positions
SINGLE_RISK_PCT = 0.15         # max 15% per trade
SINGLE_RISK_MIN_PCT = 0.10     # min 10% per trade (to avoid tiny positions)
LEVERAGE_MIN = 3
LEVERAGE_MAX = 5
DAILY_LOSS_LIMIT_PCT = 0.20    # 20% daily loss → pause
MAX_CONSECUTIVE_LOSSES = 3
MAX_DRAWDOWN_PCT = 0.30        # 30% from peak → STOP
TAKE_PROFIT_RATIO = 2.0        # TP = SL * 2 (1:2 risk-reward)

# Trailing stop: when price moves X% in our favor, move stop by Y%
TRAILING_ACTIVATE_PCT = 5.0
TRAILING_DISTANCE_PCT = 3.0


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Direction(Enum):
    LONG = "long"
    SHORT = "short"

    def __str__(self):
        return self.value


class PositionStatus(Enum):
    OPEN = "open"
    CLOSED = "closed"
    STOPPED = "stopped"
    TAKEN_PROFIT = "take_profit"


class CircuitBreakerState(Enum):
    NORMAL = "normal"
    DAILY_LOSS_PAUSED = "daily_loss_paused"
    CONSECUTIVE_LOSS_PAUSED = "consecutive_loss_paused"
    DRAWDOWN_STOPPED = "drawdown_stopped"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Position:
    """Single open position."""
    id: str                          # unique: symbol + open_timestamp
    symbol: str
    direction: str                   # "long" or "short"
    entry_price: float
    current_price: float
    size_usd: float                  # position value in USD (before leverage)
    margin_usd: float                # margin used (size_usd / leverage)
    leverage: int
    stop_loss_pct: float             # e.g., 5.0 = 5% below entry
    take_profit_pct: float           # e.g., 10.0 = 10% above entry
    strategy_source: str
    opened_at: float
    closed_at: Optional[float] = None
    status: str = "open"
    pnl_usd: float = 0.0
    pnl_pct: float = 0.0


@dataclass
class TradeRecord:
    """Completed trade record."""
    symbol: str
    direction: str
    entry_price: float
    exit_price: float
    size_usd: float
    leverage: int
    pnl_usd: float
    pnl_pct: float
    strategy_source: str
    opened_at: float
    closed_at: float
    exit_reason: str                 # "stop_loss", "take_profit", "manual", "trailing_stop"


class RiskState:
    """Persistent risk state."""

    def __init__(self, initial_equity: float = INITIAL_EQUITY):
        self.initial_equity = initial_equity
        self.current_equity = initial_equity
        self.peak_equity = initial_equity
        self.circuit_breaker: str = CircuitBreakerState.NORMAL.value
        self.daily_pnl_usd = 0.0
        self.daily_date = ""
        self.consecutive_losses = 0
        self.total_trades = 0
        self.winning_trades = 0
        self.total_pnl_usd = 0.0
        self.positions: list = field(default_factory=list)
        self.last_updated = time.time()


# ---------------------------------------------------------------------------
# Risk Controller
# ---------------------------------------------------------------------------

class RiskController:
    """
    Global risk controller for the 100U→2000U trading system.

    Manages:
    - Position sizing
    - Circuit breakers
    - Trade history
    - Equity tracking
    - Drawdown protection
    """

    def __init__(self, state_file: str = STATE_FILE,
                 history_file: str = HISTORY_FILE):
        self.state_file = state_file
        self.history_file = history_file
        self._lock = threading.Lock()
        self._state: Optional[RiskState] = None
        self._load_state()

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _load_state(self) -> None:
        """Load state from disk or create default."""
        os.makedirs(DATA_DIR, exist_ok=True)
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file) as f:
                    data = json.load(f)
                self._state = RiskState()
                for k, v in data.items():
                    setattr(self._state, k, v)
                return
            except Exception:
                pass
        self._state = RiskState()

    def _save_state(self) -> None:
        """Persist state to disk."""
        with self._lock:
            self._state.last_updated = time.time()
            with open(self.state_file, "w") as f:
                json.dump(asdict(self._state), f, indent=2, default=str)

    def _load_history(self) -> list[dict]:
        """Load trade history."""
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file) as f:
                    return json.load(f)
            except Exception:
                pass
        return []

    def _save_history(self, trade: TradeRecord) -> None:
        """Append a trade to history."""
        history = self._load_history()
        history.append(asdict(trade))
        # Keep last 500 trades
        if len(history) > 500:
            history = history[-500:]
        with open(self.history_file, "w") as f:
            json.dump(history, f, indent=2, default=str)

    # ------------------------------------------------------------------
    # Daily check
    # ------------------------------------------------------------------

    def _check_daily_reset(self) -> None:
        """Reset daily counters if a new day has started."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._state.daily_date != today:
            self._state.daily_date = today
            self._state.daily_pnl_usd = 0.0
            self._state.consecutive_losses = 0
            self._state.circuit_breaker = CircuitBreakerState.NORMAL.value
            self._save_state()

    # ------------------------------------------------------------------
    # Circuit breakers
    # ------------------------------------------------------------------

    def can_trade(self) -> tuple[bool, str]:
        """
        Check if trading is allowed.

        Returns:
            (allowed, reason) tuple.
        """
        self._check_daily_reset()

        cb = self._state.circuit_breaker
        if cb == CircuitBreakerState.DRAWDOWN_STOPPED.value:
            return False, "DRAWDOWN_STOP: Equity dropped 30% from peak. Manual restart required."

        if cb == CircuitBreakerState.DAILY_LOSS_PAUSED.value:
            return False, f"DAILY_LOSS_PAUSED: Lost {abs(self._state.daily_pnl_usd):.1f}U today ({self._get_daily_loss_pct():.1f}%). Resumes tomorrow."

        if cb == CircuitBreakerState.CONSECUTIVE_LOSS_PAUSED.value:
            return False, f"CONSECUTIVE_LOSS: {self._state.consecutive_losses} losses in a row. Resumes tomorrow."

        # Check drawdown
        if self._state.current_equity < self._state.peak_equity * (1 - MAX_DRAWDOWN_PCT):
            self._state.circuit_breaker = CircuitBreakerState.DRAWDOWN_STOPPED.value
            self._save_state()
            return False, "DRAWDOWN_STOP: 30% drawdown triggered. All trades frozen."

        # Can't trade if equity is too low
        if self._state.current_equity < 5:
            return False, f"INSUFFICIENT_EQUITY: Only {self._state.current_equity:.1f}U remaining."

        # Position limit
        active = [p for p in self._state.positions if p.get("status") == "open"]
        if len(active) >= MAX_POSITIONS:
            return False, f"MAX_POSITIONS: Already have {len(active)} positions open."

        return True, "OK"

    # ------------------------------------------------------------------
    # Position sizing
    # ------------------------------------------------------------------

    def calculate_position_size(self) -> float:
        """
        Calculate recommended position size in USDT.

        Rule:
        - Base: 10-15% of current equity
        - Scale down as equity grows (keep the absolute USD risk manageable)
        """
        equity = self._state.current_equity

        # As equity grows, lower the % risk (protect profits)
        if equity <= 200:
            risk_pct = SINGLE_RISK_PCT  # 15%
        elif equity <= 500:
            risk_pct = 0.12              # 12%
        elif equity <= 1000:
            risk_pct = 0.10              # 10%
        elif equity <= 1500:
            risk_pct = 0.08              # 8%
        else:
            risk_pct = 0.06              # 6% for 1500-2000+

        position_value = equity * risk_pct

        # Minimum: 5U per position
        position_value = max(5.0, position_value)

        # Check against available equity (not exceeding 60% total)
        total_exposure = sum(
            p.get("margin_usd", 0) for p in self._state.positions
            if p.get("status") == "open"
        )
        available_exposure = equity * MAX_EXPOSURE_PCT - total_exposure
        position_value = min(position_value, available_exposure)

        return max(5.0, round(position_value, 1))

    def calculate_leverage(self, position_value: float) -> int:
        """
        Calculate appropriate leverage.

        Conservative for low equity, gradually increase as equity grows.
        """
        equity = self._state.current_equity
        win_rate = self.get_win_rate()

        if equity >= 500 and win_rate > 0.5:
            return 5
        elif equity >= 200:
            return 5
        elif equity >= 100:
            return 3
        else:
            return 3

    def calculate_stop_loss(self, entry_price: float, direction: str) -> float:
        """
        Calculate stop loss percentage from entry.

        Based on volatility. For now, use a fixed 5% for long, 4% for short.
        """
        if direction == Direction.LONG.value:
            return 5.0
        else:
            return 4.0

    # ------------------------------------------------------------------
    # Trade management
    # ------------------------------------------------------------------

    def open_position(self, symbol: str, direction: str, size_usd: float,
                      entry_price: float, leverage: int, stop_loss_pct: float,
                      take_profit_pct: float, strategy_source: str) -> Optional[Position]:
        """
        Open a new position if allowed.

        Returns:
            Position object if successful, None if blocked.
        """
        with self._lock:
            allowed, reason = self.can_trade()
            if not allowed:
                return None

            # Create position
            pos = Position(
                id=f"{symbol}_{int(time.time() * 1000)}",
                symbol=symbol,
                direction=direction,
                entry_price=entry_price,
                current_price=entry_price,
                size_usd=size_usd,
                margin_usd=round(size_usd / leverage, 2),
                leverage=leverage,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
                strategy_source=strategy_source,
                opened_at=time.time(),
            )

            self._state.positions.append(asdict(pos))
            self._save_state()
            return pos

    def update_position_price(self, position_id: str,
                              current_price: float) -> Optional[dict]:
        """
        Update a position's current price and check TP/SL/trailing.

        Returns position dict if still open, None if closed.
        """
        with self._lock:
            for i, p in enumerate(self._state.positions):
                if p.get("id") == position_id and p.get("status") == "open":
                    p["current_price"] = current_price

                    dir = p["direction"]
                    entry = p["entry_price"]
                    sl_pct = p["stop_loss_pct"]
                    tp_pct = p["take_profit_pct"]

                    if dir == Direction.LONG.value:
                        change_pct = (current_price - entry) / entry * 100
                        sl_price = entry * (1 - sl_pct / 100)
                        tp_price = entry * (1 + tp_pct / 100)

                        # Check stop loss
                        if current_price <= sl_price:
                            pnl_pct = -sl_pct
                            pnl_usd = p["margin_usd"] * (pnl_pct * p["leverage"] / 100)
                            return self._close_position(i, "stop_loss", current_price, pnl_usd, pnl_pct)

                        # Check take profit
                        if current_price >= tp_price:
                            pnl_pct = tp_pct
                            pnl_usd = p["margin_usd"] * (pnl_pct * p["leverage"] / 100)
                            return self._close_position(i, "take_profit", current_price, pnl_usd, pnl_pct)

                        # Trailing stop: if price gained > TRAILING_ACTIVATE_PCT
                        if change_pct >= TRAILING_ACTIVATE_PCT:
                            # Move stop loss to lock in profits
                            trailing_stop = current_price * (1 - TRAILING_DISTANCE_PCT / 100)
                            current_sl = entry * (1 - sl_pct / 100)
                            if trailing_stop > current_sl:
                                p["stop_loss_pct"] = (1 - trailing_stop / entry) * 100
                                p["sl_note"] = "trailing"

                    elif dir == Direction.SHORT.value:
                        change_pct = (entry - current_price) / entry * 100  # positive when price drops
                        sl_price = entry * (1 + sl_pct / 100)
                        tp_price = entry * (1 - tp_pct / 100)

                        if current_price >= sl_price:
                            pnl_pct = -sl_pct
                            pnl_usd = p["margin_usd"] * (pnl_pct * p["leverage"] / 100)
                            return self._close_position(i, "stop_loss", current_price, pnl_usd, pnl_pct)

                        if current_price <= tp_price:
                            pnl_pct = tp_pct
                            pnl_usd = p["margin_usd"] * (pnl_pct * p["leverage"] / 100)
                            return self._close_position(i, "take_profit", current_price, pnl_usd, pnl_pct)

                        # Trailing stop for shorts
                        if change_pct >= TRAILING_ACTIVATE_PCT:
                            trailing_stop = current_price * (1 + TRAILING_DISTANCE_PCT / 100)
                            current_sl = entry * (1 + sl_pct / 100)
                            if trailing_stop < current_sl:
                                p["stop_loss_pct"] = (trailing_stop / entry - 1) * 100
                                p["sl_note"] = "trailing"

                    pnl_pct_calc = change_pct * p["leverage"]
                    pnl_usd_calc = p["margin_usd"] * (pnl_pct_calc / 100)
                    p["pnl_pct"] = round(pnl_pct_calc, 2)
                    p["pnl_usd"] = round(pnl_usd_calc, 2)

                    self._save_state()
                    return p

        return None

    def close_position_by_id(self, position_id: str,
                             reason: str = "manual") -> Optional[dict]:
        """Close a position by ID."""
        with self._lock:
            for i, p in enumerate(self._state.positions):
                if p.get("id") == position_id and p.get("status") == "open":
                    pnl_usd = p.get("pnl_usd", 0)
                    pnl_pct = p.get("pnl_pct", 0)
                    return self._close_position(i, reason, p.get("current_price", p["entry_price"]), pnl_usd, pnl_pct)
        return None

    def close_all_positions(self, reason: str = "manual") -> list[dict]:
        """Close all open positions."""
        closed = []
        positions_copy = list(self._state.positions)
        for p in positions_copy:
            if p.get("status") == "open":
                result = self.close_position_by_id(p["id"], reason)
                if result:
                    closed.append(result)
        return closed

    def _close_position(self, index: int, reason: str,
                        exit_price: float, pnl_usd: float, pnl_pct: float) -> dict:
        """Internal: close position at given index, update equity and history."""
        p = self._state.positions[index]
        p["status"] = "closed"
        p["closed_at"] = time.time()
        p["pnl_usd"] = round(pnl_usd, 2)
        p["pnl_pct"] = round(pnl_pct, 2)

        # Update equity
        self._state.current_equity = round(self._state.current_equity + pnl_usd, 2)
        if self._state.current_equity > self._state.peak_equity:
            self._state.peak_equity = self._state.current_equity

        # Daily tracking
        self._state.daily_pnl_usd = round(self._state.daily_pnl_usd + pnl_usd, 2)
        self._state.total_pnl_usd = round(self._state.total_pnl_usd + pnl_usd, 2)
        self._state.total_trades += 1

        if pnl_usd > 0:
            self._state.winning_trades += 1
            self._state.consecutive_losses = 0
        else:
            self._state.consecutive_losses += 1

        # Save trade history
        trade = TradeRecord(
            symbol=p["symbol"],
            direction=p["direction"],
            entry_price=p["entry_price"],
            exit_price=exit_price,
            size_usd=p["size_usd"],
            leverage=p["leverage"],
            pnl_usd=round(pnl_usd, 2),
            pnl_pct=round(pnl_pct, 2),
            strategy_source=p["strategy_source"],
            opened_at=p["opened_at"],
            closed_at=time.time(),
            exit_reason=reason,
        )
        self._save_history(trade)

        # Check circuit breakers
        daily_loss_pct = self._get_daily_loss_pct()
        if daily_loss_pct >= DAILY_LOSS_LIMIT_PCT:
            self._state.circuit_breaker = CircuitBreakerState.DAILY_LOSS_PAUSED.value

        if self._state.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            self._state.circuit_breaker = CircuitBreakerState.CONSECUTIVE_LOSS_PAUSED.value

        self._save_state()
        return p

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_open_positions(self) -> list[dict]:
        """Return all open positions."""
        return [
            p for p in self._state.positions
            if p.get("status") == "open"
        ]

    def get_state_summary(self) -> dict:
        """Return a snapshot of current state."""
        self._check_daily_reset()

        open_positions = self.get_open_positions()
        total_exposure = sum(p.get("margin_usd", 0) for p in open_positions)

        return {
            "equity": round(self._state.current_equity, 2),
            "peak_equity": round(self._state.peak_equity, 2),
            "drawdown_pct": round(
                (1 - self._state.current_equity / self._state.peak_equity) * 100, 1
            ) if self._state.peak_equity > 0 else 0,
            "total_pnl": round(self._state.total_pnl_usd, 2),
            "daily_pnl": round(self._state.daily_pnl_usd, 2),
            "daily_date": self._state.daily_date,
            "circuit_breaker": self._state.circuit_breaker,
            "open_positions": len(open_positions),
            "max_positions": MAX_POSITIONS,
            "total_exposure_usd": round(total_exposure, 2),
            "max_exposure_usd": round(self._state.current_equity * MAX_EXPOSURE_PCT, 2),
            "total_trades": self._state.total_trades,
            "winning_trades": self._state.winning_trades,
            "win_rate": round(self.get_win_rate(), 2),
            "consecutive_losses": self._state.consecutive_losses,
            "progress_pct": round(
                (self._state.current_equity / TARGET_EQUITY) * 100, 1
            ),
        }

    def get_win_rate(self) -> float:
        """Calculate win rate."""
        if self._state.total_trades == 0:
            return 0.0
        return self._state.winning_trades / self._state.total_trades

    def _get_daily_loss_pct(self) -> float:
        """Calculate daily loss as % of initial equity."""
        if self._state.initial_equity == 0:
            return 0.0
        return abs(self._state.daily_pnl_usd) / self._state.initial_equity * 100

    def get_trade_history(self, limit: int = 20) -> list[dict]:
        """Get recent trade history."""
        history = self._load_history()
        return history[-limit:][::-1]  # newest first
