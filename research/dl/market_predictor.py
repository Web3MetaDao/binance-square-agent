# ─────────────────────────────────────────────────────────
# market_predictor.py — 市场状态预测模型
# 架构: LSTM-Attention (轻量) / TFT (高阶)
# 输入: OHLCV + VWAP (60bar) + 技术指标 + 外部特征
# 输出: 3分类 (涨/跌/横盘) 或 回归 (未来1h预期收益)
# ─────────────────────────────────────────────────────────

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

_MODEL_DIR = Path(__file__).resolve().parent / "models"
_MODEL_DIR.mkdir(parents=True, exist_ok=True)

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset

    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    logger.warning("PyTorch not available — using numpy fallback predictor")


# ─── Feature Engineering ────────────────────────────────────────

def compute_features(ohlcv: np.ndarray) -> np.ndarray:
    """Compute all input features from OHLCV array.

    Args:
        ohlcv: shape (T, 5) — [open, high, low, close, volume]

    Returns:
        Feature matrix shape (T, n_features) or (T, ) if only volume given.
    """
    T = ohlcv.shape[0]
    close = ohlcv[:, 3]
    high = ohlcv[:, 1]
    low = ohlcv[:, 2]
    volume = ohlcv[:, 4]

    features = []

    # Returns
    returns = np.diff(close, prepend=close[0]) / (close + 1e-10)
    features.append(returns)

    # Volatility (5-period)
    vol_5 = np.zeros(T)
    for i in range(5, T):
        vol_5[i] = np.std(returns[i - 4 : i + 1])
    vol_5[:5] = vol_5[5]
    features.append(vol_5)

    # Volume change
    vol_chg = np.diff(volume, prepend=volume[0]) / (volume + 1e-10)
    vol_chg = np.clip(vol_chg, -1.0, 1.0)
    features.append(vol_chg)

    # RSI(14)
    delta = np.diff(close, prepend=close[0])
    gain = np.maximum(delta, 0)
    loss = -np.minimum(delta, 0)
    avg_gain = np.full(T, 50.0)
    avg_loss = np.full(T, 50.0)
    for i in range(1, T):
        avg_gain[i] = (avg_gain[i - 1] * 13 + gain[i]) / 14
        avg_loss[i] = (avg_loss[i - 1] * 13 + loss[i]) / 14
    rs = avg_gain / (avg_loss + 1e-10)
    rsi = 100 - 100 / (1 + rs)
    features.append(rsi / 100.0)

    # ATR(14)
    tr = np.maximum(high - low, np.abs(high - np.roll(close, 1)))
    tr = np.maximum(tr, np.abs(low - np.roll(close, 1)))
    tr[0] = tr[1]
    atr = np.full(T, np.mean(tr[:14]))
    for i in range(14, T):
        atr[i] = (atr[i - 1] * 13 + tr[i]) / 14
    features.append(atr / (close + 1e-10))

    # MACD
    ema12 = _ema(close, 12)
    ema26 = _ema(close, 26)
    macd = ema12 - ema26
    signal = _ema(macd, 9)
    features.append(macd / (close + 1e-10))
    features.append((macd - signal) / (close + 1e-10))

    # BB (20, 2)
    bb_ma = _sma(close, 20)
    bb_std = np.zeros(T)
    for i in range(20, T):
        bb_std[i] = np.std(close[i - 19 : i + 1])
    bb_std[:20] = bb_std[20] if 20 < T else 0
    bb_width = (2 * bb_std) / (bb_ma + 1e-10)
    bb_pos = (close - bb_ma) / (2 * bb_std + 1e-10)
    features.append(bb_width)
    features.append(bb_pos)

    # OBV
    obv = np.zeros(T)
    for i in range(1, T):
        if close[i] > close[i - 1]:
            obv[i] = obv[i - 1] + volume[i]
        elif close[i] < close[i - 1]:
            obv[i] = obv[i - 1] - volume[i]
        else:
            obv[i] = obv[i - 1]
    obv_norm = obv / (np.max(np.abs(obv)) + 1e-10)
    features.append(obv_norm)

    result = np.column_stack(features) if len(features) > 1 else features[0]

    # Handle NaN/inf
    nan_count = np.isnan(result).sum()
    inf_count = np.isinf(result).sum()
    if nan_count > 0 or inf_count > 0:
        logger.warning(f"Feature computation produced {nan_count} NaN, {inf_count} Inf values")
    result = np.nan_to_num(result, nan=0.0, posinf=0.0, neginf=0.0)
    return result


def _ema(data: np.ndarray, period: int) -> np.ndarray:
    alpha = 2.0 / (period + 1)
    result = np.zeros_like(data)
    result[0] = data[0]
    for i in range(1, len(data)):
        result[i] = alpha * data[i] + (1 - alpha) * result[i - 1]
    return result


def _sma(data: np.ndarray, period: int) -> np.ndarray:
    result = np.zeros_like(data)
    for i in range(len(data)):
        if i < period:
            result[i] = np.mean(data[: i + 1])
        else:
            result[i] = np.mean(data[i - period + 1 : i + 1])
    return result


def build_sequences(features: np.ndarray, labels: np.ndarray,
                    window: int = 60) -> Tuple[np.ndarray, np.ndarray]:
    """Build (X, y) sliding window sequences."""
    T = len(features)
    X, y = [], []
    for i in range(window, T):
        X.append(features[i - window : i])
        y.append(labels[i])
    return np.array(X, dtype=np.float32), np.array(y, dtype=np.float32)


# ─── PyTorch Models ─────────────────────────────────────────────

if TORCH_AVAILABLE:

    class LSTMAttention(nn.Module):
        """LSTM with additive attention for market state prediction."""

        def __init__(self, input_dim: int, hidden_dim: int = 128,
                     num_layers: int = 2, num_classes: int = 3,
                     dropout: float = 0.2):
            super().__init__()
            self.lstm = nn.LSTM(
                input_dim, hidden_dim, num_layers,
                batch_first=True, dropout=dropout, bidirectional=True
            )
            self.attention = nn.Linear(hidden_dim * 2, 1)
            self.classifier = nn.Sequential(
                nn.Linear(hidden_dim * 2, 64),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(64, num_classes),
            )

        def forward(self, x):
            # x: (B, T, input_dim)
            lstm_out, _ = self.lstm(x)  # (B, T, hidden*2)
            attn_weights = torch.softmax(
                self.attention(lstm_out), dim=1
            )  # (B, T, 1)
            context = (lstm_out * attn_weights).sum(dim=1)  # (B, hidden*2)
            return self.classifier(context)

    class TFTBlock(nn.Module):
        """Simplified Temporal Fusion Transformer block."""

        def __init__(self, d_model: int = 64, nhead: int = 4, dropout: float = 0.1):
            super().__init__()
            self.self_attn = nn.MultiheadAttention(d_model, nhead, dropout=dropout,
                                                   batch_first=True)
            self.ffn = nn.Sequential(
                nn.Linear(d_model, d_model * 4),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(d_model * 4, d_model),
            )
            self.norm1 = nn.LayerNorm(d_model)
            self.norm2 = nn.LayerNorm(d_model)

        def forward(self, x):
            attn_out, _ = self.self_attn(x, x, x)
            x = self.norm1(x + attn_out)
            ffn_out = self.ffn(x)
            x = self.norm2(x + ffn_out)
            return x

    class TemporalFusionTransformer(nn.Module):
        """Temporal Fusion Transformer for market state prediction."""

        def __init__(self, input_dim: int, d_model: int = 64,
                     nhead: int = 4, num_layers: int = 2,
                     num_classes: int = 3, dropout: float = 0.1):
            super().__init__()
            self.input_proj = nn.Linear(input_dim, d_model)
            self.pos_encoding = nn.Parameter(torch.randn(1, 60, d_model) * 0.1)
            self.blocks = nn.ModuleList([
                TFTBlock(d_model, nhead, dropout) for _ in range(num_layers)
            ])
            self.classifier = nn.Sequential(
                nn.Linear(d_model, 32),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(32, num_classes),
            )

        def forward(self, x):
            # x: (B, T, input_dim)
            x = self.input_proj(x) + self.pos_encoding[:, : x.size(1), :]
            for block in self.blocks:
                x = block(x)
            # Global average pooling over time
            x = x.mean(dim=1)
            return self.classifier(x)


# ─── MarketPredictor ─────────────────────────────────────────────

class MarketPredictor:
    """Market state prediction model.

    Supports LSTM-Attention and Temporal Fusion Transformer architectures.
    Falls back to numpy-based momentum classifier when PyTorch is unavailable.

    Parameters
    ----------
    input_dim : int
        Number of input features.
    architecture : str
        'lstm' or 'tft'. Default 'lstm'.
    num_classes : int
        3 for up/down/sideways, 1 for regression.
    device : str, optional
        'cuda' or 'cpu'. Auto-detected if None.
    """

    def __init__(self, input_dim: int = 10, architecture: str = "lstm",
                 num_classes: int = 3, device: Optional[str] = None):
        self.input_dim = input_dim
        self.architecture = architecture.lower()
        self.num_classes = num_classes
        self.device = device or ("cuda" if TORCH_AVAILABLE and torch.cuda.is_available() else "cpu")

        self._model = None
        self._fitted = False
        self._feature_mean: Optional[np.ndarray] = None
        self._feature_std: Optional[np.ndarray] = None

        if TORCH_AVAILABLE:
            if self.architecture == "lstm":
                self._model = LSTMAttention(input_dim, num_classes=num_classes)
            elif self.architecture == "tft":
                self._model = TemporalFusionTransformer(input_dim, num_classes=num_classes)
            else:
                raise ValueError(f"Unknown architecture: {architecture}")
            self._model.to(self.device)
        else:
            logger.info("Using numpy fallback predictor")

    @property
    def is_fitted(self) -> bool:
        return self._fitted

    def fit(self, X: np.ndarray, y: np.ndarray,
            val_split: float = 0.15, epochs: int = 50,
            batch_size: int = 64, patience: int = 10,
            lr: float = 1e-3, weight_decay: float = 1e-4,
            verbose: bool = True) -> dict:
        """Train the model.

        Args:
            X: shape (N, T, input_dim) or (N, input_dim) for numpy fallback
            y: shape (N,) — class labels or regression targets
            val_split: validation fraction
            epochs: max training epochs
            batch_size: batch size
            patience: early stopping patience
            lr: learning rate
            weight_decay: AdamW weight decay
            verbose: print progress

        Returns:
            Training history dict
        """
        if not TORCH_AVAILABLE:
            return self._fit_numpy(X, y)

        n = len(X)
        n_val = max(1, int(n * val_split))
        indices = np.random.RandomState(42).permutation(n)
        train_idx = indices[:-n_val]
        val_idx = indices[-n_val:]

        X_t, y_t = X[train_idx], y[train_idx]
        X_v, y_v = X[val_idx], y[val_idx]

        # Normalize per feature
        self._feature_mean = X_t.mean(axis=0, keepdims=True)
        self._feature_std = X_t.std(axis=0, keepdims=True) + 1e-10
        X_t = (X_t - self._feature_mean) / self._feature_std
        X_v = (X_v - self._feature_mean) / self._feature_std

        # Convert to tensors
        X_t_t = torch.tensor(X_t, dtype=torch.float32).to(self.device)
        y_t_t = torch.tensor(y_t, dtype=torch.long).to(self.device)
        X_v_t = torch.tensor(X_v, dtype=torch.float32).to(self.device)
        y_v_t = torch.tensor(y_v, dtype=torch.long).to(self.device)

        loader = DataLoader(
            TensorDataset(X_t_t, y_t_t),
            batch_size=batch_size, shuffle=True
        )

        optimizer = optim.AdamW(self._model.parameters(), lr=lr, weight_decay=weight_decay)
        criterion = nn.CrossEntropyLoss() if self.num_classes > 1 else nn.MSELoss()

        best_val_loss = float("inf")
        best_state = None
        patience_counter = 0
        history = {"train_loss": [], "val_loss": [], "val_acc": []}

        for epoch in range(epochs):
            self._model.train()
            train_loss = 0.0
            for Xb, yb in loader:
                optimizer.zero_grad()
                out = self._model(Xb)
                loss = criterion(out, yb)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self._model.parameters(), 1.0)
                optimizer.step()
                train_loss += loss.item()
            train_loss /= len(loader)

            self._model.eval()
            with torch.no_grad():
                out_v = self._model(X_v_t)
                val_loss = criterion(out_v, y_v_t).item()
                if self.num_classes > 1:
                    preds = out_v.argmax(dim=1)
                    val_acc = (preds == y_v_t).float().mean().item()
                else:
                    val_acc = 0.0

            history["train_loss"].append(train_loss)
            history["val_loss"].append(val_loss)
            history["val_acc"].append(val_acc)

            if verbose:
                logger.info(
                    f"Epoch {epoch+1}/{epochs} — train_loss={train_loss:.4f}, "
                    f"val_loss={val_loss:.4f}, val_acc={val_acc:.4f}"
                )

            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_state = {k: v.clone().cpu() for k, v in self._model.state_dict().items()}
                patience_counter = 0
            else:
                patience_counter += 1
                if patience_counter >= patience:
                    if verbose:
                        logger.info(f"Early stopping at epoch {epoch+1}")
                    break

        if best_state is not None:
            self._model.load_state_dict(best_state)

        self._fitted = True
        history["best_val_loss"] = best_val_loss
        return history

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict class labels.

        Args:
            X: shape (N, T, input_dim) or (N, input_dim)

        Returns:
            Predicted class labels shape (N,)
        """
        if not self._fitted:
            raise RuntimeError("Model not fitted — call fit() first")

        if not TORCH_AVAILABLE:
            return self._predict_numpy(X)

        if self._feature_mean is not None:
            X = (X - np.squeeze(self._feature_mean)) / np.squeeze(self._feature_std)
        X_t = torch.tensor(X, dtype=torch.float32).to(self.device)
        self._model.eval()
        with torch.no_grad():
            out = self._model(X_t)
            if self.num_classes > 1:
                return out.argmax(dim=1).cpu().numpy()
            return out.cpu().numpy().flatten()

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Get class probabilities (classification only)."""
        if not TORCH_AVAILABLE or self.num_classes <= 1:
            return self.predict(X)

        if not self._fitted:
            raise RuntimeError("Model not fitted")
        if self._feature_mean is not None:
            X = (X - np.squeeze(self._feature_mean)) / np.squeeze(self._feature_std)
        X_t = torch.tensor(X, dtype=torch.float32).to(self.device)
        self._model.eval()
        with torch.no_grad():
            out = torch.softmax(self._model(X_t), dim=1)
            return out.cpu().numpy()

    def predict_market_state(self, ohlcv: np.ndarray) -> dict:
        """Convenience: compute features, predict, return analysis dict.

        Args:
            ohlcv: shape (T, 5) — at least 60 bars

        Returns:
            dict with predicted_class (0=down,1=sideways,2=up),
            confidence (0-1), expected_return (if regression)
        """
        features = compute_features(ohlcv)
        if len(features) < 60:
            return {"predicted_class": 1, "confidence": 0.0,
                    "expected_return": 0.0, "error": "Not enough data (need 60 bars)"}

        X = features[-60:].reshape(1, 60, self.input_dim)
        if not self._fitted:
            # Use simple momentum heuristic
            close = ohlcv[-60:, 3]
            short_ma = np.mean(close[-5:])
            long_ma = np.mean(close[-20:])
            ret = (close[-1] - close[0]) / close[0]
            if short_ma > long_ma and ret > 0.01:
                pred, conf, exp_ret = 2, 0.4, ret
            elif short_ma < long_ma and ret < -0.01:
                pred, conf, exp_ret = 0, 0.4, ret
            else:
                pred, conf, exp_ret = 1, 0.2, ret
            return {"predicted_class": pred, "confidence": conf,
                    "expected_return": exp_ret, "fallback": True}

        pred = int(self.predict(X)[0])
        if self.num_classes > 1:
            probs = self.predict_proba(X)[0]
            confidence = float(probs[pred])
            # Expected return based on historical avg return per class
            expected_return = (probs[2] - probs[0]) * 0.02
        else:
            confidence = 0.5
            expected_return = float(pred)

        return {
            "predicted_class": pred,
            "class_label": ["down", "sideways", "up"][pred],
            "confidence": confidence,
            "expected_return": expected_return,
        }

    def save(self, path: Optional[str] = None) -> str:
        """Save model to file."""
        if not TORCH_AVAILABLE or self._model is None:
            raise RuntimeError("No PyTorch model to save")

        path = path or str(_MODEL_DIR / f"market_predictor_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.pt")
        state = {
            "architecture": self.architecture,
            "input_dim": self.input_dim,
            "num_classes": self.num_classes,
            "model_state": self._model.state_dict(),
            "feature_mean": self._feature_mean,
            "feature_std": self._feature_std,
            "fitted": self._fitted,
        }
        # Convert numpy arrays to lists for safe pickle serialization
        safe_state: dict[str, Any] = {}
        for key, value in state.items():
            if isinstance(value, np.ndarray):
                safe_state[key] = value.tolist()
            elif isinstance(value, dict):
                # Handle nested dicts (e.g., model_state may contain tensors)
                safe_state[key] = {
                    k: v.tolist() if isinstance(v, np.ndarray) else v
                    for k, v in value.items()
                }
            else:
                safe_state[key] = value
        torch.save(safe_state, path)
        logger.info(f"Model saved to {path}")
        return path

    def load(self, path: str):
        """Load model from file."""
        if not TORCH_AVAILABLE:
            raise RuntimeError("PyTorch not available")
        state = torch.load(path, map_location=self.device, weights_only=True)
        self.architecture = state.get("architecture", "lstm")
        self.input_dim = state.get("input_dim", self.input_dim)
        self.num_classes = state.get("num_classes", self.num_classes)

        # Reconstruct numpy arrays from lists if needed
        raw_mean = state.get("feature_mean")
        self._feature_mean = np.array(raw_mean, dtype=np.float64) if raw_mean is not None else None
        raw_std = state.get("feature_std")
        self._feature_std = np.array(raw_std, dtype=np.float64) if raw_std is not None else None
        self._fitted = state.get("fitted", False)

        if self.architecture == "lstm":
            self._model = LSTMAttention(self.input_dim, num_classes=self.num_classes)
        else:
            self._model = TemporalFusionTransformer(self.input_dim, num_classes=self.num_classes)
        self._model.load_state_dict(state["model_state"])
        self._model.to(self.device)
        self._model.eval()
        logger.info(f"Model loaded from {path}")

    # ── Numpy fallback ─────────────────────────────────

    def _fit_numpy(self, X: np.ndarray, y: np.ndarray) -> dict:
        """Fit a simple logreg/momentum classifier without PyTorch."""
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler

        n = X.shape[0]
        n_val = max(1, int(n * 0.15))
        indices = np.random.RandomState(42).permutation(n)
        train_idx = indices[:-n_val]
        val_idx = indices[-n_val:]

        # Flatten time dimension: take last timestep features
        X_flat = X[:, -1, :] if X.ndim == 3 else X
        X_t, X_v = X_flat[train_idx], X_flat[val_idx]
        y_t, y_v = y[train_idx], y[val_idx]

        scaler = StandardScaler()
        X_t = scaler.fit_transform(X_t)
        X_v = scaler.transform(X_v)
        self._feature_mean = scaler.mean_
        self._feature_std = scaler.scale_

        clf = LogisticRegression(max_iter=1000, random_state=42, multi_class="multinomial" if self.num_classes > 2 else "auto")
        clf.fit(X_t, y_t)
        self._model = clf
        self._fitted = True

        val_acc = clf.score(X_v, y_v)
        logger.info(f"Numpy fallback fitted — val_acc={val_acc:.4f}")
        return {"val_acc": val_acc, "best_val_loss": 1.0 - val_acc}

    def _predict_numpy(self, X: np.ndarray) -> np.ndarray:
        X_flat = X[:, -1, :] if X.ndim == 3 else X
        if self._feature_mean is not None and self._feature_std is not None:
            X_flat = (X_flat - self._feature_mean) / self._feature_std
        return self._model.predict(X_flat)
