# ─────────────────────────────────────────────────────────
# optimizer.py — Optuna 超参数搜索
# 目标: 最大化夏普比率，限制最大回撤 < 15%
# ─────────────────────────────────────────────────────────

import json
import logging
import math
import random
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

try:
    import optuna
    from optuna.samplers import TPESampler

    OPTUNA_AVAILABLE = True
except ImportError:
    OPTUNA_AVAILABLE = False
    logger.warning("Optuna not available — using grid search fallback")


# ─── OptunaStrategyOptimizer ────────────────────────────────────


class OptunaStrategyOptimizer:
    """Hyperparameter optimizer using Optuna Bayesian search.

    Objective: maximize Sharpe ratio while constraining max_drawdown < 15%.
    Falls back to grid search if Optuna is not available.

    Parameters
    ----------
    objective_fn : Callable[[dict], dict]
        Function that takes a dict of params and returns
        ``{"sharpe_ratio": float, "max_drawdown": float, ...}``
    param_space : dict
        Parameter search space definition:
        ``{"param_name": {"type": "int"|"float"|"categorical",
           "low": ..., "high": ..., "steps": ..., "choices": [...]}}``
    n_trials : int
        Number of optimization trials (default 100).
    max_drawdown_limit : float
        Maximum allowable drawdown (default 0.15).
    """

    def __init__(
        self,
        objective_fn: Callable[[dict], dict],
        param_space: Dict[str, Dict[str, Any]],
        n_trials: int = 100,
        max_drawdown_limit: float = 0.15,
    ):
        self.objective_fn = objective_fn
        self.param_space = param_space
        self.n_trials = n_trials
        self.max_drawdown_limit = max_drawdown_limit
        self._best_params: Optional[dict] = None
        self._best_value: float = -float("inf")
        self._study: Any = None
        self._all_trials: List[dict] = []

    @property
    def best_params(self) -> Optional[dict]:
        return self._best_params

    @property
    def best_value(self) -> float:
        return self._best_value

    # ── Public API ──────────────────────────────────────

    def optimize(self) -> dict:
        """Run the hyperparameter search.

        Returns
        -------
        dict with keys:
            - best_params: best parameter combination
            - best_value: best Sharpe ratio
            - n_trials: number of trials run
            - max_drawdown_limit: constraint
            - all_trials: list of trial summaries
            - study_summary: text summary of the study
        """
        if OPTUNA_AVAILABLE:
            result = self._optimize_optuna()
        else:
            result = self._optimize_grid()

        self._best_params = result["best_params"]
        self._best_value = result["best_value"]
        logger.info(
            f"Optimization complete — best Sharpe={self._best_value:.4f}, "
            f"params={self._best_params}"
        )
        return result

    # ── Optuna backend ──────────────────────────────────

    def _optimize_optuna(self) -> dict:
        study = optuna.create_study(
            direction="maximize",
            sampler=TPESampler(seed=42),
            study_name="strategy_optimization",
        )
        study.optimize(self._optuna_objective, n_trials=self.n_trials, show_progress_bar=False)

        self._study = study

        trials_summary = []
        for t in study.trials:
            if t.values is not None:
                trials_summary.append({
                    "number": t.number,
                    "value": t.values[0],
                    "params": t.params,
                    "state": str(t.state),
                })

        return {
            "best_params": study.best_params,
            "best_value": study.best_value,
            "n_trials": len(study.trials),
            "max_drawdown_limit": self.max_drawdown_limit,
            "all_trials": trials_summary,
            "study_summary": study.best_trial.__str__(),
        }

    def _optuna_objective(self, trial: optuna.Trial) -> float:
        params = {}
        for name, space in self.param_space.items():
            ptype = space.get("type", "float")
            if ptype == "int":
                params[name] = trial.suggest_int(
                    name, space["low"], space["high"],
                    log=space.get("log", False),
                )
            elif ptype == "float":
                params[name] = trial.suggest_float(
                    name, space["low"], space["high"],
                    log=space.get("log", False),
                )
            elif ptype == "categorical":
                params[name] = trial.suggest_categorical(
                    name, space.get("choices", [])
                )
            else:
                params[name] = trial.suggest_float(name, 0.0, 1.0)

        try:
            result = self.objective_fn(params)
        except Exception as e:
            logger.warning(f"Trial failed: {e}")
            return -float("inf")

        drawdown = result.get("max_drawdown", 1.0)
        sharpe = result.get("sharpe_ratio", -10.0)

        # Constraint: penalize if max drawdown exceeds limit
        if drawdown > self.max_drawdown_limit:
            penalty = (drawdown - self.max_drawdown_limit) * 10.0
            sharpe -= penalty

        self._all_trials.append({
            "params": params,
            "result": result,
            "adjusted_sharpe": sharpe,
        })
        return sharpe

    # ── Grid search fallback ─────────────────────────────

    def _optimize_grid(self, grid_points: int = 5) -> dict:
        """Grid search fallback when Optuna is unavailable.

        Args:
            grid_points: number of points per continuous parameter dimension

        Returns:
            Same structure as _optimize_optuna
        """
        best_sharpe = -float("inf")
        best_params = {}
        all_trials = []

        # Generate grid points
        param_grid = self._build_grid(grid_points)
        keys = list(param_grid.keys())
        values = list(param_grid.values())

        from itertools import product
        total = 1
        for v in values:
            total *= len(v)

        actual_trials = min(total, self.n_trials * 2)

        for i, combo in enumerate(product(*values)):
            if i >= actual_trials:
                break

            params = dict(zip(keys, combo))

            try:
                result = self.objective_fn(params)
            except Exception as e:
                logger.warning(f"Grid trial {i} failed: {e}")
                continue

            sharpe = result.get("sharpe_ratio", -10.0)
            drawdown = result.get("max_drawdown", 1.0)

            if drawdown > self.max_drawdown_limit:
                penalty = (drawdown - self.max_drawdown_limit) * 10.0
                sharpe -= penalty

            all_trials.append({
                "params": params,
                "result": result,
                "adjusted_sharpe": sharpe,
            })

            if sharpe > best_sharpe:
                best_sharpe = sharpe
                best_params = params

        return {
            "best_params": best_params,
            "best_value": best_sharpe,
            "n_trials": len(all_trials),
            "max_drawdown_limit": self.max_drawdown_limit,
            "all_trials": all_trials[:20],  # Limit output size
            "study_summary": f"Grid search: {len(all_trials)} trials, best Sharpe={best_sharpe:.4f}",
        }

    def _build_grid(self, grid_points: int) -> Dict[str, list]:
        """Convert param_space to discrete grid values."""
        grid = {}
        for name, space in self.param_space.items():
            ptype = space.get("type", "float")
            if ptype == "categorical":
                grid[name] = space.get("choices", [True, False])
            elif ptype == "int":
                low, high = space["low"], space["high"]
                pts = min(grid_points, high - low + 1)
                grid[name] = [int(low + i * (high - low) / max(pts - 1, 1))
                              for i in range(pts)]
            else:
                low, high = space["low"], space["high"]
                log_scale = space.get("log", False)
                if log_scale:
                    grid[name] = [math.exp(math.log(low) + i * (math.log(high) - math.log(low)) / max(grid_points - 1, 1))
                                  for i in range(grid_points)]
                else:
                    grid[name] = [low + i * (high - low) / max(grid_points - 1, 1)
                                  for i in range(grid_points)]
        return grid

    # ── Surge Scanner specific presets ───────────────────

    @classmethod
    def surge_scanner_param_space(cls) -> Dict[str, Dict[str, Any]]:
        """Return a default parameter space for surge_scanner_v2 style strategies."""
        return {
            "rsi_oversold": {
                "type": "int", "low": 20, "high": 45, "log": False
            },
            "rsi_overbought": {
                "type": "int", "low": 55, "high": 80, "log": False
            },
            "volume_multiplier": {
                "type": "float", "low": 1.0, "high": 5.0, "log": True
            },
            "volatility_threshold": {
                "type": "float", "low": 0.01, "high": 0.10, "log": True
            },
            "ema_fast": {
                "type": "int", "low": 3, "high": 21, "log": False
            },
            "ema_slow": {
                "type": "int", "low": 20, "high": 100, "log": False
            },
            "stop_loss_pct": {
                "type": "float", "low": 0.01, "high": 0.08, "log": True
            },
            "take_profit_pct": {
                "type": "float", "low": 0.02, "high": 0.20, "log": True
            },
            "min_trend_strength": {
                "type": "float", "low": 10.0, "high": 50.0, "log": False
            },
        }

    @classmethod
    def create_for_surge_scanner(
        cls,
        objective_fn: Callable[[dict], dict],
        n_trials: int = 100,
    ) -> "OptunaStrategyOptimizer":
        """Convenience constructor with surge scanner defaults."""
        return cls(
            objective_fn=objective_fn,
            param_space=cls.surge_scanner_param_space(),
            n_trials=n_trials,
        )
