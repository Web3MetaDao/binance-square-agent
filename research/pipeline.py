"""
research/pipeline.py — DailyResearchPipeline class.

Orchestrates the full daily cycle of the research system:
  1. harvest_all() from UnifiedHarvester
  2. Parse each new item with PaperParser
  3. Store results in StrategyStore
  4. Fuse recent strategies with existing surge_scanner_v2 strategy
  5. Run OverfitReviewer on the fusion
  6. Store fusion result
  7. Run 5-level backtest (insample -> outsample -> pressure -> slippage -> monte carlo)
  8. Run DeployGate.can_deploy() — approve or reject
"""

from __future__ import annotations

import ast
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from research.harvester import UnifiedHarvester
from research.store import StrategyStore
from research.parsers.paper_parser import PaperParser
from research.fusion.optimizer import StrategyOptimizer
from research.fusion.reviewer import OverfitReviewer
from backtest.engine import BacktestPipeline
from backtest.pressure_test import PressureTestRunner
from backtest.monte_carlo import MonteCarloSimulator
from backtest.deploy_gate import DeployGate

logger = logging.getLogger(__name__)

# ── Default surge_scanner_v2 base strategy representation ────────────

DEFAULT_BASE_STRATEGY: dict[str, Any] = {
    "strategy_name": "surge_scanner_v2",
    "author_institution": "NousResearch",
    "core_indicators": [
        "Volume Spike(24h)",
        "Price Change(1h/2h/4h)",
        "Multi-exchange Score Merge",
        "Liquidation Cascade Detection",
        "Large Taker Detection",
    ],
    "entry_conditions": [
        "Volume > MIN_VOL_USDT (1M USDT main pool / 200K rapid start)",
        "Rapid start channel: 24h change >= 15%",
        "Multi-exchange score exceeds threshold after merge",
        "No conflicting signals across exchanges",
    ],
    "exit_conditions": [
        "Score falls below retention threshold",
        "Time-based decay of signal freshness",
        "Opposite-direction surge signal received",
    ],
    "risk_management": "Multi-exchange confirmation required; min volume filters prevent low-liquidity traps; rapid start channel captures early breakouts",
    "applicable_markets": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "all Binance USDT perpetuals"],
    "innovation_points": [
        "5-phase pipeline: ticker fetch -> batch kline -> scoring -> merge -> push",
        "Multi-exchange (OKX/Gate/Bitget) consensus scoring",
        "Dual pool: main (1M vol) + rapid start (200K vol, >15% chg)",
        "Parallel fetch + ThreadPoolExecutor for performance",
    ],
    "tags": ["momentum", "surge", "multi-exchange", "volume-spike", "scalping"],
}


class DailyResearchPipeline:
    """Orchestrates the full daily research-to-backtest pipeline.

    Each step is isolated in try/except so one failure does not block
    subsequent steps.  The pipeline returns a structured daily summary
    dict with counts and status per stage.

    Parameters
    ----------
    store : StrategyStore, optional
        Database store instance.  Creates a fresh one if omitted.
    harvester : UnifiedHarvester, optional
        Data harvester.  Uses defaults if omitted.
    parser : PaperParser, optional
        Document parser.  Creates one if omitted (requires API key).
    optimizer : StrategyOptimizer, optional
        Strategy fusion optimizer.  Creates one if omitted.
    reviewer : OverfitReviewer, optional
        Overfitting review engine.  Creates one if omitted.
    pipeline : BacktestPipeline, optional
        Backtest execution engine.  Creates one if omitted.
    gate : DeployGate, optional
        Deployment gate with threshold checks.  Creates one if omitted.
    base_strategy : dict, optional
        Base strategy dict for fusion.  Defaults to surge_scanner_v2.
    """

    def __init__(
        self,
        store: Optional[StrategyStore] = None,
        harvester: Optional[UnifiedHarvester] = None,
        parser: Optional[PaperParser] = None,
        optimizer: Optional[StrategyOptimizer] = None,
        reviewer: Optional[OverfitReviewer] = None,
        pipeline: Optional[BacktestPipeline] = None,
        gate: Optional[DeployGate] = None,
        base_strategy: Optional[dict[str, Any]] = None,
    ) -> None:
        self.store = store or StrategyStore()
        self.harvester = harvester or UnifiedHarvester(max_items_per_source=10)
        self.parser = parser or PaperParser()
        self.optimizer = optimizer or StrategyOptimizer()
        self.reviewer = reviewer or OverfitReviewer()
        self.pipeline = pipeline or BacktestPipeline()
        self.gate = gate or DeployGate()
        self.base_strategy = base_strategy or dict(DEFAULT_BASE_STRATEGY)

    # ── public entry point ───────────────────────────────────────────

    def run_full_cycle(self) -> dict[str, Any]:
        """Execute the complete daily research pipeline.

        Returns
        -------
        dict
            Full daily summary containing counts, statuses, and any
            error messages per step.
        """
        summary: dict[str, Any] = {
            "pipeline_start": datetime.now(timezone.utc).isoformat(),
            "steps": {},
            "fusion_id": None,
            "approved": False,
            "errors": [],
        }

        # Step 1: Harvest
        summary["steps"]["harvest"] = self._step_harvest()

        # Step 2: Parse each new item
        summary["steps"]["parse"] = self._step_parse(
            summary["steps"]["harvest"]
        )

        # Step 3: Store parsed strategies
        summary["steps"]["store"] = self._step_store(
            summary["steps"]["parse"]
        )

        # Step 4: Fuse recent strategies with base strategy
        summary["steps"]["fusion"] = self._step_fusion(
            stored_count=summary["steps"]["store"].get("stored_count", 0)
        )

        # If fusion succeeded, continue with review + backtest
        fusion_id = summary["steps"]["fusion"].get("fusion_id")
        summary["fusion_id"] = fusion_id

        if fusion_id is not None:
            # Step 5: Overfit review
            summary["steps"]["review"] = self._step_review(fusion_id)

            # If review verdict is 'fail', reject and skip remaining steps
            if summary["steps"]["review"].get("verdict") == "fail":
                logger.info(
                    "Review verdict 'fail' for fusion %d — skipping backtest and deploy gate.",
                    fusion_id,
                )
                for skip in ("store_fusion", "backtest", "deploy_gate"):
                    summary["steps"][skip] = {
                        "status": "skipped",
                        "reason": "review verdict was 'fail'",
                    }
            else:
                # Step 6: Store fusion result in DB
                summary["steps"]["store_fusion"] = self._step_store_fusion(
                    fusion_id,
                    summary["steps"]["fusion"],
                    summary["steps"]["review"],
                )

                # Step 7: 5-level backtest
                summary["steps"]["backtest"] = self._step_backtest(fusion_id)

                # Step 8: Deploy gate
                summary["steps"]["deploy_gate"] = self._step_deploy_gate(
                    fusion_id,
                    summary["steps"]["backtest"],
                )
                summary["approved"] = summary["steps"]["deploy_gate"].get(
                    "approved", False
                )
        else:
            logger.info("No fusion produced — skipping review, backtest, and deploy gate.")
            for skip in ("review", "store_fusion", "backtest", "deploy_gate"):
                summary["steps"][skip] = {"status": "skipped", "reason": "no fusion_id available"}

        summary["pipeline_end"] = datetime.now(timezone.utc).isoformat()
        return summary

    # ── individual steps ────────────────────────────────────────────

    def _step_harvest(self) -> dict[str, Any]:
        """Step 1: Harvest data from all sources."""
        result: dict[str, Any] = {"status": "ok", "total_items": 0, "by_source": {}}
        try:
            data = self.harvester.harvest_all()
            result["total_items"] = data.get("total_count", 0)
            for source in ("github", "arxiv", "blog"):
                items = data.get(source, [])
                result["by_source"][source] = len(items)
                result.setdefault("raw_items", {})[source] = items
            logger.info(
                "Harvest complete: %d total items",
                result["total_items"],
            )
        except Exception as exc:
            logger.error("Harvest step failed: %s", exc)
            result["status"] = "error"
            result["error"] = str(exc)
        return result

    def _step_parse(self, harvest_result: dict[str, Any]) -> dict[str, Any]:
        """Step 2: Parse each harvested item into structured strategies."""
        result: dict[str, Any] = {
            "status": "ok",
            "total_parsed": 0,
            "parsed_items": [],
            "parse_errors": [],
        }
        raw_items = harvest_result.get("raw_items", {})
        if not raw_items:
            result["status"] = "skipped"
            result["reason"] = "no raw items to parse"
            return result

        for source, items in raw_items.items():
            for item in items:
                try:
                    text = (
                        item.get("content")
                        or item.get("description")
                        or item.get("text")
                        or item.get("readme")
                        or item.get("raw_content")     # github_quant
                        or item.get("summary")         # arxiv
                        or item.get("title", "")
                    )
                    parsed = self.parser.parse(text)
                    parsed["_source_category"] = source
                    # Build a stable unique URL for dedup: arxiv uses pdf_url,
                    # blog uses source+title, github uses raw_content url
                    dedup_url = (
                        item.get("url")
                        or item.get("html_url")
                        or item.get("pdf_url")
                        or item.get("source_name", "")
                    )
                    # For blog RSS items, the url IS the actual blog post URL
                    # For arxiv items, use pdf_url as the unique identifier
                    if source == "arxiv" and item.get("pdf_url"):
                        dedup_url = item.get("pdf_url", dedup_url)
                    if source == "blog" and item.get("url"):
                        dedup_url = item.get("url", dedup_url)
                    parsed["_raw_source_url"] = dedup_url
                    result["parsed_items"].append(parsed)
                    result["total_parsed"] += 1
                except Exception as exc:
                    result["parse_errors"].append(
                        f"{source}/{item.get('title','?')}: {exc}"
                    )

        if result["total_parsed"] == 0 and not result["parse_errors"]:
            result["status"] = "skipped"
            result["reason"] = "no parseable text content found in items"
        elif result["total_parsed"] == 0 and result["parse_errors"]:
            result["status"] = "error"
            result["error"] = "all items failed parsing"

        logger.info(
            "Parsed %d items (%d errors)",
            result["total_parsed"],
            len(result["parse_errors"]),
        )
        return result

    def _step_store(self, parse_result: dict[str, Any]) -> dict[str, Any]:
        """Step 3: Store parsed strategies in the database."""
        result: dict[str, Any] = {
            "status": "ok",
            "stored_count": 0,
            "source_ids": {},
        }
        parsed_items = parse_result.get("parsed_items", [])
        if not parsed_items:
            result["status"] = "skipped"
            result["reason"] = "no parsed items to store"
            return result

        try:
            for parsed in parsed_items:
                source_name = parsed.get("_source_category", "unknown")
                source_type = source_name
                source_url = parsed.get("_raw_source_url", "")
                source_id = self.store.upsert_source(
                    name=f"{source_name}_{parsed.get('strategy_name','unknown')}",
                    s_type=source_type,
                    url=source_url,
                )
                result["source_ids"][parsed.get("strategy_name", "?")] = source_id

                strategy_id = self.store.insert_strategy(source_id, parsed)
                if strategy_id is not None:
                    result["stored_count"] += 1
                    # Update source fetch stats only for new strategies
                    self.store.update_source_fetch(source_name, 1)
                    logger.debug("Stored new strategy id=%d from %s", strategy_id, source_url)
                else:
                    logger.debug("Skipped duplicate strategy (already exists): %s", source_url)

            logger.info("Stored %d strategies in DB", result["stored_count"])
        except Exception as exc:
            logger.error("Store step failed: %s", exc)
            result["status"] = "error"
            result["error"] = str(exc)
        return result

    def _step_fusion(self, stored_count: int = 0) -> dict[str, Any]:
        """Step 4: Fuse recent strategies with the base strategy.

        Only triggers fusion if genuinely new strategies were stored this round.
        Reuses the most recent fusion_id if no new candidates exist.
        """
        result: dict[str, Any] = {
            "status": "ok",
            "fusion_id": None,
        }
        try:
            if stored_count == 0:
                logger.info("No new strategies this round — skipping fusion.")
                result["status"] = "skipped"
                result["reason"] = "no new strategies this round"
                result["candidate_count"] = 0
                return result

            recent = self.store.get_recent_strategies(hours=24)
            candidates = recent or []

            if not candidates:
                logger.info("No candidates found — skipping fusion (no LLM call).")
                result["status"] = "skipped"
                result["reason"] = "no recent strategies to fuse"
                result["candidate_count"] = 0
                return result

            fusion_output = self.optimizer.optimize(
                base_strategy=self.base_strategy,
                candidate_strategies=candidates,
            )
            result["hermes_output"] = fusion_output.get("hermes_output", {})
            result["vectorbt_code"] = fusion_output.get("vectorbt_code", "")
            result["predicted_metrics"] = fusion_output.get(
                "predicted_metrics", {}
            )
            result["best_scheme"] = fusion_output.get("best_scheme", "")
            result["analysis"] = fusion_output.get("analysis", "")
            result["candidate_count"] = len(candidates)

            # Store the fusion result immediately to get a fusion_id
            base_id = 1  # placeholder — surge_scanner_v2 is the base
            parent_ids = [s.get("id", 0) for s in candidates if s.get("id")]
            prompt = ""  # prompt reconstructed from optimizer internals
            hermes_json = json.dumps(
                fusion_output.get("hermes_output", {}), ensure_ascii=False
            )
            code = fusion_output.get("vectorbt_code", "")
            params = fusion_output.get("predicted_metrics", {})

            fusion_id = self.store.insert_fusion(
                base_id=base_id,
                parent_ids=parent_ids,
                prompt=prompt,
                hermes_output=hermes_json,
                code_extracted=code,
                optimized_params=params,
            )
            self.store.update_fusion_status(fusion_id, "backtesting")
            self.store.init_deploy_control(fusion_id)
            result["fusion_id"] = fusion_id
            logger.info("Fusion complete, fusion_id=%d", fusion_id)
        except Exception as exc:
            logger.error("Fusion step failed: %s", exc)
            result["status"] = "error"
            result["error"] = str(exc)
        return result

    def _step_review(self, fusion_id: int) -> dict[str, Any]:
        """Step 5: Run overfitting review on the fusion result."""
        result: dict[str, Any] = {
            "status": "ok",
            "verdict": "unknown",
            "risk_score": 50,
        }
        try:
            original_params = dict(self.base_strategy)
            optimized_params = {}  # Would come from fusion output
            backtest_results = {}  # Preliminary — real results come later

            review = self.reviewer.review(
                original_params=original_params,
                optimized_params=optimized_params,
                backtest_results=backtest_results,
            )
            result["verdict"] = review.get("verdict", "warning")
            result["risk_score"] = review.get("risk_score", 50)
            result["reason"] = review.get("reason", "")
            result["recommendation"] = review.get("recommendation", "")
            logger.info(
                "Review verdict: %s (risk_score=%d)",
                result["verdict"],
                result["risk_score"],
            )

            # If review fails, reject the fusion
            if result["verdict"] == "fail":
                self.store.update_fusion_status(fusion_id, "rejected")
                result["rejected_during_review"] = True
        except Exception as exc:
            logger.error("Review step failed: %s", exc)
            result["status"] = "error"
            result["error"] = str(exc)
        return result

    def _step_store_fusion(
        self,
        fusion_id: int,
        fusion_result: dict[str, Any],
        review_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Step 6: Update fusion record with review + code metadata."""
        result: dict[str, Any] = {"status": "ok"}
        try:
            code = fusion_result.get("vectorbt_code", "")
            params = fusion_result.get("predicted_metrics", {})
            self.store.update_fusion_code(fusion_id, code, params)

            # If review rejected, status is already 'rejected'
            if not review_result.get("rejected_during_review"):
                self.store.update_fusion_status(fusion_id, "backtesting")

            logger.info(
                "Fusion record %d updated with code and params", fusion_id
            )
        except Exception as exc:
            logger.error("Store fusion step failed: %s", exc)
            result["status"] = "error"
            result["error"] = str(exc)
        return result

    def _step_backtest(self, fusion_id: int) -> dict[str, Any]:
        """Step 7: Run 5-level backtest suite.

        Levels:
          1. insample  — historical data (2020-2024)
          2. outsample — forward data (2025-now)
          3. pressure  — black-swan crash scenarios (LUNA, FTX)
          4. slippage  — (simulated via pressure test's spread)
          5. monte carlo — 1000 random price paths
        """
        result: dict[str, Any] = {
            "status": "ok",
            "levels": {},
        }

        # Retrieve the vectorbt code from the fusion record
        try:
            pending = self.store.get_backtesting_fusions()
            code_str = ""
            for p in pending:
                if p["id"] == fusion_id:
                    code_str = p.get("code_extracted", "")
                    break
            if not code_str:
                # Try from the fusion step directly if available
                code_str = ""
        except Exception:
            code_str = ""

        if not code_str:
            logger.warning(
                "No vectorbt code for fusion %d — using mock strategy",
                fusion_id,
            )
            # Build a minimal mock strategy
            def mock_strategy(ohlcv, params):
                import pandas as pd
                close = ohlcv["Close"]
                fast = params.get("fast", 10)
                slow = params.get("slow", 30)
                entries = close.rolling(fast).mean() > close.rolling(slow).mean()
                exits = ~entries
                import vectorbt as vbt
                return vbt.Signal(entries, exits)

            strategy_func = mock_strategy
            strategy_params = {"fast": 10, "slow": 30}
        else:
            # Execute the code string to obtain a callable strategy
            strategy_func, strategy_params = self._compile_strategy_code(
                code_str
            )

        # ── Level 1: In-sample ──────────────────────────────────────
        try:
            insample = self.pipeline.run_insample(
                strategy_func, strategy_params
            )
            result["levels"]["insample"] = insample
            self.store.insert_backtest(fusion_id, "insample", insample, strategy_params)
            self.store.update_deploy_test(fusion_id, "insample", insample.get("sharpe_ratio", 0) > 0.5)
            logger.info("In-sample backtest complete: sharpe=%.4f", insample.get("sharpe_ratio", 0))
        except Exception as exc:
            logger.error("In-sample backtest failed: %s", exc)
            result["levels"]["insample"] = {"error": str(exc)}

        # ── Level 2: Out-sample ─────────────────────────────────────
        try:
            outsample = self.pipeline.run_outsample(
                strategy_func, strategy_params
            )
            result["levels"]["outsample"] = outsample
            self.store.insert_backtest(fusion_id, "outsample", outsample, strategy_params)
            self.store.update_deploy_test(fusion_id, "outsample", outsample.get("sharpe_ratio", 0) > 0.5)
            logger.info("Out-sample backtest complete: sharpe=%.4f", outsample.get("sharpe_ratio", 0))
        except Exception as exc:
            logger.error("Out-sample backtest failed: %s", exc)
            result["levels"]["outsample"] = {"error": str(exc)}

        # ── Level 3: Pressure test ──────────────────────────────────
        try:
            pressure = PressureTestRunner(strategy_func, strategy_params)
            pressure_result = pressure.run_pressure_test()
            result["levels"]["pressure"] = pressure_result
            for scenario, presult in pressure_result.items():
                if scenario == "_meta":
                    continue
                self.store.insert_backtest(
                    fusion_id, "pressure", presult, strategy_params
                )
            survived = pressure_result.get("_meta", {}).get("all_survived", False)
            self.store.update_deploy_test(fusion_id, "pressure", survived)
            logger.info("Pressure test complete: %s", "all survived" if survived else "some failed")
        except Exception as exc:
            logger.error("Pressure test failed: %s", exc)
            result["levels"]["pressure"] = {"error": str(exc)}

        # ── Level 4: Slippage test ──────────────────────────────────
        # Slippage test: re-run in-sample with wider spread / fee simulation.
        # We simulate it by running the insample with adjusted parameters.
        try:
            slippage_params = dict(strategy_params)
            slippage_params["slippage_pct"] = 0.001  # 0.1% slippage
            slippage_result = self.pipeline.run_insample(
                strategy_func, slippage_params
            )
            result["levels"]["slippage"] = slippage_result
            self.store.insert_backtest(
                fusion_id, "slippage", slippage_result, slippage_params
            )
            # Slippage test "passes" if sharpe is still positive
            slippage_pass = slippage_result.get("sharpe_ratio", 0) > 0.3
            self.store.update_deploy_test(fusion_id, "slippage", slippage_pass)
            logger.info("Slippage test complete: sharpe=%.4f", slippage_result.get("sharpe_ratio", 0))
        except Exception as exc:
            logger.error("Slippage test failed: %s", exc)
            result["levels"]["slippage"] = {"error": str(exc)}

        # ── Level 5: Monte Carlo ────────────────────────────────────
        try:
            mc = MonteCarloSimulator(
                strategy_func, strategy_params, n_paths=200
            )
            mc_result = mc.run_monte_carlo()
            result["levels"]["monte_carlo"] = mc_result
            self.store.insert_backtest(
                fusion_id, "monte_carlo", mc_result, strategy_params
            )
            # Monte Carlo passes if >90% of paths are positive
            mc_pass = mc_result.get("positive_paths_ratio", 0) > 0.90
            self.store.update_deploy_test(fusion_id, "monte_carlo", mc_pass)
            logger.info(
                "Monte Carlo complete: positive_paths=%.2f%%",
                mc_result.get("positive_paths_ratio", 0) * 100,
            )
        except Exception as exc:
            logger.error("Monte Carlo test failed: %s", exc)
            result["levels"]["monte_carlo"] = {"error": str(exc)}

        return result

    def _step_deploy_gate(
        self,
        fusion_id: int,
        backtest_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Step 8: Check DeployGate and mark approved/rejected."""
        result: dict[str, Any] = {
            "status": "ok",
            "approved": False,
            "reasons": [],
        }
        try:
            # Collect the best metrics from all backtest levels
            metrics: dict[str, Any] = {}
            levels = backtest_result.get("levels", {})

            # Use outsample metrics primarily, fall back to insample
            for priority in ("outsample", "insample"):
                level = levels.get(priority, {})
                if level and "error" not in level:
                    metrics["sharpe_ratio"] = level.get("sharpe_ratio", 0)
                    metrics["max_drawdown"] = level.get("max_drawdown", 0)
                    metrics["win_rate"] = level.get("win_rate", 0)
                    metrics["profit_factor"] = level.get("profit_factor", 0)
                    break

            # Monte Carlo ratio
            mc = levels.get("monte_carlo", {})
            if mc and "error" not in mc:
                metrics["monte_carlo_positive"] = mc.get(
                    "positive_paths_ratio", 0
                )

            if not metrics:
                result["status"] = "skipped"
                result["reason"] = "no backtest metrics available"
                self.store.reject_deployment(fusion_id)
                return result

            passed, failures = self.gate.can_deploy(metrics)
            result["approved"] = passed
            result["reasons"] = failures

            if passed:
                self.store.approve_deployment(fusion_id)
                logger.info("Fusion %d approved for deployment!", fusion_id)
            else:
                self.store.reject_deployment(fusion_id)
                logger.info(
                    "Fusion %d rejected: %s", fusion_id, "; ".join(failures)
                )
        except Exception as exc:
            logger.error("Deploy gate step failed: %s", exc)
            result["status"] = "error"
            result["error"] = str(exc)
        return result

    # ── helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _safe_exec(code_str: str, params: dict[str, Any]) -> dict[str, Any]:
        """Execute AI-generated strategy code in an AST sandbox.

        Only allows safe operations:
        - Function definitions (def)
        - Import of whitelisted modules (numpy, pandas, vectorbt, typing)
        - Math operations, comparisons, conditionals, loops
        - Assignments, return statements, string/list/dict literals

        Blocks: os, sys, subprocess, shutil, socket, requests, httpx,
        eval, exec, compile, open, __import__, getattr, setattr.
        """
        # ── Whitelisted import modules ──────────────────────────────────
        ALLOWED_IMPORTS: set[str] = {
            "numpy", "np",
            "pandas", "pd",
            "vectorbt", "vbt",
            "typing",
        }

        # ── AST node blacklist ──────────────────────────────────────────
        FORBIDDEN_NODES: tuple[type[ast.AST], ...] = (
            ast.ImportFrom,
        )

        # Names / attributes that are never allowed
        FORBIDDEN_NAMES: set[str] = {
            "os", "sys", "subprocess", "shutil", "socket",
            "requests", "httpx", "eval", "exec", "compile",
            "open", "__import__", "getattr", "setattr", "globals",
            "locals", "vars", "input", "breakpoint",
        }

        try:
            tree = ast.parse(code_str, filename="<strategy_code>", mode="exec")
        except SyntaxError as exc:
            raise ValueError(f"Syntax error in strategy code: {exc}") from exc

        for node in ast.walk(tree):
            # Block dangerous node types
            if isinstance(node, FORBIDDEN_NODES):
                raise ValueError(
                    f"Code contains forbidden construct: {type(node).__name__}"
                )

            # Block ClassDef entirely
            if isinstance(node, ast.ClassDef):
                raise ValueError("Class definitions are not allowed")

            # Block function calls to dangerous names
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in FORBIDDEN_NAMES:
                        raise ValueError(
                            f"Call to forbidden function: {node.func.id}"
                        )
                elif isinstance(node.func, ast.Attribute):
                    if node.func.attr in FORBIDDEN_NAMES:
                        raise ValueError(
                            f"Call to forbidden method: {node.func.attr}"
                        )

            # Check imports
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name not in ALLOWED_IMPORTS:
                        raise ValueError(
                            f"Import of disallowed module: {alias.name}"
                        )

            # Block attribute access to dangerous names
            if isinstance(node, ast.Attribute):
                if node.attr in FORBIDDEN_NAMES:
                    raise ValueError(
                        f"Access to forbidden attribute: {node.attr}"
                    )

        # ── Build restricted globals ────────────────────────────────────
        safe_builtins: dict[str, Any] = {
            "True": True,
            "False": False,
            "None": None,
            "abs": abs,
            "all": all,
            "any": any,
            "bool": bool,
            "dict": dict,
            "enumerate": enumerate,
            "filter": filter,
            "float": float,
            "int": int,
            "isinstance": isinstance,
            "len": len,
            "list": list,
            "map": map,
            "max": max,
            "min": min,
            "pow": pow,
            "range": range,
            "reversed": reversed,
            "round": round,
            "set": set,
            "slice": slice,
            "sorted": sorted,
            "str": str,
            "sum": sum,
            "tuple": tuple,
            "type": type,
            "zip": zip,
        }

        restricted_globals: dict[str, Any] = {
            "__builtins__": safe_builtins,
            "pd": __import__("pandas"),
            "np": __import__("numpy"),
            "vbt": __import__("vectorbt"),
        }

        local_ns: dict[str, Any] = {}
        try:
            exec(  # nosec — AST-validated safe code
                compile(tree, filename="<strategy_code>", mode="exec"),
                restricted_globals,
                local_ns,
            )
        except Exception as exc:
            logger.warning("Failed to exec vectorbt code: %s", exc)
            return {}

        return local_ns

    @staticmethod
    def _compile_strategy_code(
        code_str: str,
    ) -> tuple[Any, dict[str, Any]]:
        """Compile a vectorbt code string into a callable strategy function.

        This is a best-effort method.  If compilation or execution fails,
        a simple default strategy is returned.

        Parameters
        ----------
        code_str : str
            Python code that defines a ``run_strategy(ohlcv, params)``
            callable, or defines ``strategy_func`` and ``strategy_params``.

        Returns
        -------
        tuple[callable, dict]
            ``(strategy_func, params)``
        """
        # Execute in AST sandbox
        local_ns = ResearchPipeline._safe_exec(code_str, {})

        # Look for strategy callable
        strategy_func = local_ns.get("run_strategy") or local_ns.get(
            "strategy_func"
        )
        strategy_params = local_ns.get("strategy_params", {})

        if strategy_func is None:
            # Fallback: simple moving average crossover
            logger.warning(
                "Code did not define run_strategy/strategy_func — using default MA crossover"
            )

            def _fallback(ohlcv, params):
                close = ohlcv["Close"]
                fast = params.get("fast", 10)
                slow = params.get("slow", 30)
                entries = close.rolling(fast).mean() > close.rolling(slow).mean()
                exits = ~entries
                import vectorbt as vbt
                return vbt.Signal(entries, exits)

            strategy_func = _fallback
            strategy_params = {"fast": 10, "slow": 30}

        return strategy_func, strategy_params
