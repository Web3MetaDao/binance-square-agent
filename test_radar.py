"""
抓庄雷达系统 — 核心逻辑单元测试

覆盖模块:
  1. report.py     — 纯字符串格式化（无 API 依赖）
  2. swing.py      — 评分逻辑（无 API 依赖）
  3. coinalyze_calibrate.py — symbol 解析（无 API 依赖）
  4. market_data.py — symbol 转换（无 API 依赖）

运行:
  cd /root/binance-square-agent && python3 -m pytest test_radar.py -v
"""

import pytest

# ── 模块 1: report.py ─────────────────────────────────────


class TestReport:
    """报告格式化测试（纯字符串处理，无API依赖）。"""

    def test_build_swing_report(self):
        """build_swing_report 应包含 📈 和具体币名。"""
        from accumulation_radar.report import build_swing_report

        data = {
            "surge": [
                {
                    "coin": "PEPE",
                    "sym": "PEPEUSDT",
                    "score": 85.5,
                    "swing": 5.12,
                    "chg24h": 12.34,
                    "fr": -0.0021,
                    "vol": 500_000_000,
                    "px": 0.00001234,
                    "mcap": 1_000_000_000,
                    "window": "5m",
                    "reasons": "放量/CG热度",
                }
            ],
            "dump": [
                {
                    "coin": "DOGE",
                    "sym": "DOGEUSDT",
                    "score": 72.3,
                    "swing": -4.56,
                    "chg24h": -8.90,
                    "fr": 0.0015,
                    "vol": 300_000_000,
                    "px": 0.12,
                    "mcap": 15_000_000_000,
                    "window": "15m",
                    "reasons": "费率偏多",
                }
            ],
            "btc_chg24h": -0.55,
            "candidates_scanned": 350,
            "timestamp": "2025-01-01 12:00:00",
        }

        result = build_swing_report(data)

        assert result is not None
        assert "📈" in result, "应包含 📈 图标"
        assert "PEPE" in result, "结果应包含币名 PEPE"
        assert "DOGE" in result, "结果应包含币名 DOGE"
        assert "BTC" in result, "应包含 BTC 24h 涨跌幅"
        assert "2025-01-01 12:00:00" in result

    def test_build_swing_silent_report(self):
        """build_swing_silent_report 应返回包含 💤 和候选数的字符串。"""
        from accumulation_radar.report import build_swing_silent_report

        info = {
            "candidates_scanned": 280,
            "btc_chg24h": 0.12,
            "timestamp": "2025-01-01 12:00:00",
        }

        result = build_swing_silent_report(info)

        assert result is not None
        assert "💤" in result, "应包含 💤 图标"
        assert "280" in result, "应包含候选数 280"
        assert "BTC" in result

    def test_build_swing_silent_report_defaults(self):
        """空 dict 时 build_swing_silent_report 不应报错。"""
        from accumulation_radar.report import build_swing_silent_report

        result = build_swing_silent_report({})
        assert result is not None
        assert "💤" in result

    def test_fmt_vol_b(self):
        """_fmt_vol(1e9) → '1.0B'。"""
        from accumulation_radar.report import _fmt_vol

        assert _fmt_vol(1e9) == "1.0B"

    def test_fmt_vol_m(self):
        """_fmt_vol(1e6) → '1.0M'。"""
        from accumulation_radar.report import _fmt_vol

        assert _fmt_vol(1e6) == "1.0M"

    def test_fmt_vol_k(self):
        """_fmt_vol(1500) → '2K'（1500 >= 1e3）。"""
        from accumulation_radar.report import _fmt_vol

        assert _fmt_vol(1500) == "2K"

    def test_fmt_vol_raw(self):
        """_fmt_vol(500) → '500'（小于 1e3 直接显示）。"""
        from accumulation_radar.report import _fmt_vol

        assert _fmt_vol(500) == "500"

    def test_build_swing_report_empty(self):
        """surge 和 dump 都为空时返回空字符串。"""
        from accumulation_radar.report import build_swing_report

        result = build_swing_report({"surge": [], "dump": []})
        assert result == "", "空候选应返回空字符串"


# ── 模块 2: swing.py 评分逻辑 ────────────────────────────


class TestSwing:
    """swing.py 评分逻辑测试（无API依赖）。"""

    def test_score_opportunities_none(self):
        """ticker_map 为空时 score_opportunities 返回 None。"""
        from accumulation_radar.swing import score_opportunities

        result = score_opportunities({}, {}, {}, set())
        assert result is None, "空 ticker_map 应返回 None"

    def test_score_candidate_surge(self):
        """_score_candidate 对 surge 币的正确评分。"""
        from accumulation_radar.swing import _score_candidate

        candidate = {
            "sym": "PEPEUSDT",
            "swing": 5.0,
            "vol": 100_000_000,
            "chg24h": 10.0,
            "fr": -0.002,  # 负费率（做多有利）
            "mcap": 1_000_000_000,
            "px": 0.00001234,
            "cg_trending": True,
            "window": "5m",
        }

        result = _score_candidate(candidate, is_surge=True)

        assert result["coin"] == "PEPE"
        assert result["sym"] == "PEPEUSDT"
        assert isinstance(result["score"], float)
        assert result["score"] >= 0
        assert result["swing"] == 5.0
        assert "放量" in result["reasons"] or "CG热度" in result["reasons"]

    def test_score_candidate_dump(self):
        """_score_candidate 对 dump 币的正确评分。"""
        from accumulation_radar.swing import _score_candidate

        candidate = {
            "sym": "DOGEUSDT",
            "swing": -4.5,
            "vol": 50_000_000,
            "chg24h": -8.0,
            "fr": 0.002,  # 正费率（做空有利）
            "mcap": 15_000_000_000,
            "px": 0.12,
            "cg_trending": False,
            "window": "15m",
        }

        result = _score_candidate(candidate, is_surge=False)

        assert result["coin"] == "DOGE"
        assert result["sym"] == "DOGEUSDT"
        assert isinstance(result["score"], float)
        assert result["score"] >= 0
        assert result["swing"] == 4.5  # abs 值

    def test_score_candidate_high_vol_score(self):
        """超高成交量应获得高 vol_score。"""
        from accumulation_radar.swing import _score_candidate

        candidate = {
            "sym": "BTCUSDT",
            "swing": 3.5,
            "vol": 50_000_000_000,  # 超大成交量
            "chg24h": 2.0,
            "fr": -0.0001,
            "mcap": 1_000_000_000_000,
            "px": 60000,
            "cg_trending": False,
            "window": "5m",
        }

        result = _score_candidate(candidate, is_surge=True)

        # 超大成交量应该让 reasons 包含"放量"
        assert "放量" in result["reasons"]

    def test_score_candidate_cg_trending(self):
        """CG trending 标记应出现在 reasons 中。"""
        from accumulation_radar.swing import _score_candidate

        candidate = {
            "sym": "SOLUSDT",
            "swing": 4.0,
            "vol": 10_000_000,
            "chg24h": 5.0,
            "fr": 0.0,
            "mcap": 50_000_000_000,
            "px": 150,
            "cg_trending": True,
            "window": "5m",
        }

        result = _score_candidate(candidate, is_surge=True)

        assert "CG热度" in result["reasons"]

    def test_score_opportunities_with_mock_data(self):
        """用 mock 的 ticker_map 测试 score_opportunities 不会报错（即使后续因无 K 线返回 None）。"""
        from accumulation_radar.swing import score_opportunities

        # 构造一个最小 ticker_map
        ticker_map = {
            "BTCUSDT": {"vol": 1_000_000_000, "chg24h": 0.5, "px": 60000},
        }
        fr_map = {"BTCUSDT": 0.0001}
        mcap_map = {"BTCUSDT": 1_000_000_000_000}
        cg_trending = set()

        # 因为没有真实 K 线数据，score_opportunities 应该返回 None
        # 但不会抛出异常
        result = score_opportunities(ticker_map, fr_map, mcap_map, cg_trending)
        # 无 K 线 -> None, 这是合理的
        assert result is None or isinstance(result, dict)


# ── 模块 3: coinalyze_calibrate.py ───────────────────────


class TestCoinalyzeCalibrate:
    """Coinalyze 校对模块 symbol 解析测试（无API调用）。"""

    def test_parse_cz_sym(self):
        """_parse_cz_sym('BTCUSD_PERP.A') → 'BTC'。"""
        from accumulation_radar.sources.coinalyze_calibrate import _parse_cz_sym

        assert _parse_cz_sym("BTCUSD_PERP.A") == "BTC"
        assert _parse_cz_sym("PEPEUSDT_PERP.A") == "PEPE"
        assert _parse_cz_sym("SOLUSD_PERP.A") == "SOL"

    def test_resolve_cz_symbol_coin_m(self):
        """COIN-M 币种应解析为 USD_PERP。"""
        from accumulation_radar.sources.coinalyze_calibrate import _resolve_cz_symbol

        assert _resolve_cz_symbol("BTC") == "BTCUSD_PERP.A"
        assert _resolve_cz_symbol("ETH") == "ETHUSD_PERP.A"
        assert _resolve_cz_symbol("SOL") == "SOLUSD_PERP.A"

    def test_resolve_cz_symbol_usdt_m(self):
        """非 COIN-M 币种应解析为 USDT_PERP。"""
        from accumulation_radar.sources.coinalyze_calibrate import _resolve_cz_symbol

        assert _resolve_cz_symbol("PEPE") == "PEPEUSDT_PERP.A"
        assert _resolve_cz_symbol("WIF") == "WIFUSDT_PERP.A"
        assert _resolve_cz_symbol("DOGS") == "DOGSUSDT_PERP.A"

    def test_calibrate_prices_no_tickers(self):
        """ticker_map 为空时 calibrate_prices 返回 (空 dict, 空 list)。"""
        from accumulation_radar.sources.coinalyze_calibrate import calibrate_prices

        calib, report = calibrate_prices({})
        assert calib == {}, "校准 map 应为空 dict"
        assert report == [], "偏差报告应为空 list"

    def test_resolve_cz_symbol_case_insensitive(self):
        """_resolve_cz_symbol 应不区分大小写。"""
        from accumulation_radar.sources.coinalyze_calibrate import _resolve_cz_symbol

        assert _resolve_cz_symbol("btc") == "BTCUSD_PERP.A"


# ── 模块 4: market_data.py ────────────────────────────────


class TestMarketData:
    """market_data.py symbol 转换测试（无API调用）。"""

    def test_okx_sym(self):
        """okx_sym('BTCUSDT') → 'BTC-USDT-SWAP'。"""
        from accumulation_radar.sources.market_data import okx_sym

        assert okx_sym("BTCUSDT") == "BTC-USDT-SWAP"
        assert okx_sym("PEPEUSDT") == "PEPE-USDT-SWAP"
        assert okx_sym("ETHUSDT") == "ETH-USDT-SWAP"

    def test_mexc_sym(self):
        """mexc_sym('BTCUSDT') → 'BTC_USDT'。"""
        from accumulation_radar.sources.market_data import mexc_sym

        assert mexc_sym("BTCUSDT") == "BTC_USDT"
        assert mexc_sym("PEPEUSDT") == "PEPE_USDT"
        assert mexc_sym("ETHUSDT") == "ETH_USDT"

    def test_okx_sym_edge_cases(self):
        """okx_sym 的边缘输入处理。"""
        from accumulation_radar.sources.market_data import okx_sym

        # 非 USDT 结尾的情况
        result = okx_sym("BTC-USD")
        assert result == "BTC-USD-USDT-SWAP"

    def test_mexc_sym_no_usdt(self):
        """mexc_sym 对不含 USDT 的输入也能正确处理。"""
        from accumulation_radar.sources.market_data import mexc_sym

        result = mexc_sym("BTC-USD")
        assert "USDT" not in result.replace("BTC-USD", "")
