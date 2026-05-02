import copy
import json
import os
import sys
import time
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.dirname(__file__))

import core.state as state_module
import layers.perception as perception
import core.orchestrator as orchestrator_module
import live.engine.market_analyzer as live_market_analyzer
import live.engine.script_generator as live_script_generator
import live.stream.live_controller as live_controller_module
import main as main_module

from core.capabilities import (
    Capability,
    CapabilityNotFoundError,
    CapabilityRegistry,
    DuplicateCapabilityError,
    PayloadValidationError,
)
from core.safety import PostIntent, SafetyGate
from providers.binance_square import BinanceSquareProvider


class _DummyResponse:
    def __init__(self, status_code=200, payload=None, json_exc=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self._json_exc = json_exc
        self.text = text

    def json(self):
        if self._json_exc:
            raise self._json_exc
        return self._payload


class _FakeSession:
    def __init__(self, response=None, exc=None):
        self.response = response
        self.exc = exc
        self.calls = []

    def post(self, url, headers=None, json=None, timeout=None):
        self.calls.append(
            {
                "url": url,
                "headers": headers,
                "json": json,
                "timeout": timeout,
            }
        )
        if self.exc:
            raise self.exc
        return self.response


def test_capability_registry_register_lookup_and_list_enabled():
    registry = CapabilityRegistry()

    cap = Capability(
        name="binance.square.create_post",
        description="create square post",
        handler=lambda payload: {"ok": True, "payload": payload},
        input_schema={
            "type": "object",
            "required": ["content", "coin"],
            "properties": {
                "content": {"type": "string", "minLength": 1},
                "coin": {"type": "string", "minLength": 1},
            },
        },
        risk_level="write",
        enabled=True,
        provider="binance",
        requires_approval=True,
    )
    registry.register(cap)

    loaded = registry.get("binance.square.create_post")
    assert loaded.name == "binance.square.create_post"
    assert loaded.risk_level == "write"
    assert loaded.enabled is True
    assert loaded.requires_approval is True
    assert [item.name for item in registry.list_enabled()] == ["binance.square.create_post"]


def test_capability_registry_duplicate_and_validation_failures_are_explicit():
    registry = CapabilityRegistry()
    registry.register(
        Capability(
            name="binance.market.scan",
            description="scan",
            handler=lambda payload: payload,
        )
    )

    try:
        registry.register(
            Capability(
                name="binance.market.scan",
                description="scan again",
                handler=lambda payload: payload,
            )
        )
        raise AssertionError("expected duplicate registration to fail")
    except DuplicateCapabilityError:
        pass

    try:
        registry.get("binance.unknown")
        raise AssertionError("expected missing capability lookup to fail")
    except CapabilityNotFoundError:
        pass

    try:
        registry.invoke("binance.market.scan", {"unexpected": True})
        raise AssertionError("expected payload validation to fail")
    except PayloadValidationError:
        pass


def test_capability_registry_invoke_calls_handler_with_valid_payload():
    calls = []
    registry = CapabilityRegistry()
    registry.register(
        Capability(
            name="binance.market.scan",
            description="scan",
            handler=lambda payload: calls.append(payload) or {"success": True},
            input_schema={
                "type": "object",
                "required": ["content"],
                "properties": {
                    "content": {"type": "string", "minLength": 1},
                },
            },
            risk_level="read",
        )
    )

    result = registry.invoke("binance.market.scan", {"content": "hello"})

    assert result == {"success": True}
    assert calls == [{"content": "hello"}]


def test_safety_gate_blocks_banned_and_same_coin_daily_uniqueness_without_mutation():
    today = time.strftime("%Y-%m-%d")
    base_state = {
        "status": "BANNED",
        "daily_count": 0,
        "last_post_time": 0,
        "coin_last_post": {"BTC": time.time() - 3600},
        "coin_last_post_date": {"BTC": today},
    }
    state = copy.deepcopy(base_state)
    gate = SafetyGate()

    decision = gate.evaluate(
        state,
        PostIntent(capability_name="binance.square.create_post", coin="BTCUSDT", content="BTC setup looks interesting")
    )

    assert decision.allowed is False
    assert "BANNED" in decision.reason
    assert state == base_state


def test_safety_gate_normalizes_coin_and_allows_safe_intent_with_audit_metadata():
    state = {
        "status": "idle",
        "daily_count": 0,
        "last_post_time": 0,
        "coin_last_post": {},
        "coin_last_post_date": {},
    }
    gate = SafetyGate(restricted_phrases=["稳赚", "guaranteed profit"])

    blocked = gate.evaluate(
        state,
        PostIntent(capability_name="binance.square.create_post", coin=" btcusdt ", content="这波稳赚")
    )
    assert blocked.allowed is False
    assert "安全" in blocked.reason or "敏感" in blocked.reason

    allowed = gate.evaluate(
        state,
        PostIntent(capability_name="binance.square.create_post", coin=" btcusdt ", content="BTC 这里 63800 一线争夺很关键，你怎么看？")
    )
    assert allowed.allowed is True
    assert allowed.normalized_coin == "BTC"
    assert allowed.risk_level == "write"
    assert "content" in allowed.checks_run
    assert state["daily_count"] == 0


def test_binance_square_provider_dry_run_skips_network():
    session = _FakeSession()
    provider = BinanceSquareProvider(api_key="", dry_run=True, session=session)

    result = provider.create_post("hello $BTC")

    assert result["success"] is True
    assert result["mock"] is True
    assert result["post_id"].startswith("MOCK_")
    assert session.calls == []


def test_binance_square_provider_sends_expected_request_and_maps_success_without_post_id():
    session = _FakeSession(response=_DummyResponse(payload={"code": "000000", "message": "success", "data": {}}))
    provider = BinanceSquareProvider(api_key="secret", dry_run=False, session=session)

    result = provider.create_post("hello $BTC")

    assert session.calls[0]["url"].endswith("/bapi/composite/v1/public/pgc/openApi/content/add")
    assert session.calls[0]["headers"]["X-Square-OpenAPI-Key"] == "secret"
    assert session.calls[0]["headers"]["clienttype"] == "binanceSkill"
    assert session.calls[0]["json"] == {"bodyTextOnly": "hello $BTC"}
    assert result["success"] is True
    assert result["post_id"] == ""
    assert result["url"] is None
    assert "无返回ID" in result["message"]


def test_binance_square_provider_handles_non_json_and_network_errors_without_leaking_api_key():
    non_json = BinanceSquareProvider(
        api_key="super-secret-key",
        dry_run=False,
        session=_FakeSession(response=_DummyResponse(status_code=502, json_exc=ValueError("not json"), text="<html>bad gateway</html>")),
    )
    result = non_json.create_post("hello")
    assert result["success"] is False
    assert result["code"] == "HTTP_502_NON_JSON"
    assert "bad gateway" in result["message"].lower()
    assert "super-secret-key" not in result["message"]

    network = BinanceSquareProvider(
        api_key="super-secret-key",
        dry_run=False,
        session=_FakeSession(exc=RuntimeError("boom timeout")),
    )
    result = network.create_post("hello")
    assert result["success"] is False
    assert result["code"] == "NETWORK_ERROR"
    assert "boom timeout" in result["message"]
    assert "super-secret-key" not in result["message"]


def test_orchestrator_run_once_reloads_state_before_quota_selection():
    original_load_state = orchestrator_module.load_state
    original_run_perception = orchestrator_module.run_perception
    original_get_futures_price = getattr(__import__("utils.price_sync", fromlist=["get_futures_price"]), "get_futures_price")

    states = [
        {
            "status": "idle",
            "daily_count": 0,
            "last_post_time": 0,
            "coin_last_post": {},
            "coin_last_post_date": {},
            "today": time.strftime("%Y-%m-%d"),
        },
        {
            "status": "idle",
            "daily_count": 0,
            "last_post_time": 0,
            "coin_last_post": {"BTC": time.time() - 7200},
            "coin_last_post_date": {"BTC": time.strftime("%Y-%m-%d")},
            "today": time.strftime("%Y-%m-%d"),
        },
    ]

    def fake_load_state():
        if states:
            return states.pop(0)
        return {
            "status": "idle",
            "daily_count": 0,
            "last_post_time": 0,
            "coin_last_post": {"BTC": time.time() - 7200},
            "coin_last_post_date": {"BTC": time.strftime("%Y-%m-%d")},
            "today": time.strftime("%Y-%m-%d"),
        }

    original_execute_post = orchestrator_module.execute_post
    try:
        orchestrator_module.load_state = fake_load_state
        orchestrator_module.run_perception = lambda state: {"resonance": [{"coin": "BTC", "tier": "S"}], "raw_tweets": [], "hot_posts": [], "topics": [], "w2e_top_creators": {}}

        import utils.price_sync as price_sync_module
        price_sync_module.get_futures_price = lambda coin: None

        orch = orchestrator_module.Orchestrator()
        orch.generator = type("Generator", (), {"generate": lambda self, coin_info, context: "content"})()
        orch.poster = object()

        executed = {"called": False}

        def fake_execute_post(coin_info, content, state, quota, poster):
            executed["called"] = True
            return {"success": True}

        orchestrator_module.execute_post = fake_execute_post
        result = orch.run_once()

        assert result is False
        assert executed["called"] is False
        assert orch.state.get("coin_last_post_date", {}).get("BTC") == time.strftime("%Y-%m-%d")
    finally:
        orchestrator_module.load_state = original_load_state
        orchestrator_module.run_perception = original_run_perception
        import utils.price_sync as price_sync_module
        price_sync_module.get_futures_price = original_get_futures_price
        orchestrator_module.execute_post = original_execute_post


def test_orchestrator_run_once_uses_patchable_price_sync_and_propagates_live_metadata():
    today = time.strftime("%Y-%m-%d")
    state = {
        "status": "idle",
        "daily_count": 0,
        "last_post_time": 0,
        "coin_last_post": {},
        "coin_last_post_date": {},
        "today": today,
    }
    market = {
        "resonance": [{"coin": "BTC", "tier": "S", "source": "resonance"}],
        "raw_tweets": [],
        "hot_posts": [],
        "topics": [],
        "w2e_top_creators": {},
    }
    price_payload = {
        "price": 65000.0,
        "change_24h": 2.5,
        "high_24h": 66000.0,
        "low_24h": 64000.0,
        "ts": time.time(),
        "is_live": True,
    }
    captured = {}

    def fake_execute_post(coin_info, content, state, quota, poster):
        captured["coin_info"] = copy.deepcopy(coin_info)
        return {"success": True}

    with patch.object(orchestrator_module, "load_state", return_value=copy.deepcopy(state)), \
         patch.object(orchestrator_module, "run_perception", return_value=copy.deepcopy(market)), \
         patch.object(orchestrator_module.price_sync, "get_futures_price", return_value=price_payload) as mocked_price, \
         patch.object(orchestrator_module, "execute_post", side_effect=fake_execute_post):
        orch = orchestrator_module.Orchestrator()
        orch.generator = type("Generator", (), {"generate": lambda self, coin_info, context: "content"})()
        orch.poster = object()

        assert orch.run_once() is True

    mocked_price.assert_called_once_with("BTC")
    coin_info = captured["coin_info"]
    assert coin_info["mark_px"] == 65000.0
    assert coin_info["change_24h"] == 2.5
    assert coin_info["high_24h"] == 66000.0
    assert coin_info["low_24h"] == 64000.0
    assert coin_info["_price_synced"] is True
    assert coin_info["_price_source"] == "binance_futures"
    assert coin_info["source"] == "binance_futures"
    assert coin_info["_price_ts"] == price_payload["ts"]
    assert coin_info["is_live"] is True
    assert "warning_reason" not in coin_info


def test_orchestrator_run_once_marks_missing_futures_price_warning_metadata():
    today = time.strftime("%Y-%m-%d")
    state = {
        "status": "idle",
        "daily_count": 0,
        "last_post_time": 0,
        "coin_last_post": {},
        "coin_last_post_date": {},
        "today": today,
    }
    market = {
        "resonance": [{"coin": "NOPE", "tier": "B"}],
        "raw_tweets": [],
        "hot_posts": [],
        "topics": [],
        "w2e_top_creators": {},
    }
    captured = {}

    def fake_execute_post(coin_info, content, state, quota, poster):
        captured["coin_info"] = copy.deepcopy(coin_info)
        return {"success": True}

    with patch.object(orchestrator_module, "load_state", return_value=copy.deepcopy(state)), \
         patch.object(orchestrator_module, "run_perception", return_value=copy.deepcopy(market)), \
         patch.object(orchestrator_module.price_sync, "get_futures_price", return_value=None), \
         patch.object(orchestrator_module, "execute_post", side_effect=fake_execute_post):
        orch = orchestrator_module.Orchestrator()
        orch.generator = type("Generator", (), {"generate": lambda self, coin_info, context: "content"})()
        orch.poster = object()

        assert orch.run_once() is True

    coin_info = captured["coin_info"]
    assert coin_info["_price_synced"] is False
    assert coin_info["_price_source"] == "binance_futures"
    assert coin_info["source"] == "binance_futures"
    assert coin_info["_price_ts"] is None
    assert coin_info["is_live"] is False
    assert coin_info.get("warning_reason") == "no_binance_price"


def test_orchestrator_start_stop_preserve_banned_status_via_atomic_updates():
    today = time.strftime("%Y-%m-%d")
    banned_state = {
        "status": "BANNED",
        "daily_count": 0,
        "last_post_time": 0,
        "coin_last_post": {},
        "coin_last_post_date": {},
        "today": today,
    }

    with patch.object(orchestrator_module, "load_state", return_value=copy.deepcopy(banned_state)), \
         patch.object(orchestrator_module, "update_state", side_effect=lambda mutator: mutator(copy.deepcopy(banned_state))), \
         patch.object(orchestrator_module.Orchestrator, "_self_check", return_value=True), \
         patch.object(orchestrator_module.time, "sleep", side_effect=lambda _seconds: None):
        orch = orchestrator_module.Orchestrator()
        orch.run_once = lambda: setattr(orch, "_running", False) or False
        orch.start()
        assert orch.state["status"] == "BANNED"

    with patch.object(orchestrator_module, "load_state", return_value=copy.deepcopy(banned_state)), \
         patch.object(orchestrator_module, "update_state", side_effect=lambda mutator: mutator(copy.deepcopy(banned_state))):
        orch = orchestrator_module.Orchestrator()
        orch.stop()
        assert orch.state["status"] == "BANNED"


def test_load_binance_skill_context_fetches_real_readonly_skill_data_and_normalizes():
    original_get = perception.requests.get
    original_post = perception.requests.post

    def fake_get(url, headers=None, params=None, timeout=None):
        if url.endswith("/wallet/market/token/pulse/social/hype/rank/leaderboard/ai"):
            return _DummyResponse(payload={
                "code": "000000",
                "data": {
                    "leaderBoardList": [
                        {
                            "metaInfo": {
                                "symbol": "BTC",
                                "chainId": "56",
                                "contractAddress": "0xbtc",
                            },
                            "marketInfo": {
                                "marketCap": 1000000,
                                "priceChange": 3.5,
                            },
                            "socialHypeInfo": {
                                "socialHype": 88,
                                "sentiment": "Positive",
                                "socialSummaryBriefTranslated": "BTC social buzz rising",
                            },
                        }
                    ]
                },
            })
        if url.endswith("/wallet/market/token/search/ai"):
            keyword = (params or {}).get("keyword")
            return _DummyResponse(payload={
                "code": "000000",
                "data": [
                    {
                        "symbol": keyword,
                        "name": f"{keyword} Token",
                        "chainId": "56",
                        "contractAddress": f"0x{keyword.lower()}",
                        "price": "1.23",
                        "percentChange24h": "2.5",
                        "volume24h": "1000",
                        "marketCap": "5000",
                        "liquidity": "800",
                        "holdersTop10Percent": "12.5",
                        "links": [{"label": "x", "link": "https://x.com/example"}],
                    }
                ],
            })
        raise AssertionError(f"unexpected GET url: {url}")

    def fake_post(url, headers=None, json=None, timeout=None):
        if url.endswith("/wallet/web/signal/smart-money/ai"):
            return _DummyResponse(payload={
                "code": "000000",
                "data": [
                    {
                        "ticker": "ETH",
                        "chainId": "CT_501",
                        "contractAddress": "eth-pump",
                        "direction": "buy",
                        "smartMoneyCount": 5,
                        "signalCount": 9,
                        "status": "active",
                        "maxGain": "12.5",
                        "exitRate": 33,
                    }
                ],
            })
        if url.endswith("/security/token/audit"):
            assert json is not None and json.get("requestId")
            return _DummyResponse(payload={
                "code": "000000",
                "data": {
                    "hasResult": True,
                    "isSupported": True,
                    "riskLevelEnum": "LOW",
                    "riskLevel": 1,
                    "extraInfo": {"buyTax": "0", "sellTax": "0", "isVerified": True},
                    "riskItems": [],
                },
            })
        raise AssertionError(f"unexpected POST url: {url}")

    try:
        perception.requests.get = fake_get
        perception.requests.post = fake_post

        ctx = perception._load_binance_skill_context({"status": "idle"})

        assert ctx["enabled"] is True
        assert ctx["rankings"][0]["symbol"] == "BTC"
        assert ctx["rankings"][0]["chain_id"] == "56"
        assert ctx["smart_money_signals"][0]["symbol"] == "ETH"
        assert ctx["smart_money_signals"][0]["direction"] == "BUY"
        assert ctx["token_info"]["BTC"]["contract_address"] == "0xbtc"
        assert ctx["token_info"]["ETH"]["contract_address"] == "0xeth"
        assert ctx["safety"]["BTC"]["status"] == "PASS"
        assert ctx["safety"]["ETH"]["status"] == "PASS"
        assert "error" not in ctx
    finally:
        perception.requests.get = original_get
        perception.requests.post = original_post


def test_load_binance_skill_context_degrades_on_partial_skill_failures():
    original_get = perception.requests.get
    original_post = perception.requests.post

    def fake_get(url, headers=None, params=None, timeout=None):
        if url.endswith("/wallet/market/token/pulse/social/hype/rank/leaderboard/ai"):
            return _DummyResponse(payload={
                "code": "000000",
                "data": {
                    "leaderBoardList": [
                        {
                            "metaInfo": {
                                "symbol": "SOL",
                                "chainId": "56",
                                "contractAddress": "0xsol",
                            },
                            "marketInfo": {"marketCap": 999, "priceChange": 1.2},
                            "socialHypeInfo": {"socialHype": 22, "sentiment": "Positive"},
                        }
                    ]
                },
            })
        if url.endswith("/wallet/market/token/search/ai"):
            return _DummyResponse(payload={"code": "000000", "data": []})
        raise AssertionError(f"unexpected GET url: {url}")

    def fake_post(url, headers=None, json=None, timeout=None):
        if url.endswith("/wallet/web/signal/smart-money/ai"):
            raise RuntimeError("signal backend timeout")
        if url.endswith("/security/token/audit"):
            raise AssertionError("audit should not run without token search matches")
        raise AssertionError(f"unexpected POST url: {url}")

    try:
        perception.requests.get = fake_get
        perception.requests.post = fake_post

        ctx = perception._load_binance_skill_context({"status": "idle"})

        assert ctx["enabled"] is True
        assert ctx["rankings"][0]["symbol"] == "SOL"
        assert ctx["smart_money_signals"] == []
        assert ctx["token_info"] == {}
        assert ctx["safety"] == {}
        assert "signal backend timeout" in ctx.get("error", "")
    finally:
        perception.requests.get = original_get
        perception.requests.post = original_post


def test_evaluate_audit_status_blocks_high_risk_and_high_tax_tokens():
    blocked = perception._evaluate_audit_status(
        {
            "hasResult": True,
            "isSupported": True,
            "riskLevelEnum": "HIGH",
            "riskLevel": 4,
            "extraInfo": {"buyTax": "12", "sellTax": "0", "isVerified": False},
            "riskItems": [
                {
                    "id": "CONTRACT_RISK",
                    "details": [
                        {"title": "Honeypot Risk Found", "isHit": True, "riskType": "RISK"}
                    ],
                }
            ],
        }
    )

    assert blocked["status"] == "BLOCK"
    assert blocked["buy_tax"] == 12.0
    assert blocked["risk_items"][0]["title"] == "Honeypot Risk Found"



def test_evaluate_audit_status_warns_when_audit_is_unavailable_or_cautionary():
    unavailable = perception._evaluate_audit_status({"hasResult": False, "isSupported": True})
    assert unavailable["status"] == "WARN_MANUAL_REVIEW"
    assert unavailable["risk_level_enum"] == "UNAVAILABLE"

    caution = perception._evaluate_audit_status(
        {
            "hasResult": True,
            "isSupported": True,
            "riskLevelEnum": "LOW",
            "riskLevel": 1,
            "extraInfo": {"buyTax": "5", "sellTax": "4", "isVerified": True},
            "riskItems": [
                {
                    "id": "TRADE_RISK",
                    "details": [
                        {"title": "Transfer Pause Capability", "isHit": True, "riskType": "CAUTION"}
                    ],
                }
            ],
        }
    )

    assert caution["status"] == "WARN_MANUAL_REVIEW"
    assert caution["sell_tax"] == 4.0
    assert caution["risk_items"][0]["risk_type"] == "CAUTION"



def test_evaluate_audit_status_normalizes_string_risk_levels_and_safe_int_inputs():
    blocked = perception._evaluate_audit_status(
        {
            "hasResult": True,
            "isSupported": True,
            "riskLevelEnum": "CRITICAL",
            "riskLevel": "5",
            "extraInfo": {"buyTax": "0", "sellTax": "0", "isVerified": False},
            "riskItems": [],
        }
    )

    assert blocked["status"] == "BLOCK"
    assert blocked["risk_level"] == 5



def test_fetch_token_info_rejects_ambiguous_or_mismatched_symbol_results():
    original_get = perception.requests.get

    def fake_get(url, headers=None, params=None, timeout=None):
        assert url.endswith("/wallet/market/token/search/ai")
        return _DummyResponse(
            payload={
                "code": "000000",
                "data": [
                    {
                        "symbol": "NOTBTC",
                        "name": "Wrong 1",
                        "chainId": "56",
                        "contractAddress": "0xwrong1",
                        "volume24h": "9999",
                    },
                    {
                        "symbol": "BTC",
                        "name": "Real BTC",
                        "chainId": "8453",
                        "contractAddress": "0xbtc-base",
                        "volume24h": "8888",
                    },
                    {
                        "symbol": "BTC",
                        "name": "Another BTC",
                        "chainId": "56",
                        "contractAddress": "0xbtc-bsc",
                        "volume24h": "7777",
                    },
                ],
            }
        )

    try:
        perception.requests.get = fake_get
        token = perception._fetch_token_info("BTC")
        assert token == {}
    finally:
        perception.requests.get = original_get



def test_binance_skill_get_and_post_reject_http_and_business_failures():
    original_get = perception.requests.get
    original_post = perception.requests.post

    try:
        perception.requests.get = lambda *args, **kwargs: _DummyResponse(status_code=503, payload={"code": "000000"})
        try:
            perception._binance_skill_get("https://example.com", params={})
            raise AssertionError("expected HTTP failure to raise")
        except Exception as exc:
            assert "503" in str(exc)

        perception.requests.post = lambda *args, **kwargs: _DummyResponse(status_code=200, payload={"code": "E10001", "message": "rate limited"})
        try:
            perception._binance_skill_post("https://example.com", payload={})
            raise AssertionError("expected business failure to raise")
        except Exception as exc:
            assert "E10001" in str(exc)
    finally:
        perception.requests.get = original_get
        perception.requests.post = original_post



def test_load_binance_skill_context_degrades_on_token_search_business_failure():
    original_get = perception.requests.get
    original_post = perception.requests.post

    def fake_get(url, headers=None, params=None, timeout=None):
        if url.endswith("/wallet/market/token/pulse/social/hype/rank/leaderboard/ai"):
            return _DummyResponse(payload={
                "code": "000000",
                "data": {"leaderBoardList": [{
                    "metaInfo": {"symbol": "BTC", "chainId": "56", "contractAddress": "0xbtc"},
                    "marketInfo": {"marketCap": 1000000, "priceChange": 1.0},
                    "socialHypeInfo": {"socialHype": 90, "sentiment": "Positive"},
                }]},
            })
        if url.endswith("/wallet/market/token/search/ai"):
            return _DummyResponse(payload={"code": "E_SEARCH", "message": "downstream unavailable"})
        raise AssertionError(f"unexpected GET url: {url}")

    def fake_post(url, headers=None, json=None, timeout=None):
        if url.endswith("/wallet/web/signal/smart-money/ai"):
            return _DummyResponse(payload={"code": "000000", "data": []})
        raise AssertionError(f"unexpected POST url: {url}")

    try:
        perception.requests.get = fake_get
        perception.requests.post = fake_post
        ctx = perception._load_binance_skill_context({"status": "idle"})
        assert ctx["rankings"][0]["symbol"] == "BTC"
        assert ctx["token_info"] == {}
        assert "E_SEARCH" in ctx.get("error", "")
    finally:
        perception.requests.get = original_get
        perception.requests.post = original_post


def test_run_perception_includes_binance_skill_context_and_persists_it(tmp_path):
    original_market_file = perception.MARKET_FILE
    original_data_dir = perception.DATA_DIR
    original_twitter_scanner = perception.TwitterScanner
    original_square_scanner = perception.SquareScanner
    original_refresh = perception._maybe_refresh_w2e_leaderboard
    original_w2e = perception._load_w2e_data
    original_smart_money = perception._load_smart_money_signals
    original_skill_context_loader = getattr(perception, "_load_binance_skill_context", None)

    class _FakeTwitterScanner:
        def __init__(self, state):
            self.state = state

        def scan(self):
            return {
                "coin_scores": {"BTC": 5.0},
                "raw_tweets": [{"text": "BTC momentum", "likes": 10, "retweets": 1}],
            }

    class _FakeSquareScanner:
        def scan(self):
            return {
                "coin_scores": {"BTC": 6.0},
                "hot_posts": [{"title": "BTC heat", "views": 2000, "likes": 100}],
                "hype_items": [{"symbol": "BTC", "rank": 1}],
                "topics": [{"topic": "BTC", "score": 99}],
            }

    try:
        perception.MARKET_FILE = tmp_path / "market_context.json"
        perception.DATA_DIR = tmp_path
        perception.TwitterScanner = _FakeTwitterScanner
        perception.SquareScanner = _FakeSquareScanner
        perception._maybe_refresh_w2e_leaderboard = lambda state: None
        perception._load_w2e_data = lambda: {"status": "ok", "top_creators": []}
        perception._load_smart_money_signals = lambda state: {"status": "ok", "top_signals": []}
        perception._load_binance_skill_context = lambda state: {
            "enabled": True,
            "rankings": [{"symbol": "BTC"}],
            "smart_money_signals": [{"symbol": "BTC", "bias": "LONG"}],
            "token_info": {"BTC": {"symbol": "BTC"}},
            "safety": {"BTC": {"status": "PASS"}},
        }

        state = {"status": "idle"}
        ctx = perception.run_perception(state)

        skill_ctx = ctx.get("binance_skill_context")
        assert skill_ctx is not None
        assert skill_ctx["enabled"] is True
        assert skill_ctx["rankings"][0]["symbol"] == "BTC"
        assert skill_ctx["safety"]["BTC"]["status"] == "PASS"
        assert perception.MARKET_FILE.exists()
        persisted = perception.load_market_context()
        assert persisted["binance_skill_context"]["enabled"] is True
    finally:
        perception.MARKET_FILE = original_market_file
        perception.DATA_DIR = original_data_dir
        perception.TwitterScanner = original_twitter_scanner
        perception.SquareScanner = original_square_scanner
        perception._maybe_refresh_w2e_leaderboard = original_refresh
        perception._load_w2e_data = original_w2e
        perception._load_smart_money_signals = original_smart_money
        if original_skill_context_loader is not None:
            perception._load_binance_skill_context = original_skill_context_loader
        elif hasattr(perception, "_load_binance_skill_context"):
            delattr(perception, "_load_binance_skill_context")


def test_run_perception_degrades_gracefully_when_binance_skill_context_fails(tmp_path):
    original_market_file = perception.MARKET_FILE
    original_data_dir = perception.DATA_DIR
    original_twitter_scanner = perception.TwitterScanner
    original_square_scanner = perception.SquareScanner
    original_refresh = perception._maybe_refresh_w2e_leaderboard
    original_w2e = perception._load_w2e_data
    original_smart_money = perception._load_smart_money_signals
    original_skill_context_loader = getattr(perception, "_load_binance_skill_context", None)

    class _FakeTwitterScanner:
        def __init__(self, state):
            self.state = state

        def scan(self):
            return {
                "coin_scores": {"ETH": 3.0},
                "raw_tweets": [{"text": "ETH setup", "likes": 5, "retweets": 1}],
            }

    class _FakeSquareScanner:
        def scan(self):
            return {
                "coin_scores": {"ETH": 4.0},
                "hot_posts": [],
                "hype_items": [],
                "topics": [],
            }

    try:
        perception.MARKET_FILE = tmp_path / "market_context.json"
        perception.DATA_DIR = tmp_path
        perception.TwitterScanner = _FakeTwitterScanner
        perception.SquareScanner = _FakeSquareScanner
        perception._maybe_refresh_w2e_leaderboard = lambda state: None
        perception._load_w2e_data = lambda: {"status": "ok", "top_creators": []}
        perception._load_smart_money_signals = lambda state: {"status": "ok", "top_signals": []}

        def _boom(state):
            raise RuntimeError("skills backend unavailable")

        perception._load_binance_skill_context = _boom

        ctx = perception.run_perception({"status": "idle"})

        skill_ctx = ctx.get("binance_skill_context")
        assert skill_ctx is not None
        assert skill_ctx["enabled"] is False
        assert skill_ctx["rankings"] == []
        assert skill_ctx["smart_money_signals"] == []
        assert skill_ctx["token_info"] == {}
        assert skill_ctx["safety"] == {}
        assert "unavailable" in skill_ctx.get("error", "")
        persisted = perception.load_market_context()
        assert persisted["binance_skill_context"]["enabled"] is False
        assert "unavailable" in persisted["binance_skill_context"].get("error", "")
        assert ctx.get("resonance"), "主链路不应因 skills 失败而中断"
    finally:
        perception.MARKET_FILE = original_market_file
        perception.DATA_DIR = original_data_dir
        perception.TwitterScanner = original_twitter_scanner
        perception.SquareScanner = original_square_scanner
        perception._maybe_refresh_w2e_leaderboard = original_refresh
        perception._load_w2e_data = original_w2e
        perception._load_smart_money_signals = original_smart_money
        if original_skill_context_loader is not None:
            perception._load_binance_skill_context = original_skill_context_loader
        elif hasattr(perception, "_load_binance_skill_context"):
            delattr(perception, "_load_binance_skill_context")


def test_live_script_generator_writes_repo_local_artifact_with_generated_at(tmp_path):
    original_file = live_script_generator.LIVE_SCRIPT_FILE
    original_dir = live_script_generator.DATA_DIR

    market_report = {
        "overview": {"btc_price": 100000, "market_sentiment": "贪婪", "market_trend": "强势上涨"},
        "major_coins": [],
        "trending": [],
    }

    try:
        live_script_generator.DATA_DIR = tmp_path
        live_script_generator.LIVE_SCRIPT_FILE = tmp_path / "live_script.json"
        scripts = live_script_generator.generate_full_live_script(market_report, cart_items=[])

        saved = json.loads(live_script_generator.LIVE_SCRIPT_FILE.read_text(encoding="utf-8"))
        assert saved["generated_at"]
        assert scripts["generated_at"] == saved["generated_at"]
        assert saved["opening"]
    finally:
        live_script_generator.LIVE_SCRIPT_FILE = original_file
        live_script_generator.DATA_DIR = original_dir


def test_live_market_analyzer_cached_report_uses_repo_local_artifact(tmp_path):
    original_file = live_market_analyzer.LIVE_MARKET_REPORT_FILE
    original_dir = live_market_analyzer.DATA_DIR

    payload = {"generated_at": "2026-04-26T13:30:00", "overview": {"btc_price": 99999}}

    try:
        live_market_analyzer.DATA_DIR = tmp_path
        live_market_analyzer.LIVE_MARKET_REPORT_FILE = tmp_path / "live_market_report.json"
        live_market_analyzer.LIVE_MARKET_REPORT_FILE.write_text(json.dumps(payload), encoding="utf-8")

        loaded = live_market_analyzer.load_cached_report()
        assert loaded["overview"]["btc_price"] == 99999
        assert loaded["generated_at"] == "2026-04-26T13:30:00"
    finally:
        live_market_analyzer.LIVE_MARKET_REPORT_FILE = original_file
        live_market_analyzer.DATA_DIR = original_dir


def test_live_market_analyzer_falls_back_to_hyperliquid_when_binance_unavailable(tmp_path, monkeypatch):
    original_file = live_market_analyzer.LIVE_MARKET_REPORT_FILE
    original_dir = live_market_analyzer.DATA_DIR
    hl_market = {
        "BTC": {"price": 95000.0, "change_pct": 2.5, "volume_usdt": 12_000_000_000.0, "high": 96000.0, "low": 93000.0},
        "ETH": {"price": 3200.0, "change_pct": 1.2, "volume_usdt": 5_000_000_000.0, "high": 3250.0, "low": 3100.0},
        "BNB": {"price": 650.0, "change_pct": 0.8, "volume_usdt": 800_000_000.0, "high": 660.0, "low": 630.0},
        "SOL": {"price": 180.0, "change_pct": 4.1, "volume_usdt": 900_000_000.0, "high": 185.0, "low": 170.0},
        "XRP": {"price": 0.62, "change_pct": -0.4, "volume_usdt": 400_000_000.0, "high": 0.64, "low": 0.60},
        "OP": {"price": 2.4, "change_pct": 8.0, "volume_usdt": 180_000_000.0, "high": 2.5, "low": 2.1},
        "ARB": {"price": 1.8, "change_pct": 5.5, "volume_usdt": 150_000_000.0, "high": 1.9, "low": 1.6},
        "W": {"price": 0.52, "change_pct": 9.0, "volume_usdt": 140_000_000.0, "high": 0.55, "low": 0.45},
        "NOT": {"price": 0.012, "change_pct": 7.5, "volume_usdt": 130_000_000.0, "high": 0.013, "low": 0.011},
        "PEPE": {"price": 0.000012, "change_pct": 6.0, "volume_usdt": 220_000_000.0, "high": 0.000013, "low": 0.000011},
    }

    try:
        live_market_analyzer.DATA_DIR = tmp_path
        live_market_analyzer.LIVE_MARKET_REPORT_FILE = tmp_path / "live_market_report.json"
        monkeypatch.setattr(live_market_analyzer, "get_ticker_24h", lambda symbol: None)
        monkeypatch.setattr(live_market_analyzer, "get_hyperliquid_market_snapshot", lambda: hl_market)

        report = live_market_analyzer.get_full_market_report()

        assert report["overview"]["status"] == "ok"
        assert report["overview"]["data_source"] == "hyperliquid"
        assert report["overview"]["btc_price"] == 95000.0
        assert len(report["major_coins"]) >= 5
        assert report["trending"]
        assert report["trending"][0]["symbol"] == "W"
    finally:
        live_market_analyzer.LIVE_MARKET_REPORT_FILE = original_file
        live_market_analyzer.DATA_DIR = original_dir



def test_live_market_analyzer_keeps_last_good_cache_when_refresh_is_unhealthy(tmp_path, monkeypatch):
    original_file = live_market_analyzer.LIVE_MARKET_REPORT_FILE
    original_dir = live_market_analyzer.DATA_DIR
    cached = {
        "generated_at": "2026-04-26T13:30:00",
        "overview": {"status": "ok", "btc_price": 88888, "data_source": "cache"},
        "major_coins": [{"symbol": "BTC", "price": 88888}],
        "trending": [{"symbol": "ETH", "price": 3000}],
    }

    try:
        live_market_analyzer.DATA_DIR = tmp_path
        live_market_analyzer.LIVE_MARKET_REPORT_FILE = tmp_path / "live_market_report.json"
        live_market_analyzer.LIVE_MARKET_REPORT_FILE.write_text(json.dumps(cached), encoding="utf-8")
        monkeypatch.setattr(live_market_analyzer, "get_ticker_24h", lambda symbol: None)
        monkeypatch.setattr(live_market_analyzer, "get_hyperliquid_market_snapshot", lambda: {})

        report = live_market_analyzer.get_full_market_report()

        saved = json.loads(live_market_analyzer.LIVE_MARKET_REPORT_FILE.read_text(encoding="utf-8"))
        assert report["overview"]["btc_price"] == 88888
        assert report["overview"]["data_source"] == "cache"
        assert saved["overview"]["btc_price"] == 88888
    finally:
        live_market_analyzer.LIVE_MARKET_REPORT_FILE = original_file
        live_market_analyzer.DATA_DIR = original_dir


def test_live_controller_import_has_no_legacy_sys_path_injection():
    source = open(live_controller_module.__file__, encoding="utf-8").read()
    assert "/home/ubuntu/clawself_agent" not in source
    assert "sys.path.insert" not in source


def test_get_status_payload_includes_observability_health_fields(tmp_path):
    original_live_script = state_module.LIVE_SCRIPT_FILE
    original_market_report = state_module.LIVE_MARKET_REPORT_FILE
    now = time.time()

    try:
        live_script = tmp_path / "live_script.json"
        market_report = tmp_path / "live_market_report.json"
        live_script.write_text(json.dumps({"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now))}), encoding="utf-8")
        market_report.write_text(json.dumps({"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now - 7200))}), encoding="utf-8")
        state_module.LIVE_SCRIPT_FILE = live_script
        state_module.LIVE_MARKET_REPORT_FILE = market_report

        state = {
            "status": "BANNED",
            "today": time.strftime("%Y-%m-%d"),
            "daily_count": 5,
            "total_posts": 42,
            "last_post_time": now - 180,
            "guest_token": "masked-token",
            "guest_token_time": now - 60,
            "posting_intent": {"coin": "BTC", "source": "w2e", "created_at": now - 30},
            "recent_post_keys": {"k1": now, "k2": now - 1},
            "coin_last_post": {"BTC": now - 1200, "ETH": now - 2400},
        }

        payload = state_module.get_status_payload(state)

        assert payload["is_banned"] is True
        assert payload["remaining_today"] == max(0, state_module.DAILY_LIMIT - 5)
        assert payload["has_guest_token"] is True
        assert payload["has_posting_intent"] is True
        assert payload["posting_intent"]["coin"] == "BTC"
        assert payload["recent_post_key_count"] == 2
        assert payload["coin_cooldown_count"] == 2
        assert payload["guest_token_age_text"] != "无"
        assert payload["posting_intent_age_text"] != "无"
        assert payload["live_script"]["exists"] is True
        assert payload["live_script"]["fresh"] is True
        assert payload["live_script"]["age_text"] != "无"
        assert payload["live_market_report"]["exists"] is True
        assert payload["live_market_report"]["fresh"] is False
        assert payload["live_market_report"]["age_seconds"] is not None
        assert payload["runtime_artifact_freshness_seconds"] > 0
    finally:
        state_module.LIVE_SCRIPT_FILE = original_live_script
        state_module.LIVE_MARKET_REPORT_FILE = original_market_report


def test_get_status_payload_handles_missing_and_invalid_runtime_artifacts(tmp_path):
    original_live_script = state_module.LIVE_SCRIPT_FILE
    original_market_report = state_module.LIVE_MARKET_REPORT_FILE

    try:
        live_script = tmp_path / "missing_live_script.json"
        market_report = tmp_path / "broken_live_market_report.json"
        market_report.write_text("not-json", encoding="utf-8")
        old_ts = time.time() - 7200
        os.utime(market_report, (old_ts, old_ts))
        state_module.LIVE_SCRIPT_FILE = live_script
        state_module.LIVE_MARKET_REPORT_FILE = market_report

        payload = state_module.get_status_payload({"status": "idle", "coin_last_post": {}, "recent_post_keys": {}})

        assert payload["live_script"]["exists"] is False
        assert payload["live_script"]["fresh"] is False
        assert payload["live_script"]["age_seconds"] is None
        assert payload["live_script"]["age_text"] == "无"
        assert payload["live_market_report"]["exists"] is True
        assert payload["live_market_report"]["fresh"] is False
        assert payload["live_market_report"]["age_seconds"] is not None
        assert payload["live_market_report"]["age_seconds"] >= state_module.RUNTIME_ARTIFACT_FRESHNESS_SECONDS
    finally:
        state_module.LIVE_SCRIPT_FILE = original_live_script
        state_module.LIVE_MARKET_REPORT_FILE = original_market_report


def test_orchestrator_status_surfaces_health_and_empty_cooldowns(capsys):
    now = time.time()
    state = {
        "status": "idle",
        "today": time.strftime("%Y-%m-%d"),
        "daily_count": 3,
        "total_posts": 9,
        "last_post_time": now - 90,
        "guest_token": "masked-token",
        "guest_token_time": now - 60,
        "recent_post_keys": {"k1": now},
        "coin_last_post": {},
        "posting_intent": {
            "coin": "BTC",
            "source": "w2e",
            "created_at": now - 15,
            "mock": False,
        },
    }

    payload = {
        "is_banned": False,
        "has_guest_token": True,
        "guest_token_age_text": "1.0m",
        "has_posting_intent": True,
        "posting_intent": state["posting_intent"],
        "posting_intent_age_text": "15s",
        "live_script": {"exists": True, "fresh": True, "timestamp_text": "2026-04-26T12:00:00", "age_text": "30s"},
        "live_market_report": {"exists": True, "fresh": False, "timestamp_text": "2026-04-26T10:00:00", "age_text": "2.0h"},
        "recent_post_key_count": 1,
        "coin_cooldown_count": 0,
        "last_post_age_text": "1.5m",
    }

    with patch.object(orchestrator_module, "load_state", return_value=state), \
         patch.object(orchestrator_module, "get_status_payload", return_value=payload):
        orch = orchestrator_module.Orchestrator()
        orch.status()

    out = capsys.readouterr().out
    assert "[状态]" in out
    assert "今日: 3/72" in out
    assert "运行模式:" in out
    assert "待发布意图: 有" in out
    assert "待发布详情: coin=BTC" in out
    assert "live_script=fresh" in out
    assert "live_market_report=stale" in out
    assert "去重键=1" in out
    assert "无冷却记录" in out



def test_orchestrator_status_json_returns_machine_readable_payload(capsys):
    payload = {
        "status": "running",
        "today": "2026-04-26",
        "daily_count": 4,
        "daily_limit": 72,
        "remaining_today": 68,
        "total_posts": 11,
        "last_post_time": 123.0,
        "last_post_time_text": "2026-04-26 12:00:00",
        "last_post_age_seconds": 33.0,
        "last_post_age_text": "33s",
        "is_banned": False,
        "has_guest_token": True,
        "guest_token_age_seconds": 61.0,
        "guest_token_age_text": "1.0m",
        "has_posting_intent": False,
        "posting_intent": None,
        "posting_intent_age_seconds": None,
        "posting_intent_age_text": "无",
        "recent_post_key_count": 2,
        "coin_cooldown_count": 1,
        "live_script": {
            "exists": True,
            "fresh": True,
            "timestamp_text": "2026-04-26T12:34:56",
            "age_text": "30s",
            "age_seconds": 30.0,
        },
        "live_market_report": {
            "exists": True,
            "fresh": False,
            "timestamp_text": "2026-04-26T10:00:00",
            "age_text": "2.5h",
            "age_seconds": 9000.0,
        },
        "runtime_artifact_freshness_seconds": 1800,
    }

    with patch.object(orchestrator_module, "load_state", return_value={"status": "running"}), \
         patch.object(orchestrator_module, "get_status_payload", return_value=payload):
        orch = orchestrator_module.Orchestrator()
        orch.status_json()

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "running"
    assert data["daily_limit"] == 72
    assert data["live_script"]["fresh"] is True
    assert data["live_market_report"]["fresh"] is False
    assert data["guest_token_age_text"] == "1.0m"



def test_main_status_json_dispatches_to_orchestrator():
    fake_agent = Mock()

    with patch.object(main_module, "Orchestrator", return_value=fake_agent), \
         patch.object(main_module.sys, "argv", ["main.py", "status-json"]):
        main_module.main()

    fake_agent.status_json.assert_called_once_with()
    fake_agent.status.assert_not_called()



def test_main_w2e_dispatches_with_20_minute_interval():
    fake_agent = Mock()

    with patch.object(main_module, "Orchestrator", return_value=fake_agent), \
         patch.object(main_module.sys, "argv", ["main.py", "w2e"]):
        main_module.main()

    fake_agent.start_w2e.assert_called_once_with(interval_minutes=20)



def test_frequency_defaults_are_20_minutes():
    import config.settings as settings_module

    assert settings_module.MIN_INTERVAL_MIN == 20
    assert settings_module.SCAN_INTERVAL_M == 20
    assert settings_module.MAX_JITTER_MIN == 0
