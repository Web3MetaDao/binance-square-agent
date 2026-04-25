import os
import sys

import w2e_post_generator as w2e_module

sys.path.insert(0, os.path.dirname(__file__))

from layers.content import ContentGenerator
from w2e_post_generator import W2EPostGenerator


def test_w2e_normalize_generated_body_removes_template_footer_and_keeps_base_cashtag():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)

    normalized = gen._normalize_generated_body(
        "$XRP 这根突破别装看不见，空头现在压力很大。$XRPUSDT 冲到 2.114\n#XRP #XRPUSDT #币安广场 #内容挖矿 #加密货币\n{future}(XRPUSDT)",
        "XRP",
    )

    assert normalized.startswith("$XRP")
    assert "$XRPUSDT" not in normalized
    assert "#XRPUSDT" not in normalized
    assert "{future}(XRPUSDT)" not in normalized
    assert "#币安广场" not in normalized
    assert "#内容挖矿" not in normalized


def test_w2e_next_cta_is_blank_in_natural_square_mode():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    gen._cta_index = 0

    cta = gen._next_cta("BTC", "BTCUSDT")

    assert cta == ""


def test_w2e_extract_main_coin_normalizes_futures_symbol():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)

    coin = gen._extract_main_coin("Breakout loading for $BTCUSDT if CPI cools")

    assert coin == "BTC"


def test_w2e_extract_main_coin_supports_single_char_symbol_w():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)

    coin = gen._extract_main_coin("$W 这一段插针之后，多空都要重新定价")

    assert coin == "W"


def test_w2e_rewrite_prompt_forbids_marker_and_template_footer():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    reference_post = {"text": "BTC will break out above 100000 soon", "views": 1200, "likes": 56}
    creator = {"nickname": "alpha", "earn_usdc": 88}

    prompt = gen._build_rewrite_prompt(reference_post, creator, "tester persona")

    assert "正文中自然包含 1~3 个 $BTC cashtag" in prompt
    assert "不要把 $BTCUSDT 写成 cashtag" in prompt
    assert "不要输出单独的模板化标签行" in prompt
    assert "不要输出裸露的语法标记（例如 {future}(BTCUSDT)）" in prompt


def test_w2e_rewrite_prompt_requires_natural_futures_semantics():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    reference_post = {"text": "BTC longs are crowded near 100000", "views": 880, "likes": 41}
    creator = {"nickname": "alpha", "earn_usdc": 88}

    prompt = gen._build_rewrite_prompt(reference_post, creator, "tester persona")

    assert "合约" in prompt or "永续" in prompt
    assert "多空" in prompt or "插针" in prompt or "爆仓" in prompt


def test_w2e_run_once_skips_cta_in_final_content(monkeypatch):
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    gen._cta_index = 0
    posted = {}

    class FakePoster:
        @staticmethod
        def post(content):
            posted["content"] = content
            return {"success": False, "code": "", "message": "skip"}

    gen.poster = FakePoster()

    monkeypatch.setattr(w2e_module, "load_state", lambda: {"daily_count": 0})
    monkeypatch.setattr(w2e_module, "save_state", lambda state: None)
    monkeypatch.setattr(w2e_module, "log_post", lambda payload: None)

    class FakeQuota:
        def __init__(self, state):
            self.state = state

        def record_post(self, coin):
            self.recorded = coin

    monkeypatch.setattr(w2e_module, "QuotaController", FakeQuota)

    reference = {
        "creator": {"nickname": "alpha", "earn_usdc": 88},
        "post": {"text": "BTC will break out above 100000 soon"},
    }
    gen._load_w2e_data = lambda: [reference["creator"]]
    gen._select_reference_post = lambda creators: reference
    gen._rewrite_with_llm = lambda selected: ("$BTC 这个位置再拉不上去，空头就要回来了。你怎么看？", "BTC")
    gen._next_cta = lambda coin, futures=None: ""

    result = gen.run_once()

    assert result["success"] is False
    assert posted["content"] == "$BTC 这个位置再拉不上去，空头就要回来了。你怎么看？"


def test_content_generator_normalize_body_removes_template_footer_artifacts():
    gen = ContentGenerator.__new__(ContentGenerator)

    body = gen._normalize_body(
        "$xrpusdt move is violent\n#xrpusdt #币安广场\n{future}(xrpusdt)",
        "XRP",
        "XRPUSDT",
    )

    assert body.startswith("$XRP")
    assert "$XRPUSDT" not in body
    assert "#XRPUSDT" not in body.upper()
    assert "{future}(XRPUSDT)" not in body
    assert "#币安广场" not in body


def test_content_generator_fallback_template_matches_natural_square_style():
    gen = ContentGenerator.__new__(ContentGenerator)

    post = gen._fallback_template({"coin": "BTC", "futures": "BTCUSDT", "tier": "S"}, {"name": "x"})

    assert "$BTC" in post
    assert "$BTCUSDT" not in post
    assert "#BTCUSDT" not in post
    assert "{future}(BTCUSDT)" not in post
    assert "#币安广场" not in post


def test_content_generator_insert_cta_returns_body_when_cta_blank():
    gen = ContentGenerator.__new__(ContentGenerator)

    body = "$BTC 这个位置如果站不稳，回踩概率很高。你怎么看？"
    full = gen._insert_cta_before_footer(body, "")

    assert full == body
