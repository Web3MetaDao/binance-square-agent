import os
import sys
import time
import pathlib
import threading
import multiprocessing
from unittest.mock import patch

import utils.price_sync as price_sync

sys.path.insert(0, os.path.dirname(__file__))

from layers.executor import QuotaController, execute_post
from w2e_post_generator import W2EPostGenerator, FIXED_DISCLAIMER
from layers.content import ContentGenerator


def test_quota_blocks_same_coin_twice_in_same_day_even_if_interval_elapsed():
    now = time.time()
    today = time.strftime("%Y-%m-%d")
    state = {
        "daily_count": 1,
        "last_post_time": now - 7200,
        "coin_last_post": {"BTC": now - 7200},
        "coin_last_post_date": {"BTC": today},
        "today": today,
    }
    quota = QuotaController(state)
    ok, reason = quota.can_post("BTC")
    assert ok is False
    assert "今日已发过" in reason


def test_quota_blocks_banned_account_before_other_limits():
    state = {
        "status": "BANNED",
        "daily_count": 0,
        "last_post_time": 0,
        "coin_last_post": {},
        "coin_last_post_date": {},
        "today": time.strftime("%Y-%m-%d"),
    }
    quota = QuotaController(state)

    ok, reason = quota.can_post("BTC")

    assert ok is False
    assert "BANNED" in reason


def test_quota_allows_same_coin_on_next_day_when_global_interval_elapsed():
    now = time.time()
    yesterday = time.strftime("%Y-%m-%d", time.localtime(now - 86400))
    today = time.strftime("%Y-%m-%d", time.localtime(now))
    state = {
        "daily_count": 1,
        "last_post_time": now - 90000,
        "coin_last_post": {"BTC": now - 90000},
        "coin_last_post_date": {"BTC": yesterday},
        "today": today,
    }
    quota = QuotaController(state)
    ok, reason = quota.can_post("BTC")
    assert ok is True
    assert reason == ""


def test_record_post_tracks_coin_post_date_for_daily_uniqueness():
    today = time.strftime("%Y-%m-%d")
    state = {
        "daily_count": 0,
        "total_posts": 0,
        "last_post_time": 0,
        "coin_last_post": {},
        "coin_last_post_date": {},
        "today": "",
    }
    quota = QuotaController(state)
    quota.record_post("SOLUSDT")
    assert state["coin_last_post_date"]["SOL"] == today
    assert state["coin_last_post"]["SOL"] > 0


def test_fixed_template_post_has_required_sections_and_disclaimer():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    raw = "暴跌后的反抽别追\n$BTC 4小时这段反抽更像空头回补，63800一线如果站不稳，回踩会很快。\n多空都盯着今晚数据，短线先看支撑和量能。\n$BTCUSDT {future}(BTCUSDT) #币安广场 #内容挖矿"
    with patch("w2e_post_generator.W2EPostGenerator._live_price_line", return_value="现在 BTC 期货最新价 $63,800.0000，24h +2.50%，日内区间 $62,000.0000-$64,500.0000。"):
        post = gen._format_fixed_template_post(raw, "BTC")
    lines = [line for line in post.splitlines() if line.strip()]
    assert lines[0] == "暴跌后的反抽别追"
    assert FIXED_DISCLAIMER in post
    assert "{future}(BTCUSDT)" not in post
    assert "$BTCUSDT" not in post
    assert "#币安广场 #内容挖矿" not in post
    assert "💡 点击下方币种标签" not in post
    assert "现在 BTC 期货最新价 $63,800.0000" in post
    cashtag_line = next(line for line in post.splitlines() if "$" in line and "#" not in line and line.startswith("$"))
    cashtags = [token for token in cashtag_line.split() if token.startswith("$")]
    assert cashtags == ["$BTC", "$BSB"]
    assert cashtags[0] == "$BTC"
    hashtag_line = next(line for line in post.splitlines() if line.startswith("#"))
    hashtags = [token for token in hashtag_line.split() if token.startswith("#")]
    assert 2 <= len(hashtags) <= 4


def test_fixed_template_post_replaces_hallucinated_price_with_synced_live_price():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    raw = (
        "BTC 别被假突破骗了\n"
        "$BTC 现在已经冲到 $69,999.0000，今晚直接看新高。\n"
        "这个位置追的人很多，你怎么看？"
    )
    with patch(
        "w2e_post_generator.W2EPostGenerator._live_price_line",
        return_value="现在 BTC 期货最新价 $63,800.0000，24h +2.50%，日内区间 $62,000.0000-$64,500.0000。",
    ):
        post = gen._format_fixed_template_post(raw, "BTC")

    assert "$69,999.0000" not in post
    assert post.count("期货最新价") == 1
    assert "现在 BTC 期货最新价 $63,800.0000" in post


def test_fixed_template_post_replaces_fake_latest_price_line_with_synced_live_price():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    raw = (
        "BTC 这波别急着追\n"
        "现在 BTC 期货最新价 $69,999.0000，24h +99.99%，日内区间 $60,000.0000-$70,000.0000。\n"
        "量能不配合的时候，假突破最容易坑人。"
    )
    with patch(
        "w2e_post_generator.W2EPostGenerator._live_price_line",
        return_value="现在 BTC 期货最新价 $63,800.0000，24h +2.50%，日内区间 $62,000.0000-$64,500.0000。",
    ):
        post = gen._format_fixed_template_post(raw, "BTC")

    assert "$69,999.0000" not in post
    assert post.count("期货最新价") == 1
    assert "现在 BTC 期货最新价 $63,800.0000" in post


def test_content_generator_drops_live_line_when_synced_price_timestamp_is_stale():
    gen = ContentGenerator.__new__(ContentGenerator)
    coin_info = {
        "coin": "OP",
        "futures": "OPUSDT",
        "mark_px": 1.8456,
        "change_24h": 3.21,
        "high_24h": 1.9,
        "low_24h": 1.7,
        "_price_synced": True,
        "_price_ts": time.time() - 601,
    }

    live_line = ContentGenerator._live_price_line(gen, coin_info)

    assert live_line == ""



def test_content_generator_drops_live_line_when_synced_price_timestamp_missing():
    gen = ContentGenerator.__new__(ContentGenerator)
    coin_info = {
        "coin": "OP",
        "futures": "OPUSDT",
        "mark_px": 1.8456,
        "change_24h": 3.21,
        "high_24h": 1.9,
        "low_24h": 1.7,
        "_price_synced": True,
    }

    live_line = ContentGenerator._live_price_line(gen, coin_info)

    assert live_line == ""


def test_content_generator_drops_live_line_when_price_is_unsynced_even_with_recent_timestamp():
    gen = ContentGenerator.__new__(ContentGenerator)
    coin_info = {
        "coin": "OP",
        "futures": "OPUSDT",
        "mark_px": 1.8456,
        "change_24h": 3.21,
        "high_24h": 1.9,
        "low_24h": 1.7,
        "_price_synced": False,
        "_price_ts": time.time(),
        "is_live": False,
    }

    live_line = ContentGenerator._live_price_line(gen, coin_info)

    assert live_line == ""


def test_content_generator_keeps_live_line_for_legacy_coin_info_without_freshness_metadata():
    gen = ContentGenerator.__new__(ContentGenerator)
    coin_info = {
        "coin": "OP",
        "futures": "OPUSDT",
        "mark_px": 1.8456,
        "change_24h": 3.21,
        "high_24h": 1.9,
        "low_24h": 1.7,
    }

    live_line = ContentGenerator._live_price_line(gen, coin_info)

    assert live_line == "现在 OP 期货最新价 $1.8456，24h +3.21%，日内区间 $1.7000-$1.9000。"



def test_w2e_live_price_line_drops_stale_price_payload_from_price_sync():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    stale_payload = {
        "coin": "BTC",
        "symbol": "BTCUSDT",
        "price": 63800.0,
        "change_24h": 2.5,
        "high_24h": 64500.0,
        "low_24h": 62000.0,
        "ts": time.time() - 601,
        "is_live": False,
    }

    with patch.object(price_sync, "get_futures_price", return_value=stale_payload):
        live_line = W2EPostGenerator._live_price_line(gen, "BTC")

    assert live_line == ""



def test_w2e_live_price_line_keeps_fresh_price_payload_from_price_sync():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    fresh_payload = {
        "coin": "BTC",
        "symbol": "BTCUSDT",
        "price": 63800.0,
        "change_24h": 2.5,
        "high_24h": 64500.0,
        "low_24h": 62000.0,
        "ts": time.time(),
        "is_live": True,
    }

    with patch.object(price_sync, "get_futures_price", return_value=fresh_payload):
        live_line = W2EPostGenerator._live_price_line(gen, "BTC")

    assert "现在 BTC 期货最新价 $63,800.0000" in live_line



def test_fixed_template_post_drops_exact_price_claims_when_live_price_unavailable():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    raw = (
        "OP 先别急着追\n"
        "$OP 现价 2.3456 美元，这里再冲就容易被砸。\n"
        "先看量能和承接，你怎么看？"
    )
    with patch("w2e_post_generator.W2EPostGenerator._live_price_line", return_value=""):
        post = gen._format_fixed_template_post(raw, "OP")

    assert "$OP 现价 2.3456 美元" not in post
    assert "2.3456 美元" not in post


def test_fixed_template_post_removes_duplicate_marketing_lines_and_repeated_body_content():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    raw = (
        "ADA 热度高不代表马上拉盘\n"
        "老韭菜先看量能，不抢第一口肉。\n"
        "老韭菜先看量能，不抢第一口肉！\n"
        "💡 点击下方币种标签🏷️ 查看实时行情，广场内交易还能给我贡献一点挖矿收益😁\n"
        "💡 点击下方币种标签🏷️ 查看实时行情，广场内交易还能给我贡献一点挖矿收益😄\n"
        "#ADA #ADAUSDT #币安广场 #合约交易"
    )
    with patch("w2e_post_generator.W2EPostGenerator._live_price_line", return_value=""):
        post = gen._format_fixed_template_post(raw, "ADA")

    assert post.count("点击下方币种标签") == 0
    assert "#ADAUSDT" not in post
    assert "#币安广场" not in post
    assert post.count("老韭菜先看量能，不抢第一口肉") == 1




def test_content_generator_prompt_discourages_fake_ai_tone_and_requires_trade_details():
    gen = ContentGenerator.__new__(ContentGenerator)
    gen._load_persona = lambda: "交易员，偏复盘和计划，不卖课。"
    coin_info = {
        "coin": "BTC",
        "futures": "BTCUSDT",
        "tier": "S",
        "mark_px": 63800.0,
        "change_24h": 2.5,
        "high_24h": 64500.0,
        "low_24h": 62000.0,
        "_price_synced": True,
        "_price_ts": time.time(),
    }
    context = {
        "raw_tweets": [{"text": "BTC discussion around 64k breakout and 4h volume."}],
        "hot_posts": [{"title": "BTC 回踩 63000 一线有没有承接"}],
        "topics": [{"topic": "ETF"}],
        "w2e_top_creators": {"top_creators": []},
    }
    style = {
        "name": "交易计划型",
        "desc": "先写观察，再写计划和失效条件，像真人复盘。",
        "hook_example": "BTC 这里我不追，除非 4H 重新站回关键位。",
    }

    prompt = ContentGenerator._build_prompt(gen, coin_info, context, style)

    assert "少用夸张情绪词和老韭菜人设" in prompt
    assert "优先写具体交易细节：点位、周期、仓位或计划、判断依据、失效条件" in prompt
    assert "不要为了吸睛硬写夸张亏损、暴富、爆仓故事" in prompt
    assert "不要每条都套用“热点+经历+数字+情绪+悬念”同一结构" in prompt


def test_content_generator_prompt_skips_stale_synced_price_block():
    gen = ContentGenerator.__new__(ContentGenerator)
    gen._load_persona = lambda: "交易员，偏复盘和计划，不卖课。"
    coin_info = {
        "coin": "BTC",
        "futures": "BTCUSDT",
        "tier": "S",
        "mark_px": 69999.0,
        "change_24h": 9.9,
        "high_24h": 71000.0,
        "low_24h": 65000.0,
        "_price_synced": True,
        "_price_ts": time.time() - 601,
    }
    context = {
        "raw_tweets": [],
        "hot_posts": [],
        "topics": [],
        "w2e_top_creators": {"top_creators": []},
    }
    style = {
        "name": "交易计划型",
        "desc": "先写观察，再写计划和失效条件，像真人复盘。",
        "hook_example": "BTC 这里我不追，除非 4H 重新站回关键位。",
    }

    prompt = ContentGenerator._build_prompt(gen, coin_info, context, style)

    assert "币安期货实时行情（必须使用这些真实数据，不能编造）" not in prompt
    assert "$69,999.0000" not in prompt


def test_w2e_rewrite_prompt_discourages_template_drama_and_requires_real_structure():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    reference_post = {"text": "$ETH 昨晚冲高后回落，关键还是看 4H 能不能站稳。", "views": 1234, "likes": 12}
    creator = {"nickname": "tester", "earnings_usdc": 88}

    with patch.object(price_sync, "get_futures_price", return_value=None):
        prompt = W2EPostGenerator._build_rewrite_prompt(
            gen,
            reference_post,
            creator,
            "偏交易复盘，少讲情绪，多讲计划。",
            tg_signal=None,
        )

    assert "不要硬凹老韭菜、神预测、逆天收益这类人设" in prompt
    assert "可以写故事，但必须有真实交易细节支撑" in prompt
    assert "优先补足点位、周期、仓位计划、判断依据、失效条件" in prompt
    assert "不要连续复用“强钩子+惨痛经历+大数字+留悬念”套路" in prompt


def test_content_generator_default_persona_is_trader_not_old_retail_cliche():
    gen = ContentGenerator.__new__(ContentGenerator)
    gen._persona_cache = None

    with patch("layers.content.PERSONA_FILE", pathlib.Path("/tmp/nonexistent-persona.md")):
        persona = ContentGenerator._load_persona(gen)

    assert "老韭菜" not in persona
    assert "亏损经历" not in persona
    assert "交易者" in persona or "交易复盘" in persona


def test_w2e_generator_default_persona_is_trader_not_edgy_creator_cliche():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)

    with patch("w2e_post_generator.PERSONA_FILE", pathlib.Path("/tmp/nonexistent-w2e-persona.md")):
        persona = W2EPostGenerator._load_persona(gen)

    assert "老韭菜" not in persona
    assert "犀利" not in persona
    assert "市场分析" in persona or "交易计划" in persona


def test_extract_main_coin_supports_single_char_symbol_w():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    coin = gen._extract_main_coin("$W 这段插针之后，多空都要重新定价")
    assert coin == "W"


def test_extract_main_coin_supports_single_char_w_futures_pair():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    coin = gen._extract_main_coin("今晚重点看 WUSDT 的承接力度")
    assert coin == "W"


def test_extract_main_coin_does_not_treat_plain_w_shape_as_w_coin():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    coin = gen._extract_main_coin("这波更像 W 底，右侧确认前先别追")
    assert coin == "BTC"


def test_extract_main_coin_prefers_real_coin_over_plain_w_shape():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    coin = gen._extract_main_coin("BTC 这里走出 W 底，量能确认再看延续")
    assert coin == "BTC"


def test_extract_main_coin_supports_future_marker():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    coin = gen._extract_main_coin("今晚重点看 {future}(OPUSDT) 的承接力度")
    assert coin == "OP"


def test_fixed_template_post_removes_global_legacy_usdt_noise_for_any_coin():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    raw = (
        "OP 先别急着追\n"
        "BTC 那边 $BTCUSDT 和 {future}(ETHUSDT) 的旧标签还在干扰情绪。\n"
        "如果 #SOLUSDT 继续放量，OP 这里更要看2小时承接。\n"
        "$OPUSDT {future}(OPUSDT) #OPUSDT #币安广场 #内容挖矿"
    )
    with patch("w2e_post_generator.W2EPostGenerator._live_price_line", return_value=""):
        post = gen._format_fixed_template_post(raw, "OP")

    assert "$OPUSDT" not in post
    assert "$BTCUSDT" not in post
    assert "{future}(OPUSDT)" not in post
    assert "{future}(ETHUSDT)" not in post
    assert "#OPUSDT" not in post
    assert "#SOLUSDT" not in post
    assert "$OP" in post
    assert "$BTC" in post

def test_content_generator_formats_single_disclaimer_and_removes_cta_duplicates():
    gen = ContentGenerator.__new__(ContentGenerator)
    coin_info = {"coin": "OP", "futures": "OPUSDT", "mark_px": 1.8456, "change_24h": 3.21, "high_24h": 1.9, "low_24h": 1.7}
    raw = "别急着追高\n$OP 这波拉升核心还是情绪回暖。\n💡 点击下方币种标签🏷️ 查看实时行情，广场内交易还能给我贡献一点挖矿收益😄\n⚠️免责声明：\n本文仅为个人行情观点分享，不构成任何投资建议，加密货币市场高波动、高风险，请理性交易、自行承担风险。\n{future}(OPUSDT) #币安广场 #内容挖矿"
    post = ContentGenerator._format_final_post(gen, raw, coin_info)
    assert post.count("⚠️免责声明：") == 1
    assert post.count("本文仅为个人行情观点分享") == 1
    assert "💡 点击下方币种标签" not in post
    assert "{future}(OPUSDT)" not in post
    assert "$OPUSDT" not in post
    assert "现在 OP 期货最新价 $1.8456" in post


def test_smart_money_prompt_formatter_preserves_single_cta():
    gen = ContentGenerator.__new__(ContentGenerator)
    coin_info = {"coin": "BTC", "futures": "BTCUSDT", "mark_px": 63800.0, "change_24h": 2.5, "high_24h": 64500.0, "low_24h": 62000.0}
    raw = (
        "别只看表面热度\n"
        "$BTC 这波更像主力试盘，追高和摸顶都容易被教育。\n"
        "💡 点击下方币种标签🏷️ 查看实时行情，广场内交易还能给我贡献一点挖矿收益😄\n"
        "⚠️免责声明：\n"
        "本文仅为个人行情观点分享，不构成任何投资建议，加密货币市场高波动、高风险，请理性交易、自行承担风险。"
    )

    post = ContentGenerator._format_final_post(
        gen,
        raw,
        coin_info,
        cta="💡 点击下方币种标签🏷️ 查看实时行情，广场内交易还能给我贡献一点挖矿收益😄",
    )

    assert post.count("点击下方币种标签") == 1
    assert post.splitlines()[-1] == "💡 点击下方币种标签🏷️ 查看实时行情，广场内交易还能给我贡献一点挖矿收益😄"


def test_content_generator_uses_natural_cta_and_precise_cashtags():
    gen = ContentGenerator.__new__(ContentGenerator)
    coin_info = {"coin": "ETH", "futures": "ETHUSDT", "mark_px": None, "change_24h": None, "high_24h": None, "low_24h": None}

    post = ContentGenerator._format_final_post(
        gen,
        "别急着追高\n等回踩确认再说，你怎么看？",
        coin_info,
        cta="💡 点下方币种标签看实时行情，也欢迎留言说说你会追突破还是等回踩。",
    )

    assert "$ETH $BSB" in post
    assert "$BTC" not in post
    assert "$BNB" not in post
    assert "给我贡献一点挖矿收益" not in post
    assert post.splitlines()[-1] == "💡 点下方币种标签看实时行情，也欢迎留言说说你会追突破还是等回踩。"
    assert "#交易复盘 #行情分析 #交易计划" in post


def test_w2e_formatter_keeps_primary_coin_and_bsb_only():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    with patch("w2e_post_generator.W2EPostGenerator._live_price_line", return_value=""):
        post = gen._format_fixed_template_post("ETH 这里别无脑追\n看回踩承接再说，你怎么看？", "ETH")

    assert "$ETH $BSB" in post
    assert "$BTC" not in post
    assert "$BNB" not in post
    assert "#交易复盘 #行情分析 #交易计划" in post


def test_content_generator_replaces_hallucinated_price_with_synced_live_price():
    gen = ContentGenerator.__new__(ContentGenerator)
    coin_info = {"coin": "OP", "futures": "OPUSDT", "mark_px": 1.8456, "change_24h": 3.21, "high_24h": 1.9, "low_24h": 1.7}
    raw = (
        "别急着追高\n"
        "$OP 现价已经来到 $9.9999，这种拉法很难回头。\n"
        "多头情绪太满了，你怎么看？"
    )
    post = ContentGenerator._format_final_post(gen, raw, coin_info)

    assert "$9.9999" not in post
    assert post.count("期货最新价") == 1
    assert "现在 OP 期货最新价 $1.8456" in post


def test_content_generator_replaces_fake_latest_price_line_with_synced_live_price():
    gen = ContentGenerator.__new__(ContentGenerator)
    coin_info = {"coin": "OP", "futures": "OPUSDT", "mark_px": 1.8456, "change_24h": 3.21, "high_24h": 1.9, "low_24h": 1.7}
    raw = (
        "别急着追高\n"
        "现在 OP 期货最新价 $9.9999，24h +88.88%，日内区间 $9.0000-$10.0000。\n"
        "多头情绪太满了，你怎么看？"
    )
    post = ContentGenerator._format_final_post(gen, raw, coin_info)

    assert "$9.9999" not in post
    assert post.count("期货最新价") == 1
    assert "现在 OP 期货最新价 $1.8456" in post


def test_content_generator_drops_exact_price_claims_when_live_price_unavailable():
    gen = ContentGenerator.__new__(ContentGenerator)
    coin_info = {"coin": "SUI", "futures": "SUIUSDT", "mark_px": None, "change_24h": None, "high_24h": None, "low_24h": None}
    raw = (
        "SUI 这位置先别冲\n"
        "$SUI 现价 3.1415 美元，追高容易两头挨打。\n"
        "等回踩确认再说，你怎么看？"
    )
    post = ContentGenerator._format_final_post(gen, raw, coin_info)

    assert "$SUI 现价 3.1415 美元" not in post
    assert "3.1415 美元" not in post


def test_content_generator_drops_hallucinated_exact_price_from_title_when_live_price_unavailable():
    gen = ContentGenerator.__new__(ContentGenerator)
    coin_info = {"coin": "SUI", "futures": "SUIUSDT", "mark_px": None, "change_24h": None, "high_24h": None, "low_24h": None}
    raw = (
        "SUI 期货最新价 $9.9999，别再追了\n"
        "等回踩确认再说，你怎么看？"
    )
    post = ContentGenerator._format_final_post(gen, raw, coin_info)

    assert "$9.9999" not in post
    assert "期货最新价" not in post


def test_w2e_formatter_drops_hallucinated_exact_price_from_title_when_live_price_unavailable():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    raw = (
        "BTC 期货最新价 $999,999.0000，主升浪来了\n"
        "$BTC 这位置别被情绪带着跑。"
    )

    with patch("w2e_post_generator.get_futures_price", return_value=None, create=True):
        post = W2EPostGenerator._format_fixed_template_post(gen, raw, "BTC")

    assert "$999,999.0000" not in post
    assert "期货最新价" not in post


def test_smart_money_fallback_uses_real_synced_price_when_available():
    from layers.content import make_data_post
    price_info = {
        "price": 63800.0,
        "change_24h": 2.5,
        "high_24h": 64500.0,
        "low_24h": 62000.0,
        "volume_24h": 2.5e10,
        "_source": "binance_futures",
        "is_live": True,
    }
    post = make_data_post("BTC", price_info)

    assert "$63,800" in post or "63,800" in post
    assert "▲" in post or "▼" in post
    assert "$BTC" in post
    assert "币安期货" in post


def test_smart_money_fallback_drops_exact_price_claims_when_live_price_unavailable():
    from layers.content import make_analysis_post
    post = make_analysis_post("ETH", tier="S")

    assert "我" in post
    assert "$ETH" in post
    assert "爆热" in post or "热度" in post
    assert "当前价格" not in post
    assert "最新价" not in post


def test_content_generator_removes_duplicate_marketing_lines_and_repeated_body_content():
    gen = ContentGenerator.__new__(ContentGenerator)
    coin_info = {"coin": "ADA", "futures": "ADAUSDT", "mark_px": None, "change_24h": None, "high_24h": None, "low_24h": None}
    raw = (
        "ADA 热度高不代表马上拉盘\n"
        "老韭菜先看量能，不抢第一口肉。\n"
        "老韭菜先看量能，不抢第一口肉！\n"
        "💡 点击下方币种标签🏷️ 查看实时行情，广场内交易还能给我贡献一点挖矿收益😁\n"
        "💡 点击下方币种标签🏷️ 查看实时行情，广场内交易还能给我贡献一点挖矿收益😄\n"
        "⚠️免责声明：\n"
        "本文仅为个人行情观点分享，不构成任何投资建议，加密货币市场高波动、高风险，请理性交易、自行承担风险。\n"
        "#ADA #ADAUSDT #币安广场 #合约交易"
    )
    post = ContentGenerator._format_final_post(gen, raw, coin_info)

    assert post.count("点击下方币种标签") == 0
    assert "#ADAUSDT" not in post
    assert "#币安广场" not in post
    assert post.count("老韭菜先看量能，不抢第一口肉") == 1


def test_content_generator_removes_global_legacy_usdt_noise_for_any_coin():
    gen = ContentGenerator.__new__(ContentGenerator)
    coin_info = {"coin": "OP", "futures": "OPUSDT", "mark_px": None, "change_24h": None, "high_24h": None, "low_24h": None}
    raw = (
        "OP 先别急着追\n"
        "正文混进 $BTCUSDT、{future}(ETHUSDT) 和 #SOLUSDT，都应该清掉。\n"
        "$OPUSDT {future}(OPUSDT) #OPUSDT #币安广场 #内容挖矿"
    )
    post = ContentGenerator._format_final_post(gen, raw, coin_info)

    assert "$OPUSDT" not in post
    assert "$BTCUSDT" not in post
    assert "{future}(OPUSDT)" not in post
    assert "{future}(ETHUSDT)" not in post
    assert "#OPUSDT" not in post
    assert "#SOLUSDT" not in post
    assert "$OP" in post
    assert "$BTC" in post


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


def test_square_poster_returns_url_none_when_success_without_post_id():
    from layers.executor import SquarePoster

    poster = SquarePoster.__new__(SquarePoster)
    poster.mock_mode = False
    payload = {"code": "000000", "message": "success", "data": {}}
    with patch("layers.executor.requests.post", return_value=_DummyResponse(payload=payload)):
        result = poster.post("hello world")

    assert result["success"] is True
    assert result["post_id"] == ""
    assert result["url"] is None
    assert "无返回ID" in result["message"]


def test_square_poster_handles_non_json_response_cleanly():
    from layers.executor import SquarePoster

    poster = SquarePoster.__new__(SquarePoster)
    poster.mock_mode = False
    with patch(
        "layers.executor.requests.post",
        return_value=_DummyResponse(status_code=502, json_exc=ValueError("not json"), text="<html>bad gateway</html>"),
    ):
        result = poster.post("hello world")

    assert result["success"] is False
    assert result["code"] == "HTTP_502_NON_JSON"
    assert "bad gateway" in result["message"].lower()


def test_square_poster_maps_official_daily_limit_error_code():
    from layers.executor import SquarePoster

    poster = SquarePoster.__new__(SquarePoster)
    poster.mock_mode = False
    payload = {"code": "220009", "message": ""}
    with patch("layers.executor.requests.post", return_value=_DummyResponse(payload=payload)):
        result = poster.post("hello world")

    assert result["success"] is False
    assert result["code"] == "220009"
    assert "每日发帖上限" in result["message"]


def test_execute_post_blocks_same_coin_duplicate_under_concurrent_threads():
    state = {
        "daily_count": 0,
        "total_posts": 0,
        "last_post_time": 0,
        "coin_last_post": {},
        "coin_last_post_date": {},
        "today": time.strftime("%Y-%m-%d"),
    }
    coin_infos = [
        {"coin": "BTC", "tier": "S", "futures": "BTCUSDT"},
        {"coin": "BTCUSDT", "tier": "S", "futures": "BTCUSDT"},
    ]

    class Poster:
        def post(self, content):
            time.sleep(0.05)
            return {"success": True, "post_id": "x", "url": None, "mock": True}

    results = []
    errors = []
    start = threading.Barrier(2)

    with patch("layers.executor.save_state", lambda *_args, **_kwargs: None), patch(
        "layers.executor.log_post", lambda *_args, **_kwargs: None
    ), patch("layers.executor.update_state", lambda mutator: mutator(state)):
        def worker(coin_info):
            try:
                quota = QuotaController(state)
                start.wait()
                results.append(execute_post(coin_info, "hello", state, quota, Poster()))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(coin_info,)) for coin_info in coin_infos]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert not errors
    success_count = sum(1 for result in results if result.get("success"))
    skipped = [result for result in results if result.get("skipped")]
    assert success_count == 1
    assert len(skipped) == 1
    assert "今日已发过" in skipped[0]["reason"] or "需再等" in skipped[0]["reason"]
    assert state["daily_count"] == 1
    assert state["coin_last_post_date"]["BTC"] == time.strftime("%Y-%m-%d")


def test_execute_post_serializes_cross_coin_posts_to_preserve_global_interval():
    state = {
        "daily_count": 0,
        "total_posts": 0,
        "last_post_time": 0,
        "coin_last_post": {},
        "coin_last_post_date": {},
        "today": time.strftime("%Y-%m-%d"),
    }
    coin_infos = [
        {"coin": "BTC", "tier": "S", "futures": "BTCUSDT"},
        {"coin": "ETH", "tier": "S", "futures": "ETHUSDT"},
    ]

    class Poster:
        def post(self, content):
            time.sleep(0.05)
            return {"success": True, "post_id": "x", "url": None, "mock": True}

    results = []
    errors = []
    start = threading.Barrier(2)

    with patch("layers.executor.save_state", lambda *_args, **_kwargs: None), patch(
        "layers.executor.log_post", lambda *_args, **_kwargs: None
    ), patch("layers.executor.update_state", lambda mutator: mutator(state)):
        def worker(coin_info):
            try:
                quota = QuotaController(state)
                start.wait()
                results.append(execute_post(coin_info, "hello", state, quota, Poster()))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(coin_info,)) for coin_info in coin_infos]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert not errors
    success_count = sum(1 for result in results if result.get("success"))
    skipped = [result for result in results if result.get("skipped")]
    assert success_count == 1
    assert len(skipped) == 1
    assert "需再等" in skipped[0]["reason"]
    assert state["daily_count"] == 1


def _execute_post_in_process(state_file: str, post_log: str, coin: str, barrier, queue):
    from pathlib import Path
    from core import state as state_module
    from layers.executor import QuotaController, execute_post

    state_module.STATE_FILE = Path(state_file)
    state_module.POST_LOG = Path(post_log)
    state = state_module.load_state()
    quota = QuotaController(state)

    class Poster:
        def post(self, content):
            time.sleep(0.05)
            return {"success": True, "post_id": f"{coin}-id", "url": None, "mock": True}

    barrier.wait(timeout=5)
    result = execute_post({"coin": coin, "tier": "S", "futures": f"{coin}USDT"}, "hello", state, quota, Poster())
    queue.put(result)


def test_execute_post_serializes_cross_process_posts_to_preserve_global_interval(tmp_path):
    from core import state as state_module

    original_state_file = state_module.STATE_FILE
    original_post_log = state_module.POST_LOG
    try:
        state_file = tmp_path / "agent_state.json"
        post_log = tmp_path / "post_log.jsonl"
        state_module.STATE_FILE = state_file
        state_module.POST_LOG = post_log
        state_module.save_state(state_module.load_state())

        barrier = multiprocessing.Barrier(2)
        queue = multiprocessing.Queue()
        workers = [
            multiprocessing.Process(
                target=_execute_post_in_process,
                args=(str(state_file), str(post_log), coin, barrier, queue),
            )
            for coin in ("BTC", "ETH")
        ]

        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
            assert worker.exitcode == 0

        results = [queue.get(timeout=2) for _ in workers]
        success_count = sum(1 for result in results if result.get("success"))
        skipped = [result for result in results if result.get("skipped")]

        assert success_count == 1
        assert len(skipped) == 1
        assert "需再等" in skipped[0]["reason"]

        reloaded = state_module.load_state()
        assert reloaded["daily_count"] == 1
    finally:
        state_module.STATE_FILE = original_state_file
        state_module.POST_LOG = original_post_log


def test_save_state_is_atomic_under_concurrent_writers(tmp_path):
    from core import state as state_module

    original_state_file = state_module.STATE_FILE
    try:
        state_module.STATE_FILE = tmp_path / "agent_state.json"
        workers = [
            {"worker": i, "today": f"2026-04-{i+1:02d}", "daily_count": i}
            for i in range(8)
        ]
        errors = []

        def write_state(payload):
            try:
                for _ in range(40):
                    state_module.save_state(dict(payload))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=write_state, args=(payload,)) for payload in workers]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert not errors
        loaded = state_module.load_state()
        assert loaded["worker"] in {payload["worker"] for payload in workers}
    finally:
        state_module.STATE_FILE = original_state_file


def test_log_post_keeps_jsonl_integrity_under_concurrent_writers(tmp_path):
    from core import state as state_module

    original_post_log = state_module.POST_LOG
    try:
        state_module.POST_LOG = tmp_path / "post_log.jsonl"
        workers = list(range(6))
        errors = []

        def append_logs(worker_id):
            try:
                for idx in range(25):
                    state_module.log_post({"worker": worker_id, "seq": idx})
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=append_logs, args=(worker_id,)) for worker_id in workers]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert not errors
        with open(state_module.POST_LOG, "r", encoding="utf-8") as handle:
            lines = [line.strip() for line in handle if line.strip()]

        assert len(lines) == len(workers) * 25
        for line in lines:
            assert line.startswith("{") and line.endswith("}")
    finally:
        state_module.POST_LOG = original_post_log


def test_update_state_merges_against_latest_disk_state_without_losing_fields(tmp_path):
    from core import state as state_module

    original_state_file = state_module.STATE_FILE
    try:
        state_module.STATE_FILE = tmp_path / "agent_state.json"
        state_module.save_state({
            "status": "idle",
            "today": time.strftime("%Y-%m-%d"),
            "daily_count": 3,
            "guest_token": "token-123",
            "guest_token_time": 99,
            "coin_last_post": {"BTC": 1},
            "coin_last_post_date": {"BTC": time.strftime("%Y-%m-%d")},
            "total_posts": 5,
            "last_post_time": 10,
        })

        updated = state_module.update_state(lambda current: {**current, "status": "BANNED"})

        assert updated["status"] == "BANNED"
        assert updated["guest_token"] == "token-123"
        assert updated["guest_token_time"] == 99
        assert updated["daily_count"] == 3
        assert updated["coin_last_post"]["BTC"] == 1
        reloaded = state_module.load_state()
        assert reloaded == updated
    finally:
        state_module.STATE_FILE = original_state_file


def _increment_state_counter_in_process(state_file: str, rounds: int):
    from pathlib import Path
    from core import state as state_module

    state_module.STATE_FILE = Path(state_file)
    for _ in range(rounds):
        state_module.update_state(
            lambda current: {**current, "daily_count": current.get("daily_count", 0) + 1}
        )


def test_update_state_serializes_across_processes(tmp_path):
    from core import state as state_module

    original_state_file = state_module.STATE_FILE
    try:
        state_file = tmp_path / "agent_state.json"
        state_module.STATE_FILE = state_file
        state_module.save_state(state_module.load_state())

        workers = [
            multiprocessing.Process(target=_increment_state_counter_in_process, args=(str(state_file), 25))
            for _ in range(4)
        ]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
            assert worker.exitcode == 0

        reloaded = state_module.load_state()
        assert reloaded["daily_count"] == 100
    finally:
        state_module.STATE_FILE = original_state_file


def test_save_state_preserves_banned_when_stale_snapshot_writes_new_token(tmp_path):
    from core import state as state_module

    original_state_file = state_module.STATE_FILE
    try:
        state_module.STATE_FILE = tmp_path / "agent_state.json"
        state_module.save_state({
            "status": "BANNED",
            "today": time.strftime("%Y-%m-%d"),
            "daily_count": 7,
            "coin_last_post": {"BTC": 123},
            "coin_last_post_date": {"BTC": time.strftime("%Y-%m-%d")},
            "guest_token": None,
            "guest_token_time": 0,
        })

        stale = state_module.load_state()
        stale["status"] = "idle"
        stale["daily_count"] = 1
        stale["coin_last_post"] = {}
        stale["coin_last_post_date"] = {}
        stale["guest_token"] = "fresh-token"
        stale["guest_token_time"] = 999
        state_module.save_state(stale)

        reloaded = state_module.load_state()
        assert reloaded["status"] == "BANNED"
        assert reloaded["daily_count"] == 7
        assert reloaded["coin_last_post"]["BTC"] == 123
        assert reloaded["guest_token"] == "fresh-token"
        assert reloaded["guest_token_time"] == 999
    finally:
        state_module.STATE_FILE = original_state_file


def test_save_state_ignores_stale_previous_day_quota_fields_when_refreshing_token(tmp_path):
    from core import state as state_module

    original_state_file = state_module.STATE_FILE
    try:
        state_module.STATE_FILE = tmp_path / "agent_state.json"
        today = time.strftime("%Y-%m-%d")
        yesterday = time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400))
        state_module.save_state({
            "status": "BANNED",
            "today": today,
            "daily_count": 0,
            "coin_last_post": {"BTC": 500},
            "coin_last_post_date": {},
            "guest_token": None,
            "guest_token_time": 0,
        })

        stale = {
            "status": "idle",
            "today": yesterday,
            "daily_count": 9,
            "coin_last_post": {},
            "coin_last_post_date": {"BTC": yesterday},
            "guest_token": "fresh-token",
            "guest_token_time": 999,
        }
        merged = state_module.save_state(stale)

        assert merged["status"] == "BANNED"
        assert merged["today"] == today
        assert merged["daily_count"] == 0
        assert merged["coin_last_post_date"] == {}
        assert merged["guest_token"] == "fresh-token"
        assert merged["guest_token_time"] == 999
        reloaded = state_module.load_state()
        assert reloaded["today"] == today
        assert reloaded["daily_count"] == 0
        assert reloaded["coin_last_post_date"] == {}
    finally:
        state_module.STATE_FILE = original_state_file


def test_save_state_preserves_latest_posting_intent_against_stale_different_intent(tmp_path):
    from core import state as state_module

    original_state_file = state_module.STATE_FILE
    try:
        state_module.STATE_FILE = tmp_path / "agent_state.json"
        today = time.strftime("%Y-%m-%d")
        latest_intent = {
            "id": "live-intent",
            "coin": "BTC",
            "content_hash": "sha256:live",
            "status": "IN_FLIGHT",
            "source": "executor",
            "created_at": 456.0,
        }
        stale_intent = {
            "id": "stale-intent",
            "coin": "ETH",
            "content_hash": "sha256:stale",
            "status": "IN_FLIGHT",
            "source": "w2e",
            "created_at": 123.0,
        }
        state_module.save_state({
            "status": "idle",
            "today": today,
            "daily_count": 0,
            "posting_intent": latest_intent,
            "recent_post_keys": {},
        })

        stale = state_module.load_state()
        stale["guest_token"] = "fresh-token"
        stale["posting_intent"] = stale_intent
        merged = state_module.save_state(stale)

        assert merged["posting_intent"] == latest_intent
        assert merged["guest_token"] == "fresh-token"
    finally:
        state_module.STATE_FILE = original_state_file


def test_save_state_does_not_resurrect_cleared_posting_intent_from_stale_snapshot(tmp_path):
    from core import state as state_module

    original_state_file = state_module.STATE_FILE
    try:
        state_module.STATE_FILE = tmp_path / "agent_state.json"
        today = time.strftime("%Y-%m-%d")
        stale_intent = {
            "id": "stale-intent",
            "coin": "ETH",
            "content_hash": "sha256:stale",
            "status": "IN_FLIGHT",
            "source": "w2e",
            "created_at": 123.0,
        }
        baseline = state_module.save_state({
            "status": "idle",
            "today": today,
            "daily_count": 0,
            "posting_intent": stale_intent,
            "recent_post_keys": {},
        })

        stale = dict(baseline)
        state_module.update_state(lambda current: {
            **current,
            "posting_intent": None,
            "posting_intent_cleared_at": 999.0,
        })
        stale["guest_token"] = "fresh-token"
        merged = state_module.save_state(stale)

        assert merged["posting_intent"] is None
        assert merged["posting_intent_cleared_at"] == 999.0
        assert merged["guest_token"] == "fresh-token"
    finally:
        state_module.STATE_FILE = original_state_file


def test_save_state_preserves_newer_recent_post_key_metadata_against_stale_snapshot(tmp_path):
    from core import state as state_module

    original_state_file = state_module.STATE_FILE
    try:
        state_module.STATE_FILE = tmp_path / "agent_state.json"
        today = time.strftime("%Y-%m-%d")
        state_module.save_state({
            "status": "idle",
            "today": today,
            "daily_count": 0,
            "recent_post_keys": {
                "sha256:dup": {
                    "coin": "BTC",
                    "created_at": 200.0,
                    "post_id": "new-post",
                    "url": "https://example.com/new",
                    "source": "executor",
                }
            },
        })

        stale = state_module.load_state()
        stale["guest_token"] = "fresh-token"
        stale["recent_post_keys"] = {
            "sha256:dup": {
                "coin": "BTC",
                "created_at": 100.0,
                "post_id": "old-post",
                "url": "https://example.com/old",
                "source": "w2e",
            }
        }
        merged = state_module.save_state(stale)

        assert merged["guest_token"] == "fresh-token"
        assert merged["recent_post_keys"]["sha256:dup"]["created_at"] == 200.0
        assert merged["recent_post_keys"]["sha256:dup"]["post_id"] == "new-post"
        assert merged["recent_post_keys"]["sha256:dup"]["url"] == "https://example.com/new"
        assert merged["recent_post_keys"]["sha256:dup"]["source"] == "executor"
    finally:
        state_module.STATE_FILE = original_state_file

def test_w2e_run_once_returns_specific_quota_reason_for_global_interval_skip():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    gen.poster = type("Poster", (), {"mock_mode": True})()

    reference = {
        "creator": {"nickname": "tester", "earnings_usdc": 1},
        "post": {"text": "$BTC 还没走完，先别追高"},
    }

    gen._load_w2e_data = lambda: [{"nickname": "tester", "recent_posts": [reference["post"]]}]
    gen._get_tg_hot_coins = lambda: []
    gen._select_reference_post_with_tg = lambda creators, tg_signals: (reference, None)
    gen._extract_main_coin = lambda text: "BTC"
    gen._rewrite_with_llm = lambda ref: (_ for _ in ()).throw(AssertionError("should skip before llm rewrite"))
    gen._format_fixed_template_post = lambda body, coin: body

    now = time.time()
    with patch("w2e_post_generator.load_state", return_value={
        "status": "idle",
        "daily_count": 0,
        "last_post_time": now,
        "coin_last_post": {},
        "coin_last_post_date": {},
        "today": time.strftime("%Y-%m-%d"),
    }):
        result = gen.run_once()

    assert result["success"] is False
    assert result["reason"].startswith("global_interval:")


def test_w2e_run_once_returns_specific_quota_reason_for_banned_account_skip():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    gen.poster = type("Poster", (), {"mock_mode": True})()

    reference = {
        "creator": {"nickname": "tester", "earnings_usdc": 1},
        "post": {"text": "$BTC 还没走完，先别追高"},
    }

    gen._load_w2e_data = lambda: [{"nickname": "tester", "recent_posts": [reference["post"]]}]
    gen._get_tg_hot_coins = lambda: []
    gen._select_reference_post_with_tg = lambda creators, tg_signals: (reference, None)
    gen._extract_main_coin = lambda text: "BTC"
    gen._rewrite_with_llm = lambda ref: (_ for _ in ()).throw(AssertionError("should skip before llm rewrite"))
    gen._format_fixed_template_post = lambda body, coin: body

    with patch("w2e_post_generator.load_state", return_value={
        "status": "BANNED",
        "daily_count": 0,
        "last_post_time": 0,
        "coin_last_post": {},
        "coin_last_post_date": {},
        "today": time.strftime("%Y-%m-%d"),
    }):
        result = gen.run_once()

    assert result["success"] is False
    assert result["reason"] == "banned"


def test_execute_post_persists_intent_before_external_post_and_finalizes_recent_key(tmp_path):
    from core import state as state_module

    original_state_file = state_module.STATE_FILE
    original_post_log = state_module.POST_LOG
    try:
        state_module.STATE_FILE = tmp_path / "agent_state.json"
        state_module.POST_LOG = tmp_path / "post_log.jsonl"
        initial = state_module.load_state()
        state = state_module.save_state(initial)
        quota = QuotaController(state)
        observed = {}

        class Poster:
            def post(self, content):
                current = state_module.load_state()
                observed["posting_intent"] = current.get("posting_intent")
                observed["recent_post_keys"] = dict(current.get("recent_post_keys", {}))
                return {"success": True, "code": "000000", "post_id": "post-123", "url": "https://example.com/p/123", "mock": True}

        result = execute_post(
            {"coin": "BTC", "tier": "S", "futures": "BTCUSDT"},
            "BTC breakout setup",
            state,
            quota,
            Poster(),
        )

        assert result["success"] is True
        assert observed["posting_intent"] is not None
        assert observed["posting_intent"]["status"] == "IN_FLIGHT"
        assert observed["posting_intent"]["coin"] == "BTC"
        assert observed["posting_intent"]["source"] == "executor"
        assert observed["recent_post_keys"] == {}

        reloaded = state_module.load_state()
        assert reloaded.get("posting_intent") is None
        assert len(reloaded.get("recent_post_keys", {})) == 1
        recent_entry = next(iter(reloaded["recent_post_keys"].values()))
        assert recent_entry["coin"] == "BTC"
        assert recent_entry["post_id"] == "post-123"
        assert recent_entry["source"] == "executor"
    finally:
        state_module.STATE_FILE = original_state_file
        state_module.POST_LOG = original_post_log


def test_w2e_run_once_persists_intent_before_external_post_and_finalizes_recent_key(tmp_path):
    from core import state as state_module

    original_state_file = state_module.STATE_FILE
    original_post_log = state_module.POST_LOG
    try:
        state_module.STATE_FILE = tmp_path / "agent_state.json"
        state_module.POST_LOG = tmp_path / "post_log.jsonl"

        gen = W2EPostGenerator.__new__(W2EPostGenerator)
        gen.state = state_module.save_state(state_module.load_state())
        observed = {}

        reference = {
            "creator": {"nickname": "tester", "earnings_usdc": 1},
            "post": {"text": "$BTC 还没走完，先别追高"},
        }
        gen._load_w2e_data = lambda: [{"nickname": "tester", "recent_posts": [reference["post"]]}]
        gen._get_tg_hot_coins = lambda: []
        gen._select_reference_post_with_tg = lambda creators, tg_signals: (reference, None)
        gen._extract_main_coin = lambda text: "BTC"
        gen._rewrite_with_llm = lambda ref: ("BTC breakout setup", "BTC")
        gen._format_fixed_template_post = lambda body, coin: body

        class Poster:
            mock_mode = True

            def post(self, content):
                current = state_module.load_state()
                observed["posting_intent"] = current.get("posting_intent")
                observed["recent_post_keys"] = dict(current.get("recent_post_keys", {}))
                return {"success": True, "code": "000000", "post_id": "w2e-123", "url": "https://example.com/p/w2e-123", "mock": True}

        gen.poster = Poster()

        result = gen.run_once()

        assert result["success"] is True
        assert observed["posting_intent"] is not None
        assert observed["posting_intent"]["status"] == "IN_FLIGHT"
        assert observed["posting_intent"]["coin"] == "BTC"
        assert observed["posting_intent"]["source"] == "w2e"
        assert observed["recent_post_keys"] == {}

        reloaded = state_module.load_state()
        assert reloaded.get("posting_intent") is None
        assert len(reloaded.get("recent_post_keys", {})) == 1
        recent_entry = next(iter(reloaded["recent_post_keys"].values()))
        assert recent_entry["coin"] == "BTC"
        assert recent_entry["post_id"] == "w2e-123"
        assert recent_entry["source"] == "w2e"
    finally:
        state_module.STATE_FILE = original_state_file
        state_module.POST_LOG = original_post_log


def test_execute_post_keeps_pending_intent_on_ambiguous_network_error(tmp_path):
    from core import state as state_module

    original_state_file = state_module.STATE_FILE
    original_post_log = state_module.POST_LOG
    try:
        state_module.STATE_FILE = tmp_path / "agent_state.json"
        state_module.POST_LOG = tmp_path / "post_log.jsonl"
        state = state_module.save_state(state_module.load_state())
        quota = QuotaController(state)

        class Poster:
            mock_mode = False

            def post(self, content):
                return {"success": False, "code": "NETWORK_ERROR", "message": "timeout", "mock": False}

        result = execute_post(
            {"coin": "BTC", "tier": "S", "futures": "BTCUSDT"},
            "BTC breakout setup",
            state,
            quota,
            Poster(),
        )

        assert result["success"] is False
        assert result["code"] == "NETWORK_ERROR"

        reloaded = state_module.load_state()
        assert reloaded.get("posting_intent") is not None
        assert reloaded["posting_intent"]["coin"] == "BTC"
        assert reloaded["posting_intent"]["source"] == "executor"
        assert reloaded.get("recent_post_keys", {}) == {}
    finally:
        state_module.STATE_FILE = original_state_file
        state_module.POST_LOG = original_post_log


def test_execute_post_skips_when_different_pending_intent_already_exists(tmp_path):
    from core import state as state_module

    original_state_file = state_module.STATE_FILE
    original_post_log = state_module.POST_LOG
    try:
        state_module.STATE_FILE = tmp_path / "agent_state.json"
        state_module.POST_LOG = tmp_path / "post_log.jsonl"
        state_module.save_state({
            **state_module.load_state(),
            "posting_intent": {
                "id": "pending-eth",
                "coin": "ETH",
                "content_hash": "sha256:eth-pending",
                "content_preview": "ETH pending",
                "source": "executor",
                "tier": "S",
                "created_at": time.time(),
                "status": "IN_FLIGHT",
                "post_id": "",
                "url": "",
                "result_code": "NETWORK_ERROR",
                "mock": False,
            },
        })
        state = state_module.load_state()
        quota = QuotaController(state)

        class Poster:
            def post(self, content):
                raise AssertionError("should skip before external post")

        result = execute_post(
            {"coin": "BTC", "tier": "S", "futures": "BTCUSDT"},
            "BTC breakout setup",
            state,
            quota,
            Poster(),
        )

        assert result["success"] is False
        assert result["skipped"] is True
        assert result["reason"] == "pending_intent_in_flight:ETH"
        reloaded = state_module.load_state()
        assert reloaded["posting_intent"]["id"] == "pending-eth"
        assert reloaded["posting_intent"]["coin"] == "ETH"
    finally:
        state_module.STATE_FILE = original_state_file
        state_module.POST_LOG = original_post_log


def test_execute_post_skips_when_recent_post_key_matches_same_coin(tmp_path):
    from core import state as state_module
    from layers import executor as executor_module

    original_state_file = state_module.STATE_FILE
    original_post_log = state_module.POST_LOG
    original_hasher = executor_module._content_hash
    try:
        state_module.STATE_FILE = tmp_path / "agent_state.json"
        state_module.POST_LOG = tmp_path / "post_log.jsonl"
        state = state_module.save_state(state_module.load_state())
        quota = QuotaController(state)

        executor_module._content_hash = lambda content: "sha256:dup"
        state_module.save_state({
            **state_module.load_state(),
            "recent_post_keys": {
                "sha256:dup": {
                    "coin": "BTC",
                    "created_at": time.time(),
                    "post_id": "existing-post",
                    "source": "executor",
                }
            },
        })

        class Poster:
            def post(self, content):
                raise AssertionError("should skip before external post")

        result = execute_post(
            {"coin": "BTC", "tier": "S", "futures": "BTCUSDT"},
            "BTC breakout setup",
            state,
            quota,
            Poster(),
        )

        assert result["success"] is False
        assert result["skipped"] is True
        assert result["reason"] == "recent_duplicate:BTC"
    finally:
        executor_module._content_hash = original_hasher
        state_module.STATE_FILE = original_state_file
        state_module.POST_LOG = original_post_log


def test_execute_post_tolerates_legacy_non_dict_recent_post_key_entry(tmp_path):
    from core import state as state_module
    from layers import executor as executor_module

    original_state_file = state_module.STATE_FILE
    original_post_log = state_module.POST_LOG
    original_hasher = executor_module._content_hash
    try:
        state_module.STATE_FILE = tmp_path / "agent_state.json"
        state_module.POST_LOG = tmp_path / "post_log.jsonl"
        state = state_module.save_state(state_module.load_state())
        quota = QuotaController(state)

        executor_module._content_hash = lambda content: "sha256:legacy"
        state_module.save_state({
            **state_module.load_state(),
            "recent_post_keys": {
                "sha256:legacy": "legacy-value"
            },
        })

        class Poster:
            mock_mode = True

            def post(self, content):
                return {
                    "success": True,
                    "code": "000000",
                    "post_id": "legacy-fixed",
                    "url": "https://example.com/p/legacy-fixed",
                    "mock": True,
                }

        result = execute_post(
            {"coin": "BTC", "tier": "S", "futures": "BTCUSDT"},
            "BTC breakout setup",
            state,
            quota,
            Poster(),
        )

        assert result["success"] is True
        reloaded = state_module.load_state()
        assert reloaded["recent_post_keys"]["sha256:legacy"]["coin"] == "BTC"
        assert reloaded["recent_post_keys"]["sha256:legacy"]["post_id"] == "legacy-fixed"
    finally:
        executor_module._content_hash = original_hasher
        state_module.STATE_FILE = original_state_file
        state_module.POST_LOG = original_post_log


def test_w2e_run_once_returns_specific_quota_reason_for_global_interval_skip():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    gen.poster = type("Poster", (), {"mock_mode": True})()

    reference = {
        "creator": {"nickname": "tester", "earnings_usdc": 1},
        "post": {"text": "$BTC 还没走完，先别追高"},
    }

    gen._load_w2e_data = lambda: [{"nickname": "tester", "recent_posts": [reference["post"]]}]
    gen._get_tg_hot_coins = lambda: []
    gen._select_reference_post_with_tg = lambda creators, tg_signals: (reference, None)
    gen._extract_main_coin = lambda text: "BTC"
    gen._rewrite_with_llm = lambda ref: (_ for _ in ()).throw(AssertionError("should skip before llm rewrite"))
    gen._format_fixed_template_post = lambda body, coin: body

    now = time.time()
    with patch("w2e_post_generator.load_state", return_value={
        "status": "idle",
        "daily_count": 0,
        "last_post_time": now,
        "coin_last_post": {},
        "coin_last_post_date": {},
        "today": time.strftime("%Y-%m-%d"),
    }):
        result = gen.run_once()

    assert result["success"] is False
    assert result["reason"].startswith("global_interval:")


def test_w2e_run_once_returns_specific_quota_reason_for_banned_account_skip():
    gen = W2EPostGenerator.__new__(W2EPostGenerator)
    gen.poster = type("Poster", (), {"mock_mode": True})()

    reference = {
        "creator": {"nickname": "tester", "earnings_usdc": 1},
        "post": {"text": "$BTC 还没走完，先别追高"},
    }

    gen._load_w2e_data = lambda: [{"nickname": "tester", "recent_posts": [reference["post"]]}]
    gen._get_tg_hot_coins = lambda: []
    gen._select_reference_post_with_tg = lambda creators, tg_signals: (reference, None)
    gen._extract_main_coin = lambda text: "BTC"
    gen._rewrite_with_llm = lambda ref: (_ for _ in ()).throw(AssertionError("should skip before llm rewrite"))
    gen._format_fixed_template_post = lambda body, coin: body

    with patch("w2e_post_generator.load_state", return_value={
        "status": "BANNED",
        "daily_count": 0,
        "last_post_time": 0,
        "coin_last_post": {},
        "coin_last_post_date": {},
        "today": time.strftime("%Y-%m-%d"),
    }):
        result = gen.run_once()

    assert result["success"] is False
    assert result["reason"] == "banned"
