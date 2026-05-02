import time

from smart_money.signal_to_content import build_content_prompt
import utils.price_sync as price_sync


def _long_signal(data_overrides=None, signal_overrides=None):
    data = {
        "whale_count": 5,
        "long_count": 4,
        "long_ratio": 80.0,
        "total_size_usd": 5_000_000,
        "mark_px": 74500.12,
        "change_24h": 2.3,
        "funding_rate": 0.0125,
        "net_direction": "LONG",
    }
    if data_overrides:
        data.update(data_overrides)
    signal = {"type": "LONG_HIGH", "coin": "BTC", "data": data, "priority": 1}
    if signal_overrides:
        signal.update(signal_overrides)
    return signal


def test_smart_money_prompt_does_not_embed_unsynced_exact_price():
    prompt = build_content_prompt(_long_signal())["prompt"]

    assert "$74,500.12" not in prompt
    assert "当前价格：" not in prompt
    assert "未获取到新鲜同步价格" in prompt
    assert "禁止在正文中编造或引用具体当前价" in prompt
    assert "24H涨跌 +2.3%" in prompt


def test_smart_money_prompt_does_not_embed_stale_synced_exact_price():
    stale_ts = time.time() - price_sync.PRICE_FRESHNESS_TTL - 5
    prompt = build_content_prompt(
        _long_signal(
            {
                "_price_synced": True,
                "_price_source": "binance_futures",
                "_price_ts": stale_ts,
                "is_live": False,
                "price_age_sec": price_sync.PRICE_FRESHNESS_TTL + 5,
            }
        )
    )["prompt"]

    assert "$74,500.12" not in prompt
    assert "币安期货实时价格" not in prompt
    assert "未获取到新鲜同步价格" in prompt


def test_smart_money_prompt_embeds_fresh_synced_price():
    prompt = build_content_prompt(
        _long_signal(
            {
                "mark_px": 63800.0,
                "change_24h": 2.5,
                "_price_synced": True,
                "_price_source": "binance_futures",
                "_price_ts": time.time(),
                "is_live": True,
                "price_age_sec": 0.1,
            }
        )
    )["prompt"]

    assert "币安期货实时价格：$63,800.00，24H涨跌 +2.5%" in prompt
    assert "未获取到新鲜同步价格" not in prompt


def test_batch_refresh_prices_mirrors_freshness_metadata_into_signal_data(monkeypatch):
    now = time.time()
    monkeypatch.setattr(price_sync, "refresh_snapshot", lambda force=False: True)
    monkeypatch.setattr(
        price_sync,
        "_snapshot",
        {
            "BTCUSDT": {
                "coin": "BTC",
                "symbol": "BTCUSDT",
                "price": 63800.0,
                "change_24h": 2.5,
                "high_24h": 64500.0,
                "low_24h": 62000.0,
                "volume_24h": 123456.0,
                "open_24h": 63000.0,
                "ts": now,
            }
        },
    )
    signal = _long_signal({"mark_px": 74500.0, "change_24h": 0.0})

    refreshed = price_sync.batch_refresh_prices([signal])[0]
    data = refreshed["data"]

    assert refreshed["_price_synced"] is True
    assert refreshed["_price_source"] == "binance_futures"
    assert refreshed["_price_ts"] == now
    assert refreshed["is_live"] is True
    assert data["mark_px"] == 63800.0
    assert data["change_24h"] == 2.5
    assert data["_price_synced"] is True
    assert data["_price_source"] == "binance_futures"
    assert data["_price_ts"] == now
    assert data["is_live"] is True
    assert data["price_age_sec"] is not None


def test_batch_refresh_prices_does_not_overwrite_with_stale_snapshot(monkeypatch):
    stale_ts = time.time() - price_sync.PRICE_FRESHNESS_TTL - 5
    monkeypatch.setattr(price_sync, "refresh_snapshot", lambda force=False: True)
    monkeypatch.setattr(price_sync, "get_futures_price", lambda coin: None)
    monkeypatch.setattr(
        price_sync,
        "_snapshot",
        {
            "BTCUSDT": {
                "coin": "BTC",
                "symbol": "BTCUSDT",
                "price": 63800.0,
                "change_24h": 2.5,
                "ts": stale_ts,
            }
        },
    )
    signal = _long_signal({"mark_px": 74500.0, "change_24h": 0.0})

    refreshed = price_sync.batch_refresh_prices([signal])[0]
    data = refreshed["data"]

    assert refreshed["_price_synced"] is False
    assert refreshed["is_live"] is False
    assert refreshed["_price_ts"] == stale_ts
    assert data["mark_px"] == 74500.0
    assert data["change_24h"] == 0.0
    assert data["_price_synced"] is False
    assert data["is_live"] is False
    assert data["_price_ts"] == stale_ts


def test_build_content_prompt_returns_sanitized_coin_info_patch_for_unsynced_signal():
    result = build_content_prompt(_long_signal())

    assert result["coin_info_patch"]["mark_px"] == 0
    assert result["coin_info_patch"]["price"] == 0
    assert result["coin_info_patch"]["change_24h"] == 2.3
    assert result["coin_info_patch"]["is_live"] is False
    assert result["price_metadata"]["_price_synced"] is False


def test_build_content_prompt_returns_fresh_coin_info_patch_for_synced_signal():
    now = time.time()
    result = build_content_prompt(
        _long_signal(
            {
                "mark_px": 63800.0,
                "price": 63800.0,
                "change_24h": 2.5,
                "high_24h": 64500.0,
                "low_24h": 62000.0,
                "_price_synced": True,
                "_price_source": "binance_futures",
                "_price_ts": now,
                "is_live": True,
            }
        )
    )

    patch = result["coin_info_patch"]
    assert patch["mark_px"] == 63800.0
    assert patch["price"] == 63800.0
    assert patch["change_24h"] == 2.5
    assert patch["high_24h"] == 64500.0
    assert patch["low_24h"] == 62000.0
    assert patch["_price_synced"] is True
    assert patch["_price_source"] == "binance_futures"
    assert patch["_price_ts"] == now
    assert patch["is_live"] is True
