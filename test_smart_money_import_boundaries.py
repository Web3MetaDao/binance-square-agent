"""Regression tests for smart_money package import boundaries."""

import importlib
import sys


def test_importing_smart_money_package_does_not_require_optional_websocket(monkeypatch):
    """Package import should not eagerly import optional address-updater deps."""
    sys.modules.pop("smart_money", None)
    sys.modules.pop("smart_money.address_updater", None)

    real_import = __import__

    def guarded_import(name, *args, **kwargs):
        if name == "websocket":
            raise ModuleNotFoundError("No module named 'websocket'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", guarded_import)

    module = importlib.import_module("smart_money")

    assert module.__name__ == "smart_money"
    assert "smart_money.address_updater" not in sys.modules


def test_core_smart_money_submodules_import_without_optional_websocket(monkeypatch):
    """Core monitor/content modules should remain usable without websocket-client."""
    for module_name in (
        "smart_money",
        "smart_money.address_updater",
        "smart_money.smart_money_monitor",
        "smart_money.signal_to_content",
    ):
        sys.modules.pop(module_name, None)

    real_import = __import__

    def guarded_import(name, *args, **kwargs):
        if name == "websocket":
            raise ModuleNotFoundError("No module named 'websocket'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", guarded_import)

    monitor = importlib.import_module("smart_money.smart_money_monitor")
    content = importlib.import_module("smart_money.signal_to_content")

    assert hasattr(monitor, "aggregate_smart_money_signals")
    assert hasattr(content, "build_content_prompt")
    assert "smart_money.address_updater" not in sys.modules
