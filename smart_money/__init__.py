"""
Perp DEX 聪明钱监控模块
专针对永续合约去中心化交易所的链上大户追踪

Submodules are intentionally not imported here.  Some smart_money features use
optional runtime dependencies (for example websocket-client for address database
updates); importing the package itself must stay lightweight so unrelated test
collection and direct submodule imports do not fail when those extras are absent.
Import the required functions from their submodules, e.g.::

    from smart_money.smart_money_monitor import aggregate_smart_money_signals
    from smart_money.signal_to_content import build_content_prompt
    from smart_money.address_updater import get_smart_money_addresses
"""

__all__ = []
