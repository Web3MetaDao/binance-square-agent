"""
Perp DEX 聪明钱监控模块
专针对永续合约去中心化交易所的链上大户追踪

子模块：
- smart_money_monitor: 核心监控器（Hyperliquid OI + 大户持仓扫描）
- address_updater: 地址库自动更新器
- signal_to_content: 信号 → 内容层适配器
"""

from .smart_money_monitor import (
    aggregate_smart_money_signals,
    get_cached_signals,
    get_market_overview,
    get_whale_positions,
    print_signal_report,
)
from .signal_to_content import (
    get_top_signal,
    get_all_signals,
    build_content_prompt,
    get_content_hints_for_display,
)
from .address_updater import (
    get_smart_money_addresses,
    update_address_database,
)

__all__ = [
    "aggregate_smart_money_signals",
    "get_cached_signals",
    "get_market_overview",
    "get_whale_positions",
    "print_signal_report",
    "get_top_signal",
    "get_all_signals",
    "build_content_prompt",
    "get_content_hints_for_display",
    "get_smart_money_addresses",
    "update_address_database",
]
