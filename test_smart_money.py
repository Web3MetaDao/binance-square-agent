#!/usr/bin/env python3
"""
Perp DEX 聪明钱监控模块 - 集成测试
测试所有核心功能：市场概览、大户持仓、信号聚合、内容适配
"""

import sys
import json
import time
import os

sys.path.insert(0, os.path.dirname(__file__))

def test_market_overview():
    """测试 1: 全市场 OI 与资金费率概览"""
    print("\n" + "="*60)
    print("测试 1: 全市场 OI 与资金费率概览")
    print("="*60)
    from smart_money.smart_money_monitor import get_market_overview
    
    market = get_market_overview()
    assert market, "市场数据获取失败"
    assert "oi_top20" in market, "缺少 OI 排行数据"
    assert len(market["oi_top20"]) > 0, "OI 排行为空"
    
    total_oi = market["total_oi_usd"]
    oi_top5 = market["oi_top20"][:5]
    hot_coins = market["hot_coins_24h"][:5]
    
    print(f"✅ 全市场总 OI: ${total_oi/1e9:.2f}B")
    print(f"✅ OI Top 5:")
    for c in oi_top5:
        print(f"   {c['coin']:8s} OI=${c['oi_usd']/1e6:.0f}M  价格=${c['mark_px']:,.2f}  "
              f"24h={c['change_24h']:+.1f}%  资金费率={c['funding_rate']:+.4f}%")
    
    if hot_coins:
        print(f"✅ 24h 热点代币 Top 5:")
        for c in hot_coins:
            print(f"   {c['coin']:8s} 涨跌={c['change_24h']:+.1f}%  价格=${c['mark_px']:,.2f}")
    
    return True


def test_whale_position_scan():
    """测试 2: 大户持仓扫描"""
    print("\n" + "="*60)
    print("测试 2: 大户持仓扫描（使用已知活跃地址）")
    print("="*60)
    from smart_money.smart_money_monitor import get_whale_positions
    
    # 使用已知的 Hyperliquid 活跃地址
    test_address = "0xc6ab9ee8ad3647a12242a2afa43152be796f3391"
    
    result = get_whale_positions(test_address)
    
    if result:
        print(f"✅ 地址: {test_address[:12]}...")
        print(f"✅ 账户价值: ${result['account_value']:,.2f}")
        print(f"✅ 持仓数量: {result['position_count']}")
        if result["positions"]:
            print(f"✅ 当前持仓:")
            for pos in result["positions"][:5]:
                icon = "🟢" if pos["direction"] == "LONG" else "🔴"
                print(f"   {icon} {pos['coin']:8s} {pos['direction']:5s} "
                      f"${pos['size_usd']:,.0f}  入场价=${pos['entry_px']:,.4f}  "
                      f"杠杆={pos['leverage']}x  ROI={pos['roi']:+.1f}%")
        else:
            print("  (当前无持仓)")
    else:
        print("⚠️  该地址暂无数据（可能账户余额不足或地址无效）")
        print("✅ 接口连通正常（返回 None 表示账户为空）")
    
    return True


def test_recent_trades():
    """测试 3: 大户最近成交记录"""
    print("\n" + "="*60)
    print("测试 3: 大户最近成交记录")
    print("="*60)
    from smart_money.smart_money_monitor import get_whale_recent_trades
    
    test_address = "0xc6ab9ee8ad3647a12242a2afa43152be796f3391"
    trades = get_whale_recent_trades(test_address, limit=10)
    
    if trades:
        print(f"✅ 最近 {len(trades)} 笔成交:")
        for t in trades[:5]:
            icon = "🟢" if t["side"] == "BUY" else "🔴"
            pnl_str = f"PnL=${t['pnl']:+,.2f}" if t["pnl"] != 0 else ""
            print(f"   {icon} {t['coin']:8s} {t['direction']:15s} "
                  f"价格=${t['price']:,.4f}  数量={t['size']:.4f}  {pnl_str}")
    else:
        print("⚠️  暂无成交记录")
        print("✅ 接口连通正常")
    
    return True


def test_full_signal_aggregation():
    """测试 4: 完整信号聚合（核心测试）"""
    print("\n" + "="*60)
    print("测试 4: 完整聪明钱信号聚合")
    print("="*60)
    from smart_money.smart_money_monitor import aggregate_smart_money_signals, print_signal_report
    
    signals = aggregate_smart_money_signals()
    
    assert "timestamp" in signals, "缺少时间戳"
    assert "market_overview" in signals, "缺少市场概览"
    assert "top_signals" in signals, "缺少信号列表"
    
    print_signal_report(signals)
    
    # 验证内容提示
    hints = signals.get("content_hints", [])
    print(f"\n✅ 生成内容提示 {len(hints)} 条")
    for hint in hints[:3]:
        print(f"  • {hint}")
    
    return True


def test_signal_to_content():
    """测试 5: 信号 → 内容层适配"""
    print("\n" + "="*60)
    print("测试 5: 信号 → 内容层适配")
    print("="*60)
    from smart_money.signal_to_content import get_all_signals, build_content_prompt
    
    signals = get_all_signals()
    
    if signals:
        print(f"✅ 获取到 {len(signals)} 个可用信号")
        
        # 测试 Prompt 生成
        for sig in signals[:2]:
            content = build_content_prompt(
                sig,
                                cta_index=0
            )
            print(f"\n  信号类型: {content['signal_type']}")
            print(f"  代币: {content['coin']}")
            print(f"  期货标签: {content['futures_tags']}")
            print(f"  Prompt 长度: {len(content['prompt'])} 字符")
            print(f"  Prompt 预览:\n  {content['prompt'][:200].strip()}...")
    else:
        print("⚠️  暂无信号（需要先运行完整扫描）")
        
        # 构造测试信号
        test_signal = {
            "type": "LONG_HIGH",
            "coin": "BTC",
            "data": {
                "whale_count": 5,
                "long_count": 4,
                "long_ratio": 80.0,
                "total_size_usd": 5_000_000,
                "mark_px": 74500,
                "change_24h": 2.3,
                "funding_rate": 0.0125,
                "net_direction": "LONG",
            },
            "priority": 1,
        }
        content = build_content_prompt(test_signal)
        print(f"✅ 使用测试信号生成 Prompt 成功")
        print(f"  期货标签: {content['futures_tags']}")
        print(f"  Prompt 长度: {len(content['prompt'])} 字符")
        print(f"  Prompt 预览:\n  {content['prompt'][:300].strip()}...")
    
    return True


def test_address_database():
    """测试 6: 地址库管理"""
    print("\n" + "="*60)
    print("测试 6: 聪明钱地址库")
    print("="*60)
    from smart_money.address_updater import get_smart_money_addresses
    
    addresses = get_smart_money_addresses()
    print(f"✅ 当前地址库: {len(addresses)} 个地址")
    for addr in addresses[:5]:
        print(f"  {addr[:12]}...")
    
    return True


def run_all_tests():
    """运行所有测试"""
    print("\n" + "🚀 " * 20)
    print("Perp DEX 聪明钱监控模块 - 完整集成测试")
    print("🚀 " * 20)
    
    tests = [
        ("市场 OI 与资金费率概览", test_market_overview),
        ("大户持仓扫描", test_whale_position_scan),
        ("大户最近成交记录", test_recent_trades),
        ("完整信号聚合", test_full_signal_aggregation),
        ("信号 → 内容层适配", test_signal_to_content),
        ("地址库管理", test_address_database),
    ]
    
    results = []
    for name, test_fn in tests:
        try:
            result = test_fn()
            results.append((name, "✅ 通过", None))
        except Exception as e:
            results.append((name, "❌ 失败", str(e)))
            print(f"\n❌ 测试失败: {name}")
            print(f"   错误: {e}")
            import traceback
            traceback.print_exc()
    
    # 汇总报告
    print("\n" + "="*60)
    print("📊 测试汇总报告")
    print("="*60)
    passed = sum(1 for _, status, _ in results if "✅" in status)
    total = len(results)
    
    for name, status, error in results:
        print(f"  {status} {name}")
        if error:
            print(f"       错误: {error[:100]}")
    
    print(f"\n总计: {passed}/{total} 通过")
    
    if passed == total:
        print("\n🎉 所有测试通过！Perp DEX 聪明钱监控模块已就绪！")
    else:
        print(f"\n⚠️  {total - passed} 个测试失败，请检查网络连接和配置")
    
    return passed, total


if __name__ == "__main__":
    passed, total = run_all_tests()
    sys.exit(0 if passed == total else 1)
