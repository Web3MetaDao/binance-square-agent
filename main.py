#!/usr/bin/env python3
"""
币安广场运营系统智能体 — 主程序入口 v2.2
==========================================
用法：
  python3 main.py start             # 启动全自动发帖循环（热点扫描模式）
  python3 main.py once              # 执行单次发帖（热点模式）
  python3 main.py w2e               # 启动 W2E 模式：每30分钟自动从排行榜博主帖子提取内容并发帖
  python3 main.py w2e-once          # W2E 模式单次测试（抓取一次帖子并发帖）
  python3 main.py smart-money       # 启动聪明钱模式：每15分钟扫描 Hyperliquid 大户持仓并发帖
  python3 main.py smart-money-once  # 聪明钱模式单次测试
  python3 main.py status            # 查看当前状态
  python3 main.py scan              # 只执行感知层扫描（不发帖）
  python3 main.py scan-sm           # 只执行聪明钱扫描（不发帖）
  python3 main.py build             # 启动灵魂提取访谈
  python3 main.py build-quick       # 快速访谈（每维度3题）
  python3 main.py live              # 启动数字人直播（全自动持续循环）
  python3 main.py live-test         # 数字人直播单次测试
  python3 main.py test-live         # 运行直播模块集成测试
"""

import sys
import os

# 确保模块路径正确
sys.path.insert(0, os.path.dirname(__file__))

from core.orchestrator import Orchestrator
from core.state import load_state
from layers.builder import PersonaBuilder
from layers.perception import run_perception


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "help"

    if cmd == "start":
        agent = Orchestrator()
        agent.start()

    elif cmd == "w2e":
        print("[主程序] 启动 W2E 模式：每30分钟从 W2E 排行榜博主帖子提取内容并发帖")
        print("[主程序] 按 Ctrl+C 停止")
        agent = Orchestrator()
        agent.start_w2e(interval_minutes=30)

    elif cmd == "w2e-once":
        import json
        print("[主程序] W2E 模式单次测试：抓取排行榜博主帖子并改写发帖")
        from w2e_post_generator import W2EPostGenerator
        gen = W2EPostGenerator()
        result = gen.run_once()
        print("\n=== W2E 单次发帖结果 ===")
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif cmd == "smart-money":
        print("[主程序] 🐋 启动聪明钱模式：每15分钟扫描 Hyperliquid 大户持仓并发帖")
        print("[主程序] 按 Ctrl+C 停止")
        agent = Orchestrator()
        agent.start_smart_money(interval_minutes=15)

    elif cmd == "smart-money-once":
        print("[主程序] 🐋 聪明钱模式单次测试：扫描大户持仓并生成发帖")
        agent = Orchestrator()
        agent._self_check()
        success = agent.run_once_smart_money()
        if success:
            print("\n[主程序] ✅ 聪明钱单次发帖成功")
        else:
            print("\n[主程序] ⚠️  聪明钱单次发帖跳过（冷却中或无信号）")

    elif cmd == "once":
        agent = Orchestrator()
        agent._self_check()
        agent._refresh_market(force=True)
        agent.run_once()

    elif cmd == "status":
        agent = Orchestrator()
        agent.status()

    elif cmd == "scan":
        state = load_state()
        ctx = run_perception(state)
        print(f"\n热点共振 Top 10:")
        for item in ctx.get("resonance", [])[:10]:
            print(
                f"  [{item['tier']}] {item['coin']:8s} → "
                f"{item['futures']:12s} 综合热度: {item['score']:.1f}"
            )
        print(f"\n热门叙事 Top 5:")
        for t in ctx.get("topics", [])[:5]:
            print(f"  {t['topic']}: {t['summary'][:60]}")
        # 打印聪明钱信号摘要
        sm = ctx.get("smart_money", {})
        if sm.get("status") == "ok" and sm.get("top_signals"):
            print(f"\n🐋 聪明钱信号 Top 5:")
            for sig in sm["top_signals"][:5]:
                icon = "🟢" if sig.get("net_direction") == "LONG" else (
                    "🔴" if sig.get("net_direction") == "SHORT" else "⚪")
                print(f"  {icon} [{sig.get('confidence', '?')}] {sig.get('signal', '')}")

    elif cmd == "scan-sm":
        print("[主程序] 🐋 执行聪明钱扫描（不发帖）...")
        from smart_money.smart_money_monitor import aggregate_smart_money_signals
        from smart_money.smart_money_monitor import print_signal_report
        signals = aggregate_smart_money_signals()
        if signals:
            print_signal_report(signals)
        else:
            print("[主程序] ⚠️  未获取到聪明钱信号")

    elif cmd == "build":
        builder = PersonaBuilder()
        builder.run_interview(quick_mode=False)

    elif cmd == "build-quick":
        builder = PersonaBuilder()
        builder.run_interview(quick_mode=True)

    elif cmd == "live":
        from live.stream.live_controller import LiveController
        print("[主程序] 启动数字人直播模块（全自动持续循环）...")
        print("[主程序] 按 Ctrl+C 停止直播")
        controller = LiveController()
        controller.start(mock_mode=False)

    elif cmd == "live-test":
        import json
        from live.stream.live_controller import LiveController
        print("[主程序] 启动数字人直播模块（单次测试模式）...")
        controller = LiveController()
        status = controller.run_once()
        print("\n=== 直播测试完成 ===")
        print(json.dumps(status, ensure_ascii=False, indent=2))

    elif cmd == "test-live":
        import subprocess
        print("[主程序] 运行直播模块集成测试...")
        subprocess.run([sys.executable, "test_live_module.py"],
                       cwd=os.path.dirname(__file__))

    else:
        print(__doc__)


if __name__ == "__main__":
    main()
