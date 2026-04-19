#!/usr/bin/env python3
"""
币安广场运营系统智能体 — 主程序入口
======================================
用法：
  python3 main.py start        # 启动全自动发帖循环
  python3 main.py once         # 执行单次发帖
  python3 main.py status       # 查看当前状态
  python3 main.py scan         # 只执行感知层扫描（不发帖）
  python3 main.py build        # 启动灵魂提取访谈
  python3 main.py build-quick  # 快速访谈（每维度3题）
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

    elif cmd == "build":
        builder = PersonaBuilder()
        builder.run_interview(quick_mode=False)

    elif cmd == "build-quick":
        builder = PersonaBuilder()
        builder.run_interview(quick_mode=True)

    else:
        print(__doc__)


if __name__ == "__main__":
    main()
