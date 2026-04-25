#!/usr/bin/env python3
"""
hermes_hotspot_script.py
Hermes Cron 执行脚本 - 热点扫描标准模式
每14分钟由 Hermes 调度一次，执行完整的「热点扫描 → LLM 生成 → 发帖」流程。
"""
import os
import sys
import json
import traceback
from datetime import datetime, timezone

# ── 切换到项目目录 ────────────────────────────────────────────────────────────
PROJECT_DIR = "/root/binance-square-agent"
sys.path.insert(0, PROJECT_DIR)
os.chdir(PROJECT_DIR)

# ── 加载 .env 环境变量 ────────────────────────────────────────────────────────
env_path = os.path.join(PROJECT_DIR, ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

# ── 执行单次热点发帖 ──────────────────────────────────────────────────────────
def main():
    start_time = datetime.now(timezone.utc)
    print(f"\n{'='*55}")
    print(f"[热点模式] 启动时间: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*55}")

    try:
        from core.orchestrator import Orchestrator

        agent = Orchestrator()

        # 检查今日配额
        daily_count = agent.state.get("daily_count", 0)
        from config.settings import DAILY_LIMIT
        if daily_count >= DAILY_LIMIT:
            print(f"[热点模式] 今日配额已满 ({daily_count}/{DAILY_LIMIT})，跳过本次")
            print("STATUS: QUOTA_FULL")
            return

        print(f"[热点模式] 今日进度: {daily_count}/{DAILY_LIMIT}")

        # 执行单次发帖
        success = agent.run_once()

        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        if success:
            new_count = agent.state.get("daily_count", daily_count)
            print(f"\n[热点模式] ✅ 发帖成功！今日进度: {new_count}/{DAILY_LIMIT} | 耗时: {elapsed:.1f}s")
            print("STATUS: SUCCESS")
        else:
            print(f"\n[热点模式] ⚠️  本次未发帖（热点冷却或无数据）| 耗时: {elapsed:.1f}s")
            print("STATUS: SKIPPED")

    except Exception as e:
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        print(f"\n[热点模式] ❌ 执行异常 | 耗时: {elapsed:.1f}s")
        print(f"错误信息: {e}")
        traceback.print_exc()
        print("STATUS: FAILED")
        sys.exit(1)

if __name__ == "__main__":
    main()
