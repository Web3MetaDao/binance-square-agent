#!/usr/bin/env python3
"""
hermes_leaderboard_update_script.py
Hermes Cron 执行脚本 - Hyperliquid 排行榜每周自动更新
每周一 UTC 00:00 由 Hermes 调度一次，自动抓取 Hyperliquid 排行榜 Top 20 地址，
更新 address_updater.py 的 SEED_ADDRESSES，确保聪明钱地址库保持最新。

链路：
  Playwright 渲染 Hyperliquid 排行榜页面
    → 提取 Top 20 地址 + PnL 数据
    → 更新 address_updater.py SEED_ADDRESSES
    → 保存排行榜缓存（leaderboard_cache.json）
    → 推送更新到 GitHub（可选）
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


def main():
    start_time = datetime.now(timezone.utc)
    print(f"\n{'='*55}")
    print(f"[排行榜更新] 启动时间: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*55}")

    try:
        from smart_money.leaderboard_auto_update import (
            run_weekly_update,
            check_cache_valid,
        )

        # 检查是否需要更新（7天内已更新则跳过）
        valid, cache = check_cache_valid()
        if valid:
            import time
            age_days = (time.time() - cache.get("updated_timestamp", 0)) / 86400
            print(f"[排行榜更新] ✅ 7天内已更新（{age_days:.1f}天前），跳过本次")
            print("STATUS: SKIPPED")
            return

        print("[排行榜更新] 开始执行每周排行榜更新...")
        result = run_weekly_update(force=True)  # 强制更新（已通过缓存检查）

        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()

        status = result.get("status", "unknown")
        if status == "success":
            count = result.get("count", 0)
            print(f"\n[排行榜更新] ✅ 更新成功！共更新 {count} 个地址 | 耗时: {elapsed:.1f}s")
            print("STATUS: SUCCESS")
        elif status == "cached":
            print(f"\n[排行榜更新] ✅ 缓存有效，跳过更新 | 耗时: {elapsed:.1f}s")
            print("STATUS: SKIPPED")
        else:
            print(f"\n[排行榜更新] ⚠️  更新失败或无新数据 | 耗时: {elapsed:.1f}s")
            print(f"原因: {result.get('message', '未知')}")
            print("STATUS: FAILED")
            sys.exit(1)

    except Exception as e:
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        print(f"\n[排行榜更新] ❌ 执行异常 | 耗时: {elapsed:.1f}s")
        print(f"错误信息: {e}")
        traceback.print_exc()
        print("STATUS: FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
