#!/usr/bin/env python3
"""
hermes_smart_money_script.py
Hermes Cron 执行脚本 - 聪明钱监控模式
每15分钟由 Hermes 调度一次，执行完整的「聪明钱扫描 → 信号生成 → LLM 改写 → 发帖」流程。

链路：
  Hyperliquid 大户持仓扫描
    → 聪明钱信号聚合（smart_money_monitor.py）
    → 信号转内容 Prompt（signal_to_content.py）
    → LLM 生成短贴（content.py ContentGenerator）
    → 配额检查 + 广场发帖（executor.py）
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
    print(f"[聪明钱模式] 启动时间: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*55}")

    try:
        from config.settings import DAILY_LIMIT, FUTURES_MAP
        from core.state import load_state, save_state
        from layers.executor import QuotaController, SquarePoster, execute_post
        from layers.content import ContentGenerator
        from smart_money.smart_money_monitor import aggregate_smart_money_signals, get_cached_signals
        from smart_money.signal_to_content import get_all_signals, build_content_prompt
        import utils.price_sync as price_sync

        state = load_state()

        # 检查今日配额
        daily_count = state.get("daily_count", 0)
        if daily_count >= DAILY_LIMIT:
            print(f"[聪明钱模式] 今日配额已满 ({daily_count}/{DAILY_LIMIT})，跳过本次")
            print("STATUS: QUOTA_FULL")
            return

        print(f"[聪明钱模式] 今日进度: {daily_count}/{DAILY_LIMIT}")

        # ── Step 1: 获取聪明钱信号（优先使用缓存）────────────────────────────
        print("\n[聪明钱模式] Step 1: 获取聪明钱信号...")
        signals_data = get_cached_signals()

        if signals_data:
            print(f"  ✅ 使用缓存信号（{len(signals_data.get('top_signals', []))} 个信号）")
        else:
            print("  🔍 缓存过期，重新扫描大户持仓...")
            signals_data = aggregate_smart_money_signals()

        if not signals_data or not signals_data.get("top_signals"):
            print("[聪明钱模式] ⚠️  未获取到有效聪明钱信号，跳过本次")
            print("STATUS: SKIPPED")
            return

        # ── Step 2: 选择最优信号（配额检查）────────────────────────────────────
        print("\n[聪明钱模式] Step 2: 选择最优信号...")
        quota = QuotaController(state)
        all_signals = get_all_signals()

        if not all_signals:
            print("[聪明钱模式] ⚠️  无有效信号（HIGH/MEDIUM confidence），跳过本次")
            print("STATUS: SKIPPED")
            return

        selected_signal = None
        for sig in all_signals:
            coin = sig["coin"]
            # 检查该代币是否在 FUTURES_MAP 中（确保可以发帖）
            if coin not in FUTURES_MAP:
                print(f"  ⏸  跳过 {coin}：不在期货合约映射表中")
                continue
            ok, reason = quota.can_post(coin)
            if ok:
                selected_signal = sig
                print(f"  ✅ 选中信号: [{sig['type']}] {coin}")
                break
            else:
                print(f"  ⏸  跳过 {coin}: {reason}")

        if not selected_signal:
            print("[聪明钱模式] ⏳ 所有聪明钱代币均在冷却中")
            print("STATUS: SKIPPED")
            return

        # ── Step 3: 发帖前强制同步币安期货价格，并保留 freshness 元数据 ───────────
        selected_signal = price_sync.enrich_signal_price(selected_signal)

        # ── Step 3: 构建 coin_info（与热点模式格式兼容）─────────────────────────
        coin = selected_signal["coin"]
        futures = FUTURES_MAP.get(coin, f"{coin}USDT")
        sig_data = selected_signal.get("data", {})

        # 根据信号类型确定热点等级
        if selected_signal.get("priority", 3) == 1:
            tier = "S"  # HIGH confidence → S 级
        elif selected_signal.get("priority", 3) == 2:
            tier = "A"  # MEDIUM confidence → A 级
        else:
            tier = "B"

        coin_info = {
            "coin": coin,
            "futures": futures,
            "tier": tier,
            "score": sig_data.get("total_size_usd", 0) / 1e6,  # 持仓规模（M）作为得分
            "tw_score": 0,
            "sq_score": 0,
            # 聪明钱专属字段
            "smart_money_signal": selected_signal["type"],
            "whale_count": sig_data.get("whale_count", 0),
            "long_ratio": sig_data.get("long_ratio", 50),
            "net_direction": sig_data.get("net_direction", "NEUTRAL"),
            "total_size_usd": sig_data.get("total_size_usd", 0),
            "mark_px": sig_data.get("mark_px", 0),
            "change_24h": sig_data.get("change_24h", 0),
            "funding_rate": sig_data.get("funding_rate", 0),
        }

        # ── Step 4: 构建聪明钱专属 Prompt ────────────────────────────────────
        print(f"\n[聪明钱模式] Step 3: 生成 [{tier}级] {coin} 聪明钱短贴...")

        # 构建聪明钱 Prompt（通过 signal_to_content 适配器）
        import random
        cta_index = random.randint(0, 4)
        sm_prompt_data = build_content_prompt(selected_signal, cta_index=cta_index)
        coin_info.update(sm_prompt_data.get("coin_info_patch", {}))

        # 构建 context（注入聪明钱信号作为核心上下文）
        content_hints = signals_data.get("content_hints", [])
        context = {
            "raw_tweets": [],
            "hot_posts": [],
            "topics": [],
            "w2e_top_creators": {},
            # 聪明钱专属上下文（注入到 context 中供内容层使用）
            "smart_money_hints": content_hints[:3],
            "smart_money_prompt": sm_prompt_data["prompt"],  # 直接使用聪明钱 Prompt
        }

        # ── Step 5: LLM 生成内容 ────────────────────────────────────────────
        generator = ContentGenerator()

        # 使用聪明钱专属 Prompt 直接调用 LLM（绕过热点模式的 _build_prompt）
        content = generator.generate_from_smart_money_prompt(
            coin_info=coin_info,
            sm_prompt=sm_prompt_data["prompt"],
            cta=sm_prompt_data["cta"],
        )

        # 打印预览
        print(f"\n{'─'*55}")
        print(content)
        print(f"{'─'*55}")

        # ── Step 6: 执行发帖 ────────────────────────────────────────────────
        poster = SquarePoster()
        result = execute_post(coin_info, content, state, quota, poster)

        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        if result.get("success"):
            new_count = state.get("daily_count", daily_count)
            print(f"\n[聪明钱模式] ✅ 发帖成功！今日进度: {new_count}/{DAILY_LIMIT} | 耗时: {elapsed:.1f}s")
            print(f"[聪明钱模式] 信号: {selected_signal['type']} | 代币: {coin} | 方向: {coin_info['net_direction']}")
            print("STATUS: SUCCESS")
        else:
            print(f"\n[聪明钱模式] ⚠️  本次未发帖（配额或冷却）| 耗时: {elapsed:.1f}s")
            print("STATUS: SKIPPED")

    except Exception as e:
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        print(f"\n[聪明钱模式] ❌ 执行异常 | 耗时: {elapsed:.1f}s")
        print(f"错误信息: {e}")
        traceback.print_exc()
        print("STATUS: FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
