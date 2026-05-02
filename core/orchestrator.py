"""
总控编排器（Orchestrator）
===========================
负责协调四层技能的完整工作流：
  感知层 → 内容层 → 执行层 → 循环

工作流循环：
  1. 自检：验证配置与账号状态
  2. 感知：双端热点扫描（每20分钟刷新）
  3. 决策：选择最优热点代币
  4. 合成：LLM 生成高转化短贴
  5. 执行：配额检查 + 广场发帖
  6. 等待：20分钟间隔后重复

UI 控制接口：
  start()    → 启动自动发帖循环
  stop()     → 优雅停止
  status()   → 打印当前状态
  run_once() → 执行单次发帖
"""

import time
import signal
import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.settings import DAILY_LIMIT, SCAN_INTERVAL_M, SQUARE_API_KEY
from core.state import load_state, save_state, get_status_summary, get_status_payload, update_state
from layers.perception import run_perception, load_market_context
from layers.content import ContentGenerator
from layers.executor import QuotaController, SquarePoster, execute_post
import utils.price_sync as price_sync


class Orchestrator:
    """
    币安广场运营系统智能体总控编排器。
    """

    def _reload_state(self):
        """从磁盘重新加载状态，确保长循环中的配额/日期/封禁状态是最新的。"""
        self.state = load_state()
        return self.state

    def __init__(self):
        self.state      = load_state()
        self.generator  = ContentGenerator()
        self.poster     = SquarePoster()
        self._running   = False
        self._market    = {}
        self._market_ts = 0

        # 注册优雅退出信号
        signal.signal(signal.SIGINT,  self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)

    def _handle_stop(self, signum, frame):
        print("\n[编排器] 收到停止信号，正在优雅退出...")
        self._running = False

    # ──────────────────────────────────────────
    # 步骤1：自检
    # ──────────────────────────────────────────
    def _self_check(self) -> bool:
        """检查系统配置是否就绪。"""
        self._reload_state()
        print("\n[编排器] ── 自检 ──")
        issues = []
        if not SQUARE_API_KEY:
            issues.append("⚠️  SQUARE_API_KEY 未配置（将以模拟模式运行）")
        if self.state.get("status") == "BANNED":
            issues.append("🚨 账号状态为 BANNED，请先处理封禁问题")
            return False
        for issue in issues:
            print(f"  {issue}")
        print(f"  ✅ 自检完成")
        return True

    # ──────────────────────────────────────────
    # 步骤2：感知（带缓存）
    # ──────────────────────────────────────────
    def _refresh_market(self, force: bool = False):
        """刷新市场热点数据（超过 SCAN_INTERVAL_M 分钟自动刷新）。"""
        now = time.time()
        if not force and self._market and (now - self._market_ts) < SCAN_INTERVAL_M * 60:
            return
        self._market    = run_perception(self.state)
        self._market_ts = now

    # ──────────────────────────────────────────
    # 步骤3：决策 — 选择最优代币
    # ──────────────────────────────────────────
    def _select_coin(self):
        """
        从共振列表中选择满足冷却条件的最高热度代币。
        """
        quota     = QuotaController(self.state)
        resonance = self._market.get("resonance", [])

        for item in resonance:
            ok, reason = quota.can_post(item["coin"])
            if ok:
                return item
            else:
                print(f"  [编排器] ⏸  跳过 {item['coin']}: {reason}")
        return None

    # ──────────────────────────────────────────
    # 核心：单次发帖循环
    # ──────────────────────────────────────────
    def run_once(self) -> bool:
        """
        执行一次完整的"感知→决策→生成→发帖"循环。
        返回 True 表示成功发帖。
        """
        self._reload_state()

        # 刷新市场数据
        self._refresh_market()

        if not self._market.get("resonance"):
            print("[编排器] ⚠️  未获取到热点数据，跳过本次")
            return False

        # 选择代币
        coin_info = self._select_coin()
        if not coin_info:
            print("[编排器] ⏳ 所有热点代币均在冷却中")
            return False

        # ── 权威价格获取（期货优先 → 现货降级）──
        _coin = coin_info["coin"]
        _ap = price_sync.get_authoritative_price(_coin)

        # 模式A：有价格 → 纯数据行情帖（不走LLM）
        if _ap:
            coin_info["mark_px"] = _ap["price"]
            coin_info["change_24h"] = _ap["change_24h"]
            coin_info["high_24h"] = _ap["high_24h"]
            coin_info["low_24h"] = _ap["low_24h"]
            coin_info["_price_synced"] = True
            coin_info["_price_source"] = _ap["_source"]
            coin_info["source"] = _ap["_source"]
            coin_info["_price_ts"] = _ap.get("ts")
            coin_info["is_live"] = _ap.get("is_live")
            coin_info.pop("warning_reason", None)

            source_label = "期货" if "futures" in _ap["_source"] else "现货"
            print(f"[编排器] 💹 {_coin} {source_label}实时价格: ${_ap['price']:,.4f} ({_ap['change_24h']:+.2f}%)")

            print(f"\n[编排器] 📊 模式A — 行情数据帖 [{coin_info['tier']}级] {_coin}")
            from layers.content import make_data_post
            content = make_data_post(_coin, _ap)
        else:
            # 模式B：无价格 → 纯走势分析帖（不走LLM，不出精确价格）
            coin_info["_price_synced"] = False
            coin_info["_price_source"] = "binance_futures"
            coin_info["source"] = coin_info.get("source", "binance_futures")
            coin_info["_price_ts"] = None
            coin_info["is_live"] = False
            coin_info["warning_reason"] = "no_binance_price"

            print(f"\n[编排器] 📊 模式B — 走势分析帖 [{coin_info['tier']}级] {_coin}（价格获取失败）")
            from layers.content import make_analysis_post
            content = make_analysis_post(_coin, coin_info.get("tier", "B"))

        # 打印预览
        print(f"\n{'─'*55}")
        print(content)
        print(f"{'─'*55}")

        # 执行发帖
        quota  = QuotaController(self.state)
        result = execute_post(coin_info, content, self.state, quota, self.poster)
        return result.get("success", False)

    # ──────────────────────────────────────────
    # 每日全自动模式
    # ──────────────────────────────────────────
    def start(self):
        """
        启动每日全自动发帖循环，直到配额耗尽或手动停止。
        """
        if not self._self_check():
            return

        self._running = True
        self.state = update_state(lambda current: {**current, "status": current.get("status") if str(current.get("status", "")).upper() == "BANNED" else "running"})

        print(f"\n{'═'*55}")
        print(f"[编排器] 🚀 币安广场运营系统智能体已启动")
        print(f"  目标: {DAILY_LIMIT} 贴/天 | 今日已发: {self.state['daily_count']}")
        print(f"{'═'*55}")

        while self._running:
            self._reload_state()

            # 每日配额检查
            if self.state["daily_count"] >= DAILY_LIMIT:
                print(f"\n[编排器] 🎉 今日 {DAILY_LIMIT} 贴配额已完成！")
                break

            # 执行单次发帖
            self.run_once()
            self._reload_state()

            if not self._running:
                break

            # 计算等待时间
            quota = QuotaController(self.state)
            wait_sec = quota.next_wait_seconds()
            remaining = DAILY_LIMIT - self.state["daily_count"]
            print(
                f"\n[编排器] 📋 今日进度: {self.state['daily_count']}/{DAILY_LIMIT} "
                f"| 剩余: {remaining} 贴 | 下次发帖: {wait_sec/60:.1f} 分钟后"
            )
            time.sleep(wait_sec)

        self._running = False
        self.state = update_state(lambda current: {**current, "status": current.get("status") if str(current.get("status", "")).upper() == "BANNED" else "idle"})
        print("\n[编排器] 系统已停止。")

    # ──────────────────────────────────────────
    # W2E 模式：从排行榜博主帖子提取内容并发布
    # ──────────────────────────────────────────
    def run_once_w2e(self) -> bool:
        """
        执行一次 W2E 模式发帖：
        从 W2E 排行榜博主帖子中提取素材 → LLM 改写 → 发布到广场。
        返回 True 表示成功发帖。
        """
        from w2e_post_generator import W2EPostGenerator
        gen = W2EPostGenerator()
        result = gen.run_once()
        return result.get("success", False)

    def start_w2e(self, interval_minutes: int = 20):
        """
        启动 W2E 模式全自动循环：
        每 interval_minutes 分钟从 W2E 排行榜博主帖子中提取内容，
        经 LLM 改写后发布到币安广场，直到每日配额耗尽或手动停止。
        """
        if not self._self_check():
            return

        self._running = True
        self.state = update_state(lambda current: {**current, "status": current.get("status") if str(current.get("status", "")).upper() == "BANNED" else "running"})

        print(f"\n{'═'*55}")
        print(f"[编排器] 🚀 W2E 模式已启动")
        print(f"  策略: 抓取排行榜前10博主帖子 → LLM 改写 → 发布")
        print(f"  频率: 每 {interval_minutes} 分钟发帖一次")
        print(f"  目标: {DAILY_LIMIT} 贴/天 | 今日已发: {self.state['daily_count']}")
        print(f"{'═'*55}")

        while self._running:
            self._reload_state()

            # 每日配额检查
            if self.state["daily_count"] >= DAILY_LIMIT:
                print(f"\n[编排器] 🎉 今日 {DAILY_LIMIT} 贴配额已完成！")
                break

            # 执行单次 W2E 发帖
            self.run_once_w2e()
            # 重新加载状态（W2EPostGenerator 内部会更新）
            self._reload_state()

            if not self._running:
                break

            remaining = DAILY_LIMIT - self.state["daily_count"]
            print(
                f"\n[编排器-W2E] 📋 今日进度: {self.state['daily_count']}/{DAILY_LIMIT} "
                f"| 剩余: {remaining} 贴 | 下次发帖: {interval_minutes} 分钟后"
            )
            time.sleep(interval_minutes * 60)

        self._running = False
        self.state = update_state(lambda current: {**current, "status": current.get("status") if str(current.get("status", "")).upper() == "BANNED" else "idle"})
        print("\n[编排器] W2E 模式已停止。")

    def stop(self):
        """优雅停止自动发帖循环。"""
        self._running = False
        self.state = update_state(lambda current: {**current, "status": current.get("status") if str(current.get("status", "")).upper() == "BANNED" else "idle"})
        print("[编排器] 已发送停止信号。")

    def status(self):
        """打印当前运行状态。"""
        self.state = load_state()
        payload = get_status_payload(self.state)
        print(f"\n{'═'*45}")
        print(f"[状态] {get_status_summary(self.state)}")
        print(f"[状态] 运行模式: {'模拟' if not SQUARE_API_KEY else '真实'}")
        print(
            f"[状态] 安全: {'熔断(BANNED)' if payload['is_banned'] else '正常'}"
            f" | guest_token: {'有' if payload['has_guest_token'] else '无'}({payload['guest_token_age_text']})"
            f" | 待发布意图: {'有' if payload['has_posting_intent'] else '无'}"
        )
        if payload["has_posting_intent"]:
            intent = payload["posting_intent"] or {}
            print(
                f"[状态] 待发布详情: coin={intent.get('coin', '未知')}"
                f" | source={intent.get('source', '未知')}"
                f" | age={payload['posting_intent_age_text']}"
                f" | mock={bool(intent.get('mock', False))}"
            )
        print(
            f"[状态] 运行工件: live_script={'fresh' if payload['live_script']['fresh'] else ('stale' if payload['live_script']['exists'] else 'missing')}"
            f"({payload['live_script']['timestamp_text']}, age={payload['live_script']['age_text']})"
            f" | live_market_report={'fresh' if payload['live_market_report']['fresh'] else ('stale' if payload['live_market_report']['exists'] else 'missing')}"
            f"({payload['live_market_report']['timestamp_text']}, age={payload['live_market_report']['age_text']})"
        )
        print(
            f"[状态] 运行缓存: 去重键={payload['recent_post_key_count']}"
            f" | 冷却币种={payload['coin_cooldown_count']}"
            f" | 最近发帖距今={payload['last_post_age_text']}"
        )
        print(f"[状态] 各币冷却情况:")
        now = time.time()
        coin_last_post = self.state.get("coin_last_post", {})
        if not coin_last_post:
            print("  无冷却记录")
        for coin, ts in coin_last_post.items():
            elapsed_h = (now - ts) / 3600
            from config.settings import COIN_COOLDOWN_H
            if elapsed_h < COIN_COOLDOWN_H:
                print(f"  {coin}: 冷却中 (还需 {COIN_COOLDOWN_H - elapsed_h:.1f}h)")
            else:
                print(f"  {coin}: 可发帖 ✓")
        print(f"{'═'*45}\n")

    def status_json(self):
        """输出机器可读的 JSON 状态载荷。"""
        self.state = load_state()
        payload = get_status_payload(self.state)
        print(json.dumps(payload, ensure_ascii=False, indent=2))

    # ──────────────────────────────────────────
    # 聪明钱模式：Hyperliquid 大户持仓信号发帖
    # ──────────────────────────────────────────
    def run_once_smart_money(self) -> bool:
        """
        执行一次聪明钱模式发帖：
        聪明钱信号扫描 → 选择最优信号 → LLM 生成 → 发布到广场。
        返回 True 表示成功发帖。
        """
        self._reload_state()
        from config.settings import FUTURES_MAP
        from smart_money.smart_money_monitor import aggregate_smart_money_signals, get_cached_signals
        from smart_money.signal_to_content import get_all_signals, build_content_prompt
        import random

        print("\n[编排器-聪明钱] ── 开始聪明钱信号扫描 ──")

        # 获取信号（优先使用缓存）
        signals_data = get_cached_signals()
        if not signals_data:
            print("[编排器-聪明钱] 缓存过期，重新扫描...")
            signals_data = aggregate_smart_money_signals()

        if not signals_data or not signals_data.get("top_signals"):
            print("[编排器-聪明钱] ⚠️  未获取到有效聪明钱信号，跳过本次")
            return False

        # 选择最优信号（配额检查）
        quota = QuotaController(self.state)
        all_signals = get_all_signals()

        if not all_signals:
            print("[编排器-聪明钱] ⚠️  无 HIGH/MEDIUM 置信度信号，跳过本次")
            return False

        selected_signal = None
        for sig in all_signals:
            coin = sig["coin"]
            if coin not in FUTURES_MAP:
                continue
            ok, reason = quota.can_post(coin)
            if ok:
                selected_signal = sig
                print(f"[编排器-聪明钱] ✅ 选中信号: [{sig['type']}] {coin}")
                break
            else:
                print(f"[编排器-聪明钱] ⏸  跳过 {coin}: {reason}")

        if not selected_signal:
            print("[编排器-聪明钱] ⏳ 所有聪明钱代币均在冷却中")
            return False

        # 构建 coin_info
        coin = selected_signal["coin"]
        futures = FUTURES_MAP.get(coin, f"{coin}USDT")
        sig_data = selected_signal.get("data", {})
        tier = "S" if selected_signal.get("priority", 3) == 1 else (
            "A" if selected_signal.get("priority", 3) == 2 else "B"
        )

        # ── 权威价格获取（期货优先 → 现货降级）──
        _ap = price_sync.get_authoritative_price(coin)
        if _ap:
            realtime_px = _ap["price"]
            realtime_chg = _ap["change_24h"]
            source_label = "期货" if "futures" in _ap["_source"] else "现货"
            print(f"[编排器-聪明钱] 💹 {coin} {source_label}实时价格: ${realtime_px:,.4f} ({realtime_chg:+.2f}%)")
        else:
            realtime_px = sig_data.get("mark_px", 0)
            realtime_chg = sig_data.get("change_24h", 0)
            print(f"[编排器-聪明钱] ⚠️  {coin} 所有币安价格API均失败，使用 Hyperliquid 价格")

        coin_info = {
            "coin": coin,
            "futures": futures,
            "tier": tier,
            "score": sig_data.get("total_size_usd", 0) / 1e6,
            "tw_score": 0,
            "sq_score": 0,
            "smart_money_signal": selected_signal["type"],
            "whale_count": sig_data.get("whale_count", 0),
            "long_ratio": sig_data.get("long_ratio", 50),
            "net_direction": sig_data.get("net_direction", "NEUTRAL"),
            "total_size_usd": sig_data.get("total_size_usd", 0),
            "mark_px": realtime_px,       # 权威价格
            "change_24h": realtime_chg,   # 权威涨跌幅
            "funding_rate": sig_data.get("funding_rate", 0),
        }

        # 双模式：有价格 → 数据帖；无价格 → LLM 聪明钱信号帖
        if _ap:
            print(f"\n[编排器-聪明钱] 📊 模式A — 行情数据帖 [{tier}级] {coin}")
            from layers.content import make_data_post
            content = make_data_post(coin, _ap)
        else:
            print(f"\n[编排器-聪明钱] ✍️  模式B — LLM信号帖 [{tier}级] {coin}（价格获取失败）")
            cta_index = random.randint(0, 4)
            sm_prompt_data = build_content_prompt(selected_signal, cta_index=cta_index)
            content = self.generator.generate_from_smart_money_prompt(
                coin_info=coin_info,
                sm_prompt=sm_prompt_data["prompt"],
                cta=sm_prompt_data["cta"],
            )

        # 打印预览
        print(f"\n{'─'*55}")
        print(content)
        print(f"{'─'*55}")

        # 执行发帖
        result = execute_post(coin_info, content, self.state, quota, self.poster)
        return result.get("success", False)

    def start_smart_money(self, interval_minutes: int = 15):
        """
        启动聪明钱模式全自动循环：
        每 interval_minutes 分钟扫描一次 Hyperliquid 大户持仓，
        发现高置信度信号后生成并发布到币安广场，直到每日配额耗尽或手动停止。
        """
        if not self._self_check():
            return
        self._running = True
        self.state = update_state(lambda current: {**current, "status": current.get("status") if str(current.get("status", "")).upper() == "BANNED" else "running"})
        print(f"\n{'═'*55}")
        print(f"[编排器] 🐋 聪明钱模式已启动")
        print(f"  策略: Hyperliquid 大户持仓扫描 → 信号生成 → LLM 改写 → 发布")
        print(f"  频率: 每 {interval_minutes} 分钟扫描一次")
        print(f"  目标: {DAILY_LIMIT} 贴/天 | 今日已发: {self.state['daily_count']}")
        print(f"{'═'*55}")
        while self._running:
            self._reload_state()
            if self.state["daily_count"] >= DAILY_LIMIT:
                print(f"\n[编排器] 🎉 今日 {DAILY_LIMIT} 贴配额已完成！")
                break
            self.run_once_smart_money()
            self._reload_state()
            if not self._running:
                break
            remaining = DAILY_LIMIT - self.state["daily_count"]
            print(
                f"\n[编排器-聪明钱] 📋 今日进度: {self.state['daily_count']}/{DAILY_LIMIT} "
                f"| 剩余: {remaining} 贴 | 下次扫描: {interval_minutes} 分钟后"
            )
            time.sleep(interval_minutes * 60)
        self._running = False
        self.state = update_state(lambda current: {**current, "status": current.get("status") if str(current.get("status", "")).upper() == "BANNED" else "idle"})
        print("\n[编排器] 聪明钱模式已停止。")
