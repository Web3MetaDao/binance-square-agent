"""
总控编排器（Orchestrator）
===========================
负责协调四层技能的完整工作流：
  感知层 → 内容层 → 执行层 → 循环

工作流循环：
  1. 自检：验证配置与账号状态
  2. 感知：双端热点扫描（每30分钟刷新）
  3. 决策：选择最优热点代币
  4. 合成：LLM 生成高转化短贴
  5. 执行：配额检查 + 广场发帖
  6. 等待：14分钟间隔后重复

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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.settings import DAILY_LIMIT, SCAN_INTERVAL_M, SQUARE_API_KEY
from core.state import load_state, save_state, get_status_summary
from layers.perception import run_perception, load_market_context
from layers.content import ContentGenerator
from layers.executor import QuotaController, SquarePoster, execute_post


class Orchestrator:
    """
    币安广场运营系统智能体总控编排器。
    """

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

        # 生成内容
        print(f"\n[编排器] ✍️  生成 [{coin_info['tier']}级] {coin_info['coin']} 短贴...")
        context = {
            "raw_tweets": self._market.get("raw_tweets", []),
            "hot_posts":  self._market.get("hot_posts", []),
            "topics":     self._market.get("topics", []),
        }
        content = self.generator.generate(coin_info, context)

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
        self.state["status"] = "running"
        save_state(self.state)

        print(f"\n{'═'*55}")
        print(f"[编排器] 🚀 币安广场运营系统智能体已启动")
        print(f"  目标: {DAILY_LIMIT} 贴/天 | 今日已发: {self.state['daily_count']}")
        print(f"{'═'*55}")

        while self._running:
            # 每日配额检查
            if self.state["daily_count"] >= DAILY_LIMIT:
                print(f"\n[编排器] 🎉 今日 {DAILY_LIMIT} 贴配额已完成！")
                break

            # 执行单次发帖
            self.run_once()

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
        self.state["status"] = "idle"
        save_state(self.state)
        print("\n[编排器] 系统已停止。")

    def stop(self):
        """优雅停止自动发帖循环。"""
        self._running = False
        self.state["status"] = "idle"
        save_state(self.state)
        print("[编排器] 已发送停止信号。")

    def status(self):
        """打印当前运行状态。"""
        self.state = load_state()
        print(f"\n{'═'*45}")
        print(f"[状态] {get_status_summary(self.state)}")
        print(f"[状态] 运行模式: {'模拟' if not SQUARE_API_KEY else '真实'}")
        print(f"[状态] 各币冷却情况:")
        now = time.time()
        for coin, ts in self.state.get("coin_last_post", {}).items():
            elapsed_h = (now - ts) / 3600
            from config.settings import COIN_COOLDOWN_H
            if elapsed_h < COIN_COOLDOWN_H:
                print(f"  {coin}: 冷却中 (还需 {COIN_COOLDOWN_H - elapsed_h:.1f}h)")
            else:
                print(f"  {coin}: 可发帖 ✓")
        print(f"{'═'*45}\n")
