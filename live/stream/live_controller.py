#!/usr/bin/env python3
"""
数字人直播模块 — 直播控制器（核心调度器）
功能：全自动直播循环调度
  - 每30分钟刷新市场数据并生成新话术
  - 实时处理弹幕队列，生成 AI 回复
  - 每5分钟自动推送小车
  - 通过 HTTP API 向 LiveTalking 发送播报指令
  - 支持 RTMP 推流到各大直播平台
"""
import os
import time
import json
import threading
import requests
from datetime import datetime
from typing import Optional

from live.engine.market_analyzer import get_full_market_report, load_cached_report
from live.engine.script_generator import generate_full_live_script
from live.engine.danmu_ai import DanmuQueue, generate_danmu_reply, MOCK_DANMUS
from live.cart.cart_manager import CartManager

# ── 配置 ──────────────────────────────────────────────────
LIVETALKING_HOST   = os.getenv("LIVETALKING_HOST", "http://localhost:8010")
MARKET_REFRESH_SEC = int(os.getenv("MARKET_REFRESH_SEC", "1800"))   # 30分钟
SCRIPT_CYCLE_SEC   = int(os.getenv("SCRIPT_CYCLE_SEC", "600"))      # 10分钟一轮
DANMU_POLL_SEC     = float(os.getenv("DANMU_POLL_SEC", "2"))         # 2秒轮询弹幕
CART_PUSH_SEC      = int(os.getenv("CART_PUSH_SEC", "300"))          # 5分钟推小车


class LiveController:
    """全自动直播控制器"""

    def __init__(self):
        self.is_running = False
        self.market_report = {}
        self.scripts = {}
        self.danmu_queue = DanmuQueue()
        self.cart_manager = CartManager()
        self.session_id = f"live_{int(time.time())}"
        self.stats = {
            "start_time": None,
            "total_scripts_sent": 0,
            "total_danmu_replied": 0,
            "total_cart_pushes": 0,
            "market_refreshes": 0,
        }
        self._lock = threading.Lock()

    # ── LiveTalking API 对接 ──────────────────────────────
    def _send_to_avatar(self, text: str, mode: str = "echo") -> bool:
        """
        向 LiveTalking 数字人发送播报指令
        mode: "echo" = 直接播报, "chat" = LLM交互
        """
        try:
            r = requests.post(
                f"{LIVETALKING_HOST}/human",
                json={
                    "text": text,
                    "type": mode,
                    "sessionid": self.session_id,
                },
                timeout=10,
            )
            if r.status_code == 200:
                self.stats["total_scripts_sent"] += 1
                print(f"[直播控制器] ✅ 数字人播报: {text[:50]}...")
                return True
            else:
                print(f"[直播控制器] ⚠️  LiveTalking 返回 {r.status_code}（模拟模式继续）")
        except requests.exceptions.ConnectionError:
            # LiveTalking 未启动时，打印模拟输出
            print(f"[直播控制器] 🎭 [模拟播报] {text[:80]}...")
            self.stats["total_scripts_sent"] += 1
            return True
        except Exception as e:
            print(f"[直播控制器] ❌ 发送失败: {e}")
        return False

    # ── 市场数据刷新 ──────────────────────────────────────
    def refresh_market(self):
        """刷新市场数据和直播脚本"""
        print(f"\n[直播控制器] 🔄 刷新市场数据...")
        with self._lock:
            self.market_report = get_full_market_report()
            self.scripts = generate_full_live_script(
                self.market_report,
                cart_items=self.cart_manager.get_active_cart()
            )
        self.stats["market_refreshes"] += 1
        overview = self.market_report.get("overview", {})
        print(f"[直播控制器] 📊 BTC: ${overview.get('btc_price', 0):,.0f} | 情绪: {overview.get('market_sentiment', 'N/A')}")

    # ── 直播脚本播报循环 ──────────────────────────────────
    def run_script_cycle(self):
        """执行一轮完整的直播脚本"""
        if not self.scripts:
            return

        with self._lock:
            scripts = self.scripts.copy()
            market = self.market_report.copy()

        print(f"\n[直播控制器] 🎬 开始新一轮直播脚本...")

        # 1. 大盘分析
        self._send_to_avatar(scripts.get("market_overview", ""), "echo")
        time.sleep(8)

        # 2. 主流币逐一分析
        for i, coin_script in enumerate(scripts.get("major_coins", [])):
            self._send_to_avatar(coin_script, "echo")
            time.sleep(6)
            # 每2个币推一次小车
            if i % 2 == 1:
                cart_script = self.cart_manager.auto_push()
                if cart_script:
                    self._send_to_avatar(cart_script, "echo")
                    self.stats["total_cart_pushes"] += 1
                    time.sleep(4)

        # 3. 热点推荐
        trending_script = scripts.get("trending_recommendation", "")
        if trending_script:
            self._send_to_avatar(trending_script, "echo")
            time.sleep(8)

        # 4. 小车推送
        cart_script = self.cart_manager.get_push_script()
        self._send_to_avatar(cart_script, "echo")
        self.stats["total_cart_pushes"] += 1
        time.sleep(4)

    # ── 弹幕处理线程 ──────────────────────────────────────
    def _danmu_worker(self):
        """弹幕处理工作线程（后台持续运行）"""
        print("[直播控制器] 🎯 弹幕处理线程启动")
        while self.is_running:
            msg = self.danmu_queue.get_next()
            if msg:
                with self._lock:
                    market = self.market_report.copy()
                reply = generate_danmu_reply(msg["text"], msg["username"], market)
                print(f"[弹幕] {msg['username']}: {msg['text']}")
                print(f"[主播] {reply}")
                self._send_to_avatar(reply, "echo")
                self.stats["total_danmu_replied"] += 1
                time.sleep(3)  # 弹幕回复间隔
            else:
                time.sleep(DANMU_POLL_SEC)

    # ── 模拟弹幕注入（测试用）────────────────────────────
    def inject_mock_danmus(self):
        """注入模拟弹幕（测试弹幕问答功能）"""
        for username, text in MOCK_DANMUS:
            self.danmu_queue.add(username, text)
        print(f"[直播控制器] 注入 {len(MOCK_DANMUS)} 条模拟弹幕")

    # ── 主控制循环 ────────────────────────────────────────
    def start(self, mock_mode: bool = True):
        """启动全自动直播"""
        self.is_running = True
        self.stats["start_time"] = datetime.now().isoformat()
        print(f"\n{'='*60}")
        print(f"  币安广场运营系统智能体 — 数字人直播模块")
        print(f"  会话ID: {self.session_id}")
        print(f"  模式: {'模拟' if mock_mode else '真实直播'}")
        print(f"{'='*60}\n")

        # 1. 初始化市场数据
        self.refresh_market()

        # 2. 开场白
        opening = self.scripts.get("opening", "")
        if opening:
            self._send_to_avatar(opening, "echo")
            time.sleep(5)

        # 3. 启动弹幕处理线程
        danmu_thread = threading.Thread(target=self._danmu_worker, daemon=True)
        danmu_thread.start()

        # 4. 注入测试弹幕
        if mock_mode:
            self.inject_mock_danmus()

        # 5. 主循环
        last_market_refresh = time.time()
        last_script_cycle   = 0

        try:
            while self.is_running:
                now = time.time()

                # 定时刷新市场数据
                if now - last_market_refresh >= MARKET_REFRESH_SEC:
                    self.refresh_market()
                    last_market_refresh = now

                # 定时执行脚本循环
                if now - last_script_cycle >= SCRIPT_CYCLE_SEC:
                    self.run_script_cycle()
                    last_script_cycle = now

                    # 打印状态
                    self._print_status()

                time.sleep(10)

        except KeyboardInterrupt:
            print("\n[直播控制器] 收到停止信号，正在关闭...")
        finally:
            self.stop()

    def stop(self):
        """停止直播"""
        self.is_running = False
        # 播报结束语
        closing = self.scripts.get("closing", "感谢大家今天的陪伴，我们下次再见！")
        self._send_to_avatar(closing, "echo")
        self._print_status()
        print("[直播控制器] 直播已停止")

    def run_once(self) -> dict:
        """单次运行（测试用，不进入循环）"""
        self.refresh_market()

        opening = self.scripts.get("opening", "")
        if opening:
            self._send_to_avatar(opening, "echo")

        self.run_script_cycle()

        # 处理几条模拟弹幕
        self.inject_mock_danmus()
        for _ in range(3):
            msg = self.danmu_queue.get_next()
            if msg:
                reply = generate_danmu_reply(msg["text"], msg["username"], self.market_report)
                print(f"[弹幕] {msg['username']}: {msg['text']}")
                print(f"[主播] {reply}")
                self._send_to_avatar(reply, "echo")
                self.stats["total_danmu_replied"] += 1

        closing = self.scripts.get("closing", "")
        if closing:
            self._send_to_avatar(closing, "echo")

        return self.get_status()

    def _print_status(self):
        """打印当前直播状态"""
        overview = self.market_report.get("overview", {})
        print(f"\n[状态] {datetime.now().strftime('%H:%M:%S')} | "
              f"BTC: ${overview.get('btc_price', 0):,.0f} | "
              f"播报: {self.stats['total_scripts_sent']} | "
              f"弹幕: {self.stats['total_danmu_replied']} | "
              f"小车: {self.stats['total_cart_pushes']}")

    def get_status(self) -> dict:
        """获取直播状态"""
        overview = self.market_report.get("overview", {})
        return {
            "session_id": self.session_id,
            "is_running": self.is_running,
            "btc_price": overview.get("btc_price", 0),
            "market_sentiment": overview.get("market_sentiment", "N/A"),
            "stats": self.stats,
            "cart_status": self.cart_manager.get_status(),
            "danmu_queue_size": self.danmu_queue.size(),
        }


if __name__ == "__main__":
    controller = LiveController()
    status = controller.run_once()
    print(f"\n=== 直播测试完成 ===")
    print(json.dumps(status, ensure_ascii=False, indent=2))
