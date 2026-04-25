#!/usr/bin/env python3
"""
数字人直播模块 — 直播小车管理器
功能：自动挂载直播小车（内容挖矿引导），根据当前热点自动切换推荐内容
"""
import os
import json
import time
from datetime import datetime
from typing import Optional

WRITE_TO_EARN_URL = "https://www.binance.com/zh-CN/square/write-to-earn"

# ── 小车商品库（可扩展）────────────────────────────────────
# 每个商品对应一个推广场景
CART_PRODUCTS = [
    {
        "id": "write_to_earn",
        "name": "币安广场内容挖矿（Write to Earn）",
        "type": "earn",
        "link": WRITE_TO_EARN_URL,
        "trigger_keywords": ["内容挖矿", "写作赚钱", "广场收益", "cashtag", "返佣", "怎么赚"],
        "push_script": "老铁们！点击帖子里的 $BTC、$ETH 等 cashtag 标签参与交易，就能给创作者贡献挖矿收益，创作者最高可获得50%手续费分成！",
        "priority": 1,
    },
    {
        "id": "btc_contract",
        "name": "BTC合约交易（新手教程）",
        "type": "educational",
        "link": "https://www.binance.com/zh-CN/futures/BTCUSDT",
        "trigger_keywords": ["BTC", "比特币", "合约", "做多", "做空"],
        "push_script": "想参与BTC合约的老铁，点击小车里的BTC合约教程，新手必看！记得控制仓位！",
        "priority": 2,
    },
    {
        "id": "eth_contract",
        "name": "ETH合约交易",
        "type": "product",
        "link": "https://www.binance.com/zh-CN/futures/ETHUSDT",
        "trigger_keywords": ["ETH", "以太坊"],
        "push_script": "ETH合约在小车里，点击直达！注意风险管理！",
        "priority": 3,
    },
    {
        "id": "spot_trading",
        "name": "币安现货交易（零手续费活动）",
        "type": "promotion",
        "link": "https://www.binance.com/zh-CN/trade/BTC_USDT",
        "trigger_keywords": ["现货", "买币", "购买"],
        "push_script": "现货交易的老铁，小车里有零手续费活动链接，现在进场划算！",
        "priority": 4,
    },
]


class CartManager:
    """直播小车管理器"""

    def __init__(self):
        self.active_items = []
        self.push_history = []
        self.last_push_time = 0
        self.push_interval = 300  # 每5分钟推一次小车

    def get_active_cart(self) -> list:
        """获取当前激活的小车商品列表"""
        # 默认挂载内容挖矿引导（最高优先级）
        return CART_PRODUCTS

    def should_push_cart(self) -> bool:
        """判断是否应该推送小车"""
        now = time.time()
        return (now - self.last_push_time) >= self.push_interval

    def get_push_script(self, context_keywords: list = None) -> str:
        """根据当前直播内容选择最匹配的小车推送话术"""
        if not context_keywords:
            # 默认推内容挖矿引导
            return CART_PRODUCTS[0]["push_script"]

        # 根据关键词匹配最相关的商品
        best_match = None
        best_score = 0
        for product in CART_PRODUCTS:
            score = sum(1 for kw in context_keywords if kw in product["trigger_keywords"])
            if score > best_score:
                best_score = score
                best_match = product

        if best_match:
            return best_match["push_script"]
        return CART_PRODUCTS[0]["push_script"]

    def record_push(self, script: str):
        """记录小车推送"""
        self.last_push_time = time.time()
        self.push_history.append({
            "ts": datetime.now().isoformat(),
            "script": script[:50] + "..."
        })
        print(f"[小车管理器] 推送小车: {script[:60]}...")

    def auto_push(self, context_keywords: list = None) -> Optional[str]:
        """自动推送小车（如果到时间了）"""
        if self.should_push_cart():
            script = self.get_push_script(context_keywords)
            self.record_push(script)
            return script
        return None

    def get_status(self) -> dict:
        """获取小车状态"""
        return {
            "active_items": len(self.get_active_cart()),
            "push_count": len(self.push_history),
            "last_push": self.push_history[-1]["ts"] if self.push_history else "无",
            "next_push_in": max(0, self.push_interval - (time.time() - self.last_push_time)),
        }


def generate_cart_config(trending_coins: list) -> dict:
    """根据热点代币动态生成小车配置"""
    config = {
        "base_items": [CART_PRODUCTS[0]],  # 内容挖矿引导始终挂载
        "trending_items": [],
        "generated_at": datetime.now().isoformat(),
    }

    # 为热点代币添加期货合约链接
    for coin in trending_coins[:3]:
        symbol = coin.get("symbol", "")
        if symbol:
            config["trending_items"].append({
                "id": f"{symbol.lower()}_futures",
                "name": f"{symbol} 合约交易",
                "type": "futures",
                "link": f"https://www.binance.com/zh-CN/futures/{symbol}USDT",
                "push_script": f"热点来了！{symbol}合约在小车里，点击直达，注意风控！",
                "priority": 2,
            })

    return config


if __name__ == "__main__":
    cart = CartManager()
    print("=== 小车管理器测试 ===")
    print(f"激活商品数: {len(cart.get_active_cart())}")
    for item in cart.get_active_cart():
        print(f"  [{item['priority']}] {item['name']}")

    # 模拟推送
    script = cart.auto_push(["BTC", "合约"])
    print(f"\n推送话术: {script}")
    print(f"状态: {cart.get_status()}")
