#!/usr/bin/env python3
"""
Hyperliquid API 接口探测脚本
找到排行榜、持仓、大单等聪明钱相关接口
"""
import requests, json

__test__ = False

BASE = "https://api.hyperliquid.xyz/info"
HEADERS = {"Content-Type": "application/json"}

def post(payload, label=""):
    try:
        r = requests.post(BASE, headers=HEADERS, json=payload, timeout=15)
        status = r.status_code
        if status == 200:
            data = r.json()
            if isinstance(data, list):
                print(f"  ✅ {label}: list[{len(data)}]  sample: {json.dumps(data[0])[:200]}")
            elif isinstance(data, dict):
                print(f"  ✅ {label}: dict  keys={list(data.keys())[:8]}")
            else:
                print(f"  ✅ {label}: {type(data)} = {str(data)[:200]}")
        else:
            print(f"  ❌ {label}: HTTP {status}")
    except Exception as e:
        print(f"  ❌ {label}: {e}")

def main():
    print("\n=== Hyperliquid Info API 接口探测 ===\n")

    # 1. 永续合约元数据（获取所有交易对）
    post({"type": "meta"}, "meta（永续合约交易对列表）")

    # 2. 永续合约资产上下文（价格、资金费率、OI）
    post({"type": "metaAndAssetCtxs"}, "metaAndAssetCtxs（价格+OI+资金费率）")

    # 3. 排行榜（尝试多种参数格式）
    post({"type": "leaderboard"}, "leaderboard（无参）")
    post({"type": "leaderboard", "window": "day"}, "leaderboard（window=day）")
    post({"type": "leaderboard", "window": "week"}, "leaderboard（window=week）")
    post({"type": "leaderboard", "window": "allTime"}, "leaderboard（window=allTime）")

    # 4. 查询特定地址的持仓（用已知大户地址测试）
    known_whale = "0xc6ab9ee8ad3647a12242a2afa43152be796f3391"
    post({"type": "clearinghouseState", "user": known_whale}, f"clearinghouseState（鲸鱼地址持仓）")

    # 5. 查询地址的历史成交
    post({"type": "userFills", "user": known_whale}, "userFills（历史成交）")
    post({"type": "userFills", "user": known_whale, "aggregateByTime": True}, "userFills（聚合）")

    # 6. 全局大单/清算数据
    post({"type": "recentTrades", "coin": "BTC"}, "recentTrades（BTC最新成交）")
    post({"type": "l2Book", "coin": "BTC"}, "l2Book（BTC盘口）")

    # 7. 资金费率
    post({"type": "fundingHistory", "coin": "BTC", "startTime": 1713456000000}, "fundingHistory（BTC资金费率历史）")

    # 8. 开放利率（OI）
    post({"type": "openInterest"}, "openInterest")

    # 9. 前100名持仓者（大户持仓分布）
    post({"type": "topTraders"}, "topTraders")
    post({"type": "richList"}, "richList")

    print("\n=== 探测完成 ===\n")


if __name__ == "__main__":
    main()
