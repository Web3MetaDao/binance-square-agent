#!/usr/bin/env python3
"""
深度测试 Hyperliquid 排行榜与大户数据接口
"""
import requests, json, time

BASE = "https://api.hyperliquid.xyz/info"
HEADERS = {"Content-Type": "application/json"}

def post(payload, label=""):
    try:
        r = requests.post(BASE, headers=HEADERS, json=payload, timeout=15)
        status = r.status_code
        if status == 200:
            data = r.json()
            if isinstance(data, list):
                print(f"  ✅ {label}: list[{len(data)}]")
                print(f"     sample: {json.dumps(data[0])[:300]}")
            elif isinstance(data, dict):
                print(f"  ✅ {label}: dict keys={list(data.keys())}")
                print(f"     {json.dumps(data)[:300]}")
        else:
            print(f"  ❌ {label}: HTTP {status} → {r.text[:100]}")
    except Exception as e:
        print(f"  ❌ {label}: {e}")

print("\n=== 测试1: 排行榜接口变体 ===")
# 尝试 Hyperliquid 前端使用的排行榜接口
post({"type": "leaderboard", "req": {"timeWindow": "day"}}, "leaderboard req.timeWindow=day")
post({"type": "leaderboard", "req": {"timeWindow": "week"}}, "leaderboard req.timeWindow=week")
post({"type": "leaderboard", "req": {"timeWindow": "allTime"}}, "leaderboard req.timeWindow=allTime")
post({"type": "leaderboard", "timeWindow": "day"}, "leaderboard timeWindow=day")

print("\n=== 测试2: 抓取 metaAndAssetCtxs 获取 OI 最高代币 ===")
r = requests.post(BASE, headers=HEADERS, json={"type": "metaAndAssetCtxs"}, timeout=15)
if r.status_code == 200:
    data = r.json()
    universe = data[0]["universe"]
    ctxs = data[1]
    coins_oi = []
    for i, (meta, ctx) in enumerate(zip(universe, ctxs)):
        if ctx and ctx.get("openInterest"):
            oi = float(ctx["openInterest"]) * float(ctx.get("markPx", 0))
            coins_oi.append({
                "coin": meta["name"],
                "oi_usd": oi,
                "mark_px": float(ctx.get("markPx", 0)),
                "funding": float(ctx.get("funding", 0)),
                "premium": float(ctx.get("premium", 0)),
            })
    coins_oi.sort(key=lambda x: x["oi_usd"], reverse=True)
    print(f"  ✅ OI Top 10:")
    for c in coins_oi[:10]:
        print(f"     {c['coin']:8s} OI=${c['oi_usd']/1e6:.1f}M  资金费率={c['funding']*100:.4f}%  溢价={c['premium']*100:.4f}%")

print("\n=== 测试3: 已知大户地址的当前持仓 ===")
# 从 Coinglass 等平台已知的 Hyperliquid 大户地址
WHALES = [
    ("0xc6ab9ee8ad3647a12242a2afa43152be796f3391", "Whale A"),
    ("0x4d3b7d9e5b2c1f8a6e0c3d7b9f2e5a8c1d4b7e0a", "Whale B"),
    ("0x9f8e7d6c5b4a3f2e1d0c9b8a7f6e5d4c3b2a1f0e", "Whale C"),
]
for addr, name in WHALES[:1]:
    r2 = requests.post(BASE, headers=HEADERS,
                       json={"type": "clearinghouseState", "user": addr}, timeout=15)
    if r2.status_code == 200:
        state = r2.json()
        positions = state.get("assetPositions", [])
        acct_val = float(state["marginSummary"]["accountValue"])
        print(f"  ✅ {name} ({addr[:10]}...) 账户价值: ${acct_val:,.0f}")
        for p in positions:
            pos = p["position"]
            print(f"     {pos['coin']:8s} 方向={'多' if float(pos['szi'])>0 else '空'} "
                  f"仓位=${float(pos['positionValue']):,.0f}  "
                  f"未实现PnL=${float(pos['unrealizedPnl']):,.0f}  "
                  f"杠杆={pos['leverage']['value']}x")

print("\n=== 测试4: 通过 userFills 分析大户最近交易 ===")
WHALE = "0xc6ab9ee8ad3647a12242a2afa43152be796f3391"
r3 = requests.post(BASE, headers=HEADERS,
                   json={"type": "userFills", "user": WHALE}, timeout=15)
if r3.status_code == 200:
    fills = r3.json()
    # 统计最近100笔交易的盈亏
    recent = fills[:100]
    total_pnl = sum(float(f.get("closedPnl", 0)) for f in recent)
    coins = {}
    for f in recent:
        coin = f["coin"]
        coins[coin] = coins.get(coin, 0) + 1
    top_coins = sorted(coins.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"  ✅ 最近100笔交易 总已实现PnL: ${total_pnl:,.2f}")
    print(f"     最活跃交易对: {top_coins}")
    # 找最新的开仓动作
    opens = [f for f in fills[:50] if "Open" in f.get("dir", "")]
    print(f"     最近开仓次数: {len(opens)}")
    if opens:
        latest = opens[0]
        print(f"     最新开仓: {latest['coin']} {latest['dir']} "
              f"价格={latest['px']} 数量={latest['sz']}")

print("\n=== 测试完成 ===\n")
