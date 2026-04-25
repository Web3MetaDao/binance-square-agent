#!/usr/bin/env python3
"""
币安广场运营系统智能体 — 完整集成测试
"""
import sys, os, time, requests
sys.path.insert(0, os.path.dirname(__file__))

from config.settings import DAILY_LIMIT, KOL_LIST, FUTURES_MAP, SQUARE_API_KEY
from core.state import load_state, save_state, get_status_summary
from layers.perception import run_perception, load_market_context
from layers.content import ContentGenerator
from layers.executor import QuotaController, SquarePoster, execute_post

PASS = "✅ PASS"
FAIL = "❌ FAIL"
results = []

def test(name, fn):
    t0 = time.time()
    try:
        fn()
        elapsed = time.time() - t0
        print(f"  {PASS}  {name}  ({elapsed:.1f}s)")
        results.append((name, True))
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  {FAIL}  {name}  ({elapsed:.1f}s)  → {e}")
        results.append((name, False))

print("\n" + "═" * 60)
print("  币安广场运营系统智能体 — 集成测试报告")
print("═" * 60)

# ── T1: 模块导入 ──────────────────────────────────────────
print("\n[T1] 模块导入与配置检查")
def t1():
    assert DAILY_LIMIT == 72
    assert len(KOL_LIST) >= 7
    assert len(FUTURES_MAP) >= 30
    print(f"     每日上限:{DAILY_LIMIT} | KOL:{len(KOL_LIST)}位 | 代币映射:{len(FUTURES_MAP)}个")
    mode = "真实" if SQUARE_API_KEY else "模拟"
    print(f"     运行模式: {mode}")
test("模块导入与配置检查", t1)

# ── T2: 状态管理 ──────────────────────────────────────────
print("\n[T2] 状态管理（持久化）")
def t2():
    state = load_state()
    assert isinstance(state, dict)
    state["_test"] = "ok"
    save_state(state)
    s2 = load_state()
    assert s2.get("_test") == "ok", "持久化失败"
    print(f"     {get_status_summary(s2)}")
test("状态持久化读写", t2)

# ── T3: 感知层 ──────────────────────────────────────────
print("\n[T3] 感知层（双端热点扫描）")
def t3():
    state = load_state()
    ctx = run_perception(state)
    resonance = ctx.get("resonance", [])
    topics = ctx.get("topics", [])
    assert len(resonance) > 0, "未获取到热点代币"
    top = resonance[0]
    print(f"     共振代币:{len(resonance)}个 | 热门叙事:{len(topics)}个")
    print(f"     Top热点: [{top['tier']}] {top['coin']} → {top['futures']} 热度:{top['score']:.1f}")
    s_tier = [r for r in resonance if r["tier"] == "S"]
    print(f"     S级双端共振: {len(s_tier)}个 ({', '.join([r['coin'] for r in s_tier[:3]])})")
test("双端热点扫描", t3)

# ── T4: 配额控制 ──────────────────────────────────────────
print("\n[T4] 配额控制与冷却机制")
def t4():
    state_t = {"daily_count": 0, "last_post_time": 0, "coin_last_post": {}, "today": "2026-01-01"}
    q = QuotaController(state_t)
    ok, _ = q.can_post("BTC")
    assert ok, "初始状态应允许发帖"
    
    state_t["last_post_time"] = time.time()
    q2 = QuotaController(state_t)
    ok2, r2 = q2.can_post("ETH")
    assert not ok2, "全局间隔应拒绝"
    
    state_t["daily_count"] = 100
    q3 = QuotaController(state_t)
    ok3, r3 = q3.can_post("SOL")
    assert not ok3, "满额应拒绝"
    
    state_t["coin_last_post"]["BTC"] = time.time()
    state_t["daily_count"] = 0
    state_t["last_post_time"] = 0
    q4 = QuotaController(state_t)
    ok4, r4 = q4.can_post("BTC")
    assert not ok4, "同币种冷却应拒绝"
    
    print("     初始允许✅ | 全局间隔✅ | 每日满额✅ | 同币种冷却✅")
test("四重配额控制逻辑", t4)

# ── T5: 内容生成 ──────────────────────────────────────────
print("\n[T5] LLM内容生成（真实广场自然格式）")
def t5():
    ctx = load_market_context()
    if not ctx.get("resonance"):
        raise Exception("无市场数据，请先运行感知层")
    gen = ContentGenerator()
    coin_info = ctx["resonance"][0]
    context = {
        "raw_tweets": ctx.get("raw_tweets", []),
        "hot_posts": ctx.get("hot_posts", []),
        "topics": ctx.get("topics", []),
    }
    post = gen.generate(coin_info, context)
    coin = coin_info["coin"]
    futures = coin_info["futures"]
    assert f"${coin}" in post, f"缺少基础 cashtag ${coin}"
    assert f"${futures}" not in post, f"不应出现期货 cashtag ${futures}"
    assert f"{{future}}({futures})" not in post, "不应出现裸露 future marker"
    assert "#币安广场 #内容挖矿" not in post, "不应出现模板化挖矿标签尾行"
    char_count = len(post)
    print(f"     代币:{coin} | 基础cashtag✅ | 自然格式✅ | 字数:{char_count}")
    print(f"     预览: {post[:100]}...")
test("LLM短贴生成（自然格式验证）", t5)

# ── T6: 发帖API ──────────────────────────────────────────
print("\n[T6] 发帖API连通性")
def t6():
    r = requests.post(
        "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add",
        headers={
            "X-Square-OpenAPI-Key": "test_key_for_connectivity_check",
            "Content-Type": "application/json",
            "clienttype": "binanceSkill",
        },
        json={"bodyTextOnly": "connectivity test"},
        timeout=12,
    )
    assert r.status_code == 200, f"HTTP状态码异常: {r.status_code}"
    data = r.json()
    code = str(data.get("code", ""))
    assert code in ("220003", "220004", "000000"), f"意外的错误码: {code}"
    print(f"     HTTP:{r.status_code} | code:{code} | msg:{data.get('message','')[:50]}")
    print(f"     API接口地址正确，网络链路畅通 ✅")
test("发帖API连通性验证", t6)

# ── T7: 模拟发帖闭环 ──────────────────────────────────────
print("\n[T7] 端到端模拟发帖闭环")
def t7():
    from core.orchestrator import Orchestrator
    agent = Orchestrator()
    agent._refresh_market(force=False)  # 使用缓存数据
    success = agent.run_once()
    state = load_state()
    print(f"     发帖结果: {'成功' if success else '跳过/失败'}")
    print(f"     今日进度: {state.get('daily_count', 0)}/{DAILY_LIMIT}")
test("端到端模拟发帖闭环", t7)

# ── 汇总 ──────────────────────────────────────────────────
print("\n" + "═" * 60)
passed = sum(1 for _, ok in results if ok)
total = len(results)
print(f"  总体通过率: {passed}/{total} ({100*passed//total}%)")
print(f"  {'🎉 所有测试通过！系统可以投入使用。' if passed == total else '⚠️  存在失败项，请检查上方错误信息。'}")
print("═" * 60 + "\n")
