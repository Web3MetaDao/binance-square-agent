#!/usr/bin/env python3
"""
数字人直播模块 — 集成测试
"""
import sys, os, time, json
sys.path.insert(0, os.path.dirname(__file__))

__test__ = False

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

def t1():
    from live.engine.market_analyzer import get_full_market_report
    report = get_full_market_report()
    overview = report.get("overview", {})
    assert overview.get("btc_price", 0) > 0, "BTC价格获取失败"
    majors = report.get("major_coins", [])
    assert len(majors) >= 3, "主流币数据不足"
    trending = report.get("trending", [])
    assert len(trending) >= 3, "热点币数据不足"
    print(f"     BTC: ${overview['btc_price']:,.0f} | 情绪: {overview['market_sentiment']}")
    print(f"     主流币: {len(majors)}个 | 热点推荐: {len(trending)}个")
    print(f"     Top热点: {trending[0]['symbol']} {trending[0]['change_pct']:+.2f}% [{trending[0]['recommend_level']}]")
def t2():
    from live.engine.market_analyzer import load_cached_report
    from live.engine.script_generator import generate_full_live_script
    report = load_cached_report()
    scripts = generate_full_live_script(report)
    assert scripts.get("opening"), "开场白生成失败"
    assert scripts.get("market_overview"), "大盘分析话术生成失败"
    assert len(scripts.get("major_coins", [])) >= 3, "主流币话术不足"
    assert scripts.get("trending_recommendation"), "热点推荐话术生成失败"
    assert scripts.get("cart_push"), "小车引导话术生成失败"
    assert scripts.get("closing"), "结束语生成失败"
    print(f"     开场白: {scripts['opening'][:60]}...")
    print(f"     大盘分析: {scripts['market_overview'][:60]}...")
    print(f"     热点推荐: {scripts['trending_recommendation'][:60]}...")
def t3():
    from live.engine.market_analyzer import load_cached_report
    from live.engine.danmu_ai import generate_danmu_reply, classify_danmu, MOCK_DANMUS
    report = load_cached_report()

    test_cases = [
        ("老铁666", "BTC现在多少钱？", "price_query"),
        ("新手小白", "以太坊能涨到5000吗？", "analysis"),
        ("合约大佬", "SOL合约怎么操作？", "contract"),
        ("想赚钱的", "有没有返佣链接？", "referral"),
    ]

    for username, text, expected_cat in test_cases:
        cat = classify_danmu(text)
        assert cat == expected_cat, f"分类错误: {text} → {cat} (期望 {expected_cat})"
        reply = generate_danmu_reply(text, username, report)
        assert len(reply) > 10, f"回复太短: {reply}"
        print(f"     [{cat}] {username}: {text[:20]} → {reply[:40]}...")
def t4():
    from live.cart.cart_manager import CartManager, generate_cart_config
    from live.engine.market_analyzer import load_cached_report
    report = load_cached_report()
    trending = report.get("trending", [])

    cart = CartManager()
    items = cart.get_active_cart()
    assert len(items) >= 4, "小车商品不足"

    # 测试关键词匹配
    script_btc = cart.get_push_script(["BTC", "合约"])
    assert "合约" in script_btc or "BTC" in script_btc or "小车" in script_btc, "BTC关键词匹配失败"

    script_ref = cart.get_push_script(["返佣", "注册"])
    assert len(script_ref) > 10, "返佣话术生成失败"

    # 测试动态小车配置
    config = generate_cart_config(trending)
    assert len(config["base_items"]) >= 1, "基础商品缺失"
    assert len(config["trending_items"]) >= 1, "热点商品缺失"

    print(f"     商品数: {len(items)} | BTC话术: {script_btc[:40]}...")
    print(f"     热点商品: {[i['name'] for i in config['trending_items'][:2]]}")
def t5():
    from live.stream.live_controller import LiveController
    controller = LiveController()
    status = controller.run_once()

    assert status.get("btc_price", 0) > 0, "BTC价格未获取"
    assert status["stats"]["total_scripts_sent"] >= 5, "播报次数不足"
    assert status["stats"]["total_danmu_replied"] >= 2, "弹幕回复不足"
    assert status["stats"]["total_cart_pushes"] >= 1, "小车未推送"

    print(f"     BTC: ${status['btc_price']:,.0f} | 情绪: {status['market_sentiment']}")
    print(f"     播报: {status['stats']['total_scripts_sent']}次 | "
          f"弹幕: {status['stats']['total_danmu_replied']}条 | "
          f"小车: {status['stats']['total_cart_pushes']}次")
def main():
    print("\n" + "═"*60)
    print("  数字人直播模块 — 集成测试报告")
    print("═"*60)

    print("\n[T1] 行情分析引擎")
    test("行情数据获取（BTC+主流币+热点）", t1)

    print("\n[T2] 话术生成器")
    test("完整直播脚本生成（6段话术）", t2)

    print("\n[T3] 弹幕问答 AI")
    test("弹幕分类 + AI 问答生成", t3)

    print("\n[T4] 小车管理器")
    test("小车商品管理 + 动态配置", t4)

    print("\n[T5] 直播控制器（端到端单次运行）")
    test("直播控制器端到端运行", t5)

    print("\n" + "═"*60)
    passed = sum(1 for _, ok in results if ok)
    total = len(results)
    print(f"  总体通过率: {passed}/{total} ({100*passed//total}%)")
    print(f"  {'🎉 数字人直播模块全部测试通过！' if passed == total else '⚠️  存在失败项，请检查上方错误信息。'}")
    print("═"*60 + "\n")


if __name__ == "__main__":
    main()
