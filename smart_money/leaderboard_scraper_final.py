#!/usr/bin/env python3
"""
Hyperliquid 排行榜 Top 20 地址自动抓取器（最终版）
核心策略：通过 JavaScript evaluate 一次性从 DOM 提取所有 href 中的完整地址，
避免 React 重新渲染导致的 DOM 失效问题。
"""

import asyncio
import json
import os
import re
import time
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(PROJECT_DIR, "data")
ADDRESS_UPDATER_PATH = os.path.join(BASE_DIR, "address_updater.py")
LEADERBOARD_CACHE_PATH = os.path.join(DATA_DIR, "leaderboard_cache.json")

os.makedirs(DATA_DIR, exist_ok=True)

HL_LEADERBOARD_URL = "https://app.hyperliquid.xyz/leaderboard"


async def scrape_via_js_evaluate(top_n: int = 20) -> list:
    """
    通过 JavaScript evaluate 一次性提取所有行的完整地址
    这是最稳定的方案，不依赖 Python 端的 DOM 操作
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return []

    results = []

    async with async_playwright() as p:
        print(f"  🌐 启动浏览器，访问 Hyperliquid 排行榜...")
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()

        try:
            await page.goto(HL_LEADERBOARD_URL, wait_until="networkidle", timeout=30000)
            await page.wait_for_selector("table tbody tr", timeout=15000)
            print(f"  ✅ 排行榜已加载")

            # 等待更多数据渲染
            await page.wait_for_timeout(2000)

            # 通过 JavaScript 一次性提取所有行数据
            js_result = await page.evaluate("""
                () => {
                    const rows = document.querySelectorAll('table tbody tr');
                    const data = [];
                    
                    rows.forEach((row, index) => {
                        const cells = row.querySelectorAll('td');
                        if (cells.length < 2) return;
                        
                        // 提取排名
                        let rank = index + 1;
                        const rankText = cells[0]?.innerText?.trim();
                        if (rankText && /^\\d+$/.test(rankText)) {
                            rank = parseInt(rankText);
                        }
                        
                        // 从所有 <a> 标签的 href 中提取完整地址
                        let fullAddress = null;
                        const links = row.querySelectorAll('a[href]');
                        for (const link of links) {
                            const href = link.getAttribute('href') || '';
                            const match = href.match(/0x[a-fA-F0-9]{40}/);
                            if (match) {
                                fullAddress = match[0].toLowerCase();
                                break;
                            }
                        }
                        
                        // 备用：从行的完整 HTML 中提取
                        if (!fullAddress) {
                            const html = row.innerHTML;
                            const match = html.match(/0x[a-fA-F0-9]{40}/);
                            if (match) {
                                fullAddress = match[0].toLowerCase();
                            }
                        }
                        
                        // 备用：从 data 属性中提取
                        if (!fullAddress) {
                            const allElements = row.querySelectorAll('*');
                            for (const el of allElements) {
                                for (const attr of el.attributes) {
                                    const match = attr.value.match(/0x[a-fA-F0-9]{40}/);
                                    if (match) {
                                        fullAddress = match[0].toLowerCase();
                                        break;
                                    }
                                }
                                if (fullAddress) break;
                            }
                        }
                        
                        if (!fullAddress) return;
                        
                        // 提取数值数据
                        const parseNum = (text) => {
                            if (!text) return 0;
                            const clean = text.replace(/[$,%+]/g, '').replace(/,/g, '').trim();
                            const match = clean.match(/[\\d.]+/);
                            const val = match ? parseFloat(match[0]) : 0;
                            return text.includes('-') ? -val : val;
                        };
                        
                        data.push({
                            rank: rank,
                            address: fullAddress,
                            account_value: parseNum(cells[2]?.innerText) * 1000,
                            pnl: parseNum(cells[3]?.innerText) * 1000,
                            roi: parseNum(cells[4]?.innerText),
                        });
                    });
                    
                    return data;
                }
            """)

            print(f"  📊 JS 提取到 {len(js_result)} 行数据")

            for item in js_result[:top_n]:
                if item.get("address"):
                    item["time_window"] = "30D"
                    item["scraped_at"] = datetime.now(timezone.utc).isoformat()
                    results.append(item)
                    print(f"  #{item['rank']:3d} ✅ {item['address']}  "
                          f"账户=${item['account_value']/1e3:.0f}k  "
                          f"PnL=${item['pnl']/1e3:.0f}k  ROI={item['roi']:.1f}%")

        except Exception as e:
            print(f"  ❌ 失败: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await browser.close()

    print(f"\n  ✅ 成功抓取 {len(results)} 个完整地址")
    return results


def update_seed_addresses_in_file(new_addresses: list) -> bool:
    """将新地址写入 address_updater.py 的 SEED_ADDRESSES"""
    if not new_addresses:
        return False

    with open(ADDRESS_UPDATER_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    lines = ['SEED_ADDRESSES = {\n']
    lines.append(f'    # 自动更新时间: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}\n')
    lines.append(f'    # 来源: Hyperliquid 排行榜 Top {len(new_addresses)} (30D PnL)\n')

    for item in new_addresses:
        addr = item["address"]
        rank = item["rank"]
        pnl_k = item["pnl"] / 1000 if item.get("pnl") else 0
        account_k = item["account_value"] / 1000 if item.get("account_value") else 0
        lines.append(
            f'    "{addr}": {{"source": "leaderboard_30d", "rank": {rank}, '
            f'"pnl_k": {pnl_k:.0f}, "account_k": {account_k:.0f}}},\n'
        )

    # 保留已知大户
    lines.append('    # 已知的 Hyperliquid 知名大户（公开信息，手动维护）\n')
    lines.append('    "0xc6ab9ee8ad3647a12242a2afa43152be796f3391": {"source": "coinglass", "tier": "whale"},\n')
    lines.append('}\n')

    new_seed_block = "".join(lines)

    pattern = r'SEED_ADDRESSES\s*=\s*\{.*?\n\}'
    if re.search(pattern, content, re.DOTALL):
        new_content = re.sub(pattern, new_seed_block.rstrip('\n'), content, flags=re.DOTALL)
    else:
        new_content = content.rstrip() + "\n\n" + new_seed_block

    with open(ADDRESS_UPDATER_PATH, "w", encoding="utf-8") as f:
        f.write(new_content)

    print(f"  ✅ address_updater.py 已更新，写入 {len(new_addresses)} 个地址")
    return True


def save_cache(addresses: list):
    """保存缓存"""
    cache = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "updated_timestamp": time.time(),
        "total_count": len(addresses),
        "addresses": addresses,
    }
    with open(LEADERBOARD_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    print(f"  💾 缓存已保存: {LEADERBOARD_CACHE_PATH}")


def run_weekly_update(force: bool = False) -> dict:
    """执行每周更新任务（供定时调度调用）"""
    print("\n" + "="*60)
    print("🔄 Hyperliquid 排行榜地址库 - 每周自动更新")
    print(f"   执行时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("="*60)

    # 检查缓存（7天内有效）
    if not force and os.path.exists(LEADERBOARD_CACHE_PATH):
        try:
            with open(LEADERBOARD_CACHE_PATH, "r") as f:
                cache = json.load(f)
            age_days = (time.time() - cache.get("updated_timestamp", 0)) / 86400
            if age_days < 7:
                print(f"  📦 缓存有效（{age_days:.1f} 天前更新），跳过本次抓取")
                return {
                    "status": "cached",
                    "count": cache["total_count"],
                    "message": f"缓存有效，{age_days:.1f}天前已更新"
                }
        except Exception:
            pass

    # 执行抓取
    print("\n  🕷️  开始抓取 Hyperliquid 排行榜 Top 20（30D PnL）...")
    addresses = asyncio.run(scrape_via_js_evaluate(top_n=20))

    if not addresses:
        # 备用方案：通过 funding history 接口获取活跃地址
        print("  ⚠️  Playwright 抓取失败，启用备用方案...")
        import requests
        try:
            r = requests.post(
                "https://api.hyperliquid.xyz/info",
                headers={"Content-Type": "application/json"},
                json={"type": "fundingHistory", "coin": "BTC",
                      "startTime": int((time.time() - 86400) * 1000)},
                timeout=15
            )
            if r.status_code == 200:
                seen = set()
                for item in r.json()[:200]:
                    if isinstance(item, dict):
                        user = item.get("user", "")
                        if user and user not in seen and len(user) == 42:
                            seen.add(user)
                            addresses.append({
                                "rank": len(addresses) + 1,
                                "address": user.lower(),
                                "account_value": 0, "pnl": 0, "roi": 0,
                                "time_window": "fallback_funding",
                                "scraped_at": datetime.now(timezone.utc).isoformat(),
                            })
                            if len(addresses) >= 20:
                                break
                print(f"  ✅ 备用方案获取 {len(addresses)} 个地址")
        except Exception as e:
            print(f"  ❌ 备用方案失败: {e}")

    if addresses:
        update_seed_addresses_in_file(addresses)
        save_cache(addresses)
        result = {
            "status": "success",
            "count": len(addresses),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "top5_addresses": [a["address"] for a in addresses[:5]],
        }
        print(f"\n  🎉 更新完成！共 {len(addresses)} 个地址已写入地址库")
        return result
    else:
        return {"status": "failed", "count": 0, "message": "所有方案均失败"}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Hyperliquid 排行榜地址自动更新器")
    parser.add_argument("--force", action="store_true", help="强制更新（忽略7天缓存）")
    parser.add_argument("--test", action="store_true", help="测试模式（只抓取前5个）")
    args = parser.parse_args()

    if args.test:
        print("🧪 测试模式：抓取前5个地址")
        results = asyncio.run(scrape_via_js_evaluate(top_n=5))
        print(f"\n抓取结果（{len(results)} 个）:")
        for r in results:
            print(f"  #{r['rank']} {r['address']}")
    else:
        result = run_weekly_update(force=args.force)
        print(f"\n执行结果: {json.dumps(result, indent=2, ensure_ascii=False)}")
