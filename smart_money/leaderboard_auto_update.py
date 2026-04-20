#!/usr/bin/env python3
"""
Hyperliquid 排行榜 Top 20 地址自动抓取与更新器（生产版）

核心策略：
1. Playwright 渲染排行榜页面（使用 domcontentloaded 避免超时）
2. 通过 JavaScript 一次性从 DOM 提取所有完整地址（href 中的 0x...）
3. 将 Top 20 地址写入 address_updater.py 的 SEED_ADDRESSES
4. 每周一 UTC 00:00 自动执行（通过 Manus 定时任务调度）

备用方案：
- 当 Playwright 失败时，通过 Hyperliquid funding history API 获取活跃地址
"""

import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(PROJECT_DIR, "data")
ADDRESS_UPDATER_PATH = os.path.join(BASE_DIR, "address_updater.py")
LEADERBOARD_CACHE_PATH = os.path.join(DATA_DIR, "leaderboard_cache.json")
LOG_PATH = os.path.join(PROJECT_DIR, "logs", "leaderboard_update.log")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(os.path.join(PROJECT_DIR, "logs"), exist_ok=True)

HL_LEADERBOARD_URL = "https://app.hyperliquid.xyz/leaderboard"


def log(msg: str):
    """写入日志"""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


async def scrape_leaderboard_playwright(top_n: int = 20) -> list:
    """
    使用 Playwright 抓取排行榜完整地址
    使用 domcontentloaded + 手动等待，避免 networkidle 超时
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log("  [错误] Playwright 未安装")
        return []

    results = []

    async with async_playwright() as p:
        log("  🌐 启动 Chromium 浏览器...")
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
                  "--disable-web-security", "--disable-features=IsolateOrigins"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()

        try:
            # 使用 domcontentloaded 避免 networkidle 超时
            log(f"  📡 访问 {HL_LEADERBOARD_URL}...")
            await page.goto(HL_LEADERBOARD_URL, wait_until="domcontentloaded", timeout=30000)

            # 等待 React 渲染（等待 table 出现）
            log("  ⏳ 等待排行榜表格渲染...")
            try:
                await page.wait_for_selector("table", timeout=20000)
                await page.wait_for_timeout(3000)  # 额外等待数据填充
                log("  ✅ 表格已渲染")
            except Exception:
                log("  ⚠️  等待 table 超时，尝试直接提取...")

            # 通过 JavaScript 一次性提取所有地址
            log("  🔍 通过 JavaScript 提取地址...")
            js_result = await page.evaluate(f"""
                () => {{
                    const results = [];
                    const seen = new Set();
                    
                    // 方法1: 从 table tbody tr 中提取
                    const rows = document.querySelectorAll('table tbody tr');
                    rows.forEach((row, index) => {{
                        if (results.length >= {top_n}) return;
                        
                        const cells = row.querySelectorAll('td');
                        
                        // 从 href 提取完整地址
                        let addr = null;
                        const links = row.querySelectorAll('a[href]');
                        for (const link of links) {{
                            const href = link.getAttribute('href') || '';
                            const m = href.match(/0x[a-fA-F0-9]{{40}}/);
                            if (m) {{ addr = m[0].toLowerCase(); break; }}
                        }}
                        
                        // 从行 HTML 中提取
                        if (!addr) {{
                            const m = row.innerHTML.match(/0x[a-fA-F0-9]{{40}}/);
                            if (m) addr = m[0].toLowerCase();
                        }}
                        
                        if (!addr || seen.has(addr)) return;
                        seen.add(addr);
                        
                        // 提取排名
                        let rank = index + 1;
                        if (cells[0]) {{
                            const t = cells[0].innerText.trim();
                            if (/^\\d+$/.test(t)) rank = parseInt(t);
                        }}
                        
                        // 提取数值
                        const parseVal = (text) => {{
                            if (!text) return 0;
                            const clean = text.replace(/[$,%+]/g, '').replace(/,/g, '').trim();
                            const m = clean.match(/[\\d.]+/);
                            const v = m ? parseFloat(m[0]) : 0;
                            return text.includes('-') ? -v : v;
                        }};
                        
                        results.push({{
                            rank: rank,
                            address: addr,
                            account_value: cells[2] ? parseVal(cells[2].innerText) * 1000 : 0,
                            pnl: cells[3] ? parseVal(cells[3].innerText) * 1000 : 0,
                            roi: cells[4] ? parseVal(cells[4].innerText) : 0,
                        }});
                    }});
                    
                    // 方法2: 如果 table 没有数据，从全页面提取 href
                    if (results.length === 0) {{
                        const allLinks = document.querySelectorAll('a[href]');
                        allLinks.forEach(link => {{
                            if (results.length >= {top_n}) return;
                            const href = link.getAttribute('href') || '';
                            const m = href.match(/0x[a-fA-F0-9]{{40}}/);
                            if (m) {{
                                const addr = m[0].toLowerCase();
                                if (!seen.has(addr)) {{
                                    seen.add(addr);
                                    results.push({{
                                        rank: results.length + 1,
                                        address: addr,
                                        account_value: 0, pnl: 0, roi: 0
                                    }});
                                }}
                            }}
                        }});
                    }}
                    
                    return results;
                }}
            """)

            log(f"  📊 JS 提取到 {len(js_result)} 个地址")

            for item in js_result:
                item["time_window"] = "30D"
                item["scraped_at"] = datetime.now(timezone.utc).isoformat()
                results.append(item)
                log(f"  #{item['rank']:3d} ✅ {item['address']}  "
                    f"账户=${item['account_value']/1e3:.0f}k  "
                    f"PnL=${item['pnl']/1e3:.0f}k  ROI={item['roi']:.1f}%")

        except Exception as e:
            log(f"  ❌ Playwright 操作失败: {e}")
        finally:
            await browser.close()

    log(f"  ✅ Playwright 共抓取 {len(results)} 个地址")
    return results


def fallback_get_addresses_from_api(top_n: int = 20) -> list:
    """
    备用方案：通过 Hyperliquid API 获取活跃大户地址
    数据来源：最近 24h 的资金费率支付记录（大额支付者通常是大户）
    """
    import requests
    log("  🔄 备用方案：通过 API 获取活跃地址...")

    addresses = []
    seen = set()

    # 获取多个主流币的资金费率历史，交叉验证活跃地址
    coins = ["BTC", "ETH", "SOL", "BNB", "XRP"]
    start_time = int((time.time() - 86400) * 1000)

    for coin in coins:
        if len(addresses) >= top_n:
            break
        try:
            r = requests.post(
                "https://api.hyperliquid.xyz/info",
                headers={"Content-Type": "application/json"},
                json={"type": "fundingHistory", "coin": coin, "startTime": start_time},
                timeout=10
            )
            if r.status_code == 200:
                for item in r.json()[:100]:
                    if isinstance(item, dict):
                        user = item.get("user", "")
                        if user and user not in seen and len(user) == 42 and user.startswith("0x"):
                            seen.add(user)
                            addresses.append({
                                "rank": len(addresses) + 1,
                                "address": user.lower(),
                                "account_value": 0, "pnl": 0, "roi": 0,
                                "time_window": f"fallback_{coin}",
                                "scraped_at": datetime.now(timezone.utc).isoformat(),
                            })
        except Exception as e:
            log(f"  ⚠️  获取 {coin} 资金费率失败: {e}")

    log(f"  ✅ 备用方案获取 {len(addresses)} 个地址")
    return addresses[:top_n]


def update_seed_addresses_in_file(new_addresses: list) -> bool:
    """将新地址写入 address_updater.py 的 SEED_ADDRESSES"""
    if not new_addresses:
        log("  ⚠️  无新地址，跳过文件更新")
        return False

    try:
        with open(ADDRESS_UPDATER_PATH, "r", encoding="utf-8") as f:
            content = f.read()

        # 构建新的 SEED_ADDRESSES 块
        lines = ['SEED_ADDRESSES = {\n']
        lines.append(f'    # 自动更新时间: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}\n')
        lines.append(f'    # 来源: Hyperliquid 排行榜 Top {len(new_addresses)} (30D PnL)\n')

        for item in new_addresses:
            addr = item["address"]
            rank = item["rank"]
            pnl_k = item.get("pnl", 0) / 1000
            account_k = item.get("account_value", 0) / 1000
            source = item.get("time_window", "leaderboard_30d")
            lines.append(
                f'    "{addr}": {{"source": "{source}", "rank": {rank}, '
                f'"pnl_k": {pnl_k:.0f}, "account_k": {account_k:.0f}}},\n'
            )

        # 保留已知大户（手动维护）
        lines.append('    # 已知的 Hyperliquid 知名大户（公开信息，手动维护）\n')
        lines.append('    "0xc6ab9ee8ad3647a12242a2afa43152be796f3391": {"source": "coinglass", "tier": "whale"},\n')
        lines.append('}\n')

        new_seed_block = "".join(lines)

        # 替换文件中的 SEED_ADDRESSES 块
        pattern = r'SEED_ADDRESSES\s*=\s*\{.*?\n\}'
        if re.search(pattern, content, re.DOTALL):
            new_content = re.sub(pattern, new_seed_block.rstrip('\n'), content, flags=re.DOTALL)
        else:
            new_content = content.rstrip() + "\n\n" + new_seed_block

        with open(ADDRESS_UPDATER_PATH, "w", encoding="utf-8") as f:
            f.write(new_content)

        log(f"  ✅ address_updater.py 已更新，写入 {len(new_addresses)} 个地址")
        return True

    except Exception as e:
        log(f"  ❌ 文件更新失败: {e}")
        return False


def save_leaderboard_cache(addresses: list):
    """保存排行榜缓存（7天有效期）"""
    cache = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "updated_timestamp": time.time(),
        "total_count": len(addresses),
        "addresses": addresses,
    }
    with open(LEADERBOARD_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    log(f"  💾 排行榜缓存已保存至 {LEADERBOARD_CACHE_PATH}")


def check_cache_valid() -> tuple:
    """检查缓存是否有效（7天内）"""
    if not os.path.exists(LEADERBOARD_CACHE_PATH):
        return False, None
    try:
        with open(LEADERBOARD_CACHE_PATH, "r") as f:
            cache = json.load(f)
        age_days = (time.time() - cache.get("updated_timestamp", 0)) / 86400
        if age_days < 7:
            return True, cache
    except Exception:
        pass
    return False, None


def run_weekly_update(force: bool = False) -> dict:
    """
    执行每周地址库更新任务
    
    Args:
        force: 强制更新，忽略7天缓存
    
    Returns:
        dict: 更新结果摘要
    """
    log("\n" + "="*60)
    log("🔄 Hyperliquid 排行榜地址库 - 每周自动更新")
    log(f"   执行时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log("="*60)

    # 检查缓存
    if not force:
        valid, cache = check_cache_valid()
        if valid:
            age_days = (time.time() - cache.get("updated_timestamp", 0)) / 86400
            log(f"  📦 缓存有效（{age_days:.1f} 天前更新），本次跳过")
            return {
                "status": "cached",
                "count": cache["total_count"],
                "message": f"缓存有效，{age_days:.1f}天前已更新，下次更新将在{7-age_days:.1f}天后"
            }

    # 步骤1：Playwright 抓取
    log("\n  📌 步骤1: Playwright 浏览器抓取排行榜...")
    addresses = asyncio.run(scrape_leaderboard_playwright(top_n=20))

    # 步骤2：如果 Playwright 失败，使用备用方案
    if len(addresses) < 5:
        log(f"\n  ⚠️  Playwright 仅获取 {len(addresses)} 个地址，启用备用方案...")
        fallback_addresses = fallback_get_addresses_from_api(top_n=20)
        # 合并两个来源的地址（去重）
        existing_addrs = {a["address"] for a in addresses}
        for fa in fallback_addresses:
            if fa["address"] not in existing_addrs:
                fa["rank"] = len(addresses) + 1
                addresses.append(fa)
                existing_addrs.add(fa["address"])

    if not addresses:
        log("  ❌ 所有方案均失败，本次更新中止")
        return {"status": "failed", "count": 0, "message": "所有抓取方案均失败"}

    # 步骤3：更新 SEED_ADDRESSES
    log(f"\n  📌 步骤2: 更新 SEED_ADDRESSES（共 {len(addresses)} 个地址）...")
    file_updated = update_seed_addresses_in_file(addresses)

    # 步骤4：保存缓存
    log("\n  📌 步骤3: 保存排行榜缓存...")
    save_leaderboard_cache(addresses)

    result = {
        "status": "success",
        "count": len(addresses),
        "file_updated": file_updated,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "top5_addresses": [a["address"] for a in addresses[:5]],
        "message": f"成功更新 {len(addresses)} 个地址到 SEED_ADDRESSES"
    }

    log(f"\n  🎉 更新完成！共 {len(addresses)} 个地址已写入地址库")
    log(f"  Top 5: {', '.join(a[:12]+'...' for a in result['top5_addresses'])}")
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Hyperliquid 排行榜地址自动更新器")
    parser.add_argument("--force", action="store_true", help="强制更新（忽略7天缓存）")
    parser.add_argument("--test", action="store_true", help="测试模式（仅抓取，不写入文件）")
    args = parser.parse_args()

    if args.test:
        log("🧪 测试模式：仅抓取，不写入文件")
        addresses = asyncio.run(scrape_leaderboard_playwright(top_n=5))
        if not addresses:
            log("Playwright 失败，测试备用方案...")
            addresses = fallback_get_addresses_from_api(top_n=5)
        log(f"\n测试结果（{len(addresses)} 个地址）:")
        for a in addresses:
            log(f"  #{a['rank']} {a['address']}")
    else:
        result = run_weekly_update(force=args.force)
        log(f"\n执行结果: {json.dumps(result, indent=2, ensure_ascii=False)}")
