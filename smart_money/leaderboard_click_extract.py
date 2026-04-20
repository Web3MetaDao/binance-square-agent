#!/usr/bin/env python3
"""
Hyperliquid 排行榜 Top 20 地址提取器（点击跳转版）
核心策略：
1. 等待 WebSocket 数据注入（约4秒）
2. 逐行点击排行榜行，触发页面跳转到 /trade/0x... 或 /portfolio/0x...
3. 从 URL 中提取完整的 42 位地址
4. 返回导航到排行榜继续下一行
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
LOG_PATH = os.path.join(PROJECT_DIR, "logs", "leaderboard_update.log")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(os.path.join(PROJECT_DIR, "logs"), exist_ok=True)

HL_LEADERBOARD_URL = "https://app.hyperliquid.xyz/leaderboard"


def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


async def wait_for_leaderboard_data(page, max_wait: int = 15) -> bool:
    """等待排行榜数据从 WebSocket 注入完成（skeleton 消失）"""
    for i in range(max_wait):
        await page.wait_for_timeout(1000)
        check = await page.evaluate('''
            () => {
                const tbody = document.querySelector("table tbody");
                if (!tbody) return {rows: 0, hasSkeleton: true};
                const rows = tbody.querySelectorAll("tr");
                const html = tbody.innerHTML;
                const hasSkeleton = html.includes("jtPqEj") || html.includes("skeleton");
                const text = tbody.innerText.trim();
                return {rows: rows.length, hasSkeleton: hasSkeleton, textLen: text.length};
            }
        ''')
        if check['rows'] > 1 and not check['hasSkeleton'] and check['textLen'] > 100:
            log(f"  ✅ 排行榜数据已加载（{i+1}秒，{check['rows']}行）")
            return True
    log(f"  ⚠️  等待超时（{max_wait}秒），尝试继续...")
    return False


async def extract_row_data_from_text(page, top_n: int = 20) -> list:
    """从 tbody 文本中提取截断地址和数值，用于后续匹配"""
    rows_data = await page.evaluate(f'''
        () => {{
            const tbody = document.querySelector("table tbody");
            if (!tbody) return [];
            const rows = tbody.querySelectorAll("tr");
            const result = [];
            
            rows.forEach((row, idx) => {{
                if (result.length >= {top_n}) return;
                const cells = row.querySelectorAll("td");
                if (cells.length < 4) return;
                
                const rankText = cells[0]?.innerText?.trim() || String(idx + 1);
                const traderText = cells[1]?.innerText?.trim() || "";
                const accountText = cells[2]?.innerText?.trim() || "0";
                const pnlText = cells[3]?.innerText?.trim() || "0";
                const roiText = cells[4]?.innerText?.trim() || "0";
                
                // 提取截断地址（如 0x4ec8...9a80）
                const addrMatch = traderText.match(/0x[a-fA-F0-9]{{4}}\\.\\.\\.[a-fA-F0-9]{{4}}/);
                const truncAddr = addrMatch ? addrMatch[0] : "";
                
                const parseVal = (text) => {{
                    const clean = text.replace(/[$,%+\\s]/g, "").replace(/,/g, "");
                    const m = clean.match(/[\\d.]+/);
                    const v = m ? parseFloat(m[0]) : 0;
                    return text.includes("-") ? -v : v;
                }};
                
                result.push({{
                    rank: parseInt(rankText) || (idx + 1),
                    truncAddr: truncAddr,
                    account_value: parseVal(accountText),
                    pnl: parseVal(pnlText),
                    roi: parseVal(roiText),
                    rowIndex: idx
                }});
            }});
            
            return result;
        }}
    ''')
    return rows_data


async def click_row_get_address(page, row_index: int) -> str:
    """点击指定行，从 URL 或页面提取完整地址"""
    try:
        # 重新获取行（每次点击后 DOM 可能重建）
        rows = await page.query_selector_all("table tbody tr")
        if row_index >= len(rows):
            return ""
        
        row = rows[row_index]
        
        # 检查行是否有 href 链接
        links = await row.query_selector_all("a[href]")
        for link in links:
            href = await link.get_attribute("href")
            if href:
                m = re.search(r'0x[a-fA-F0-9]{40}', href)
                if m:
                    return m.group(0).lower()
        
        # 点击行，等待 URL 变化
        current_url = page.url
        await row.click()
        
        # 等待 URL 变化（最多3秒）
        for _ in range(6):
            await page.wait_for_timeout(500)
            new_url = page.url
            if new_url != current_url:
                m = re.search(r'0x[a-fA-F0-9]{40}', new_url)
                if m:
                    return m.group(0).lower()
                break
        
        # 检查当前 URL
        m = re.search(r'0x[a-fA-F0-9]{40}', page.url)
        if m:
            return m.group(0).lower()
        
        return ""
    except Exception as e:
        log(f"  ⚠️  点击第{row_index+1}行失败: {e}")
        return ""


async def scrape_leaderboard_with_click(top_n: int = 20) -> list:
    """
    主抓取函数：等待数据加载 -> 逐行点击提取完整地址
    """
    from playwright.async_api import async_playwright
    
    results = []
    
    async with async_playwright() as p:
        log("  🌐 启动 Chromium 浏览器...")
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()
        
        try:
            log(f"  📡 访问排行榜...")
            await page.goto(HL_LEADERBOARD_URL, wait_until="domcontentloaded", timeout=30000)
            
            # 等待 WebSocket 数据注入
            await wait_for_leaderboard_data(page, max_wait=12)
            
            # 提取行数据（含截断地址）
            rows_data = await extract_row_data_from_text(page, top_n=top_n)
            log(f"  📊 检测到 {len(rows_data)} 行数据")
            
            if not rows_data:
                log("  ❌ 未检测到排行榜数据")
                await browser.close()
                return []
            
            # 逐行点击提取完整地址
            log(f"  🖱️  开始逐行点击提取完整地址（共{len(rows_data)}行）...")
            
            for i, row_info in enumerate(rows_data[:top_n]):
                # 每次点击后可能跳转，需要返回排行榜
                if page.url != HL_LEADERBOARD_URL:
                    await page.goto(HL_LEADERBOARD_URL, wait_until="domcontentloaded", timeout=20000)
                    await wait_for_leaderboard_data(page, max_wait=8)
                
                full_addr = await click_row_get_address(page, i)
                
                if full_addr:
                    item = {
                        "rank": row_info["rank"],
                        "address": full_addr,
                        "account_value": row_info["account_value"],
                        "pnl": row_info["pnl"],
                        "roi": row_info["roi"],
                        "time_window": "30D",
                        "scraped_at": datetime.now(timezone.utc).isoformat(),
                    }
                    results.append(item)
                    log(f"  #{row_info['rank']:3d} ✅ {full_addr}  "
                        f"PnL=${row_info['pnl']/1e6:.1f}M  ROI={row_info['roi']:.1f}%")
                    
                    # 返回排行榜
                    if page.url != HL_LEADERBOARD_URL:
                        await page.go_back()
                        await wait_for_leaderboard_data(page, max_wait=6)
                else:
                    log(f"  #{row_info['rank']:3d} ⚠️  截断地址={row_info['truncAddr']}，无法获取完整地址")
                
                # 避免操作过快
                await page.wait_for_timeout(500)
        
        except Exception as e:
            log(f"  ❌ 抓取失败: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await browser.close()
    
    log(f"  ✅ 共成功提取 {len(results)} 个完整地址")
    return results


def update_seed_addresses(new_addresses: list) -> bool:
    """将新地址写入 address_updater.py 的 SEED_ADDRESSES"""
    if not new_addresses:
        return False
    
    try:
        with open(ADDRESS_UPDATER_PATH, "r", encoding="utf-8") as f:
            content = f.read()
        
        lines = ['SEED_ADDRESSES = {\n']
        lines.append(f'    # 自动更新时间: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}\n')
        lines.append(f'    # 来源: Hyperliquid 排行榜 Top {len(new_addresses)} (30D PnL)\n')
        
        for item in new_addresses:
            addr = item["address"]
            rank = item["rank"]
            pnl_m = item.get("pnl", 0) / 1e6
            account_m = item.get("account_value", 0) / 1e6
            lines.append(
                f'    "{addr}": {{"source": "leaderboard_30d", "rank": {rank}, '
                f'"pnl_M": {pnl_m:.2f}, "account_M": {account_m:.2f}}},\n'
            )
        
        lines.append('    # 已知大户（手动维护）\n')
        lines.append('    "0xc6ab9ee8ad3647a12242a2afa43152be796f3391": {"source": "coinglass", "tier": "whale"},\n')
        lines.append('}\n')
        
        new_block = "".join(lines)
        pattern = r'SEED_ADDRESSES\s*=\s*\{.*?\n\}'
        if re.search(pattern, content, re.DOTALL):
            new_content = re.sub(pattern, new_block.rstrip('\n'), content, flags=re.DOTALL)
        else:
            new_content = content.rstrip() + "\n\n" + new_block
        
        with open(ADDRESS_UPDATER_PATH, "w", encoding="utf-8") as f:
            f.write(new_content)
        
        log(f"  ✅ SEED_ADDRESSES 已更新（{len(new_addresses)} 个地址）")
        return True
    except Exception as e:
        log(f"  ❌ 文件更新失败: {e}")
        return False


def save_cache(addresses: list):
    cache = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "updated_timestamp": time.time(),
        "total_count": len(addresses),
        "addresses": addresses,
    }
    with open(LEADERBOARD_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    log(f"  💾 缓存已保存")


def run_weekly_update(force: bool = False) -> dict:
    """每周自动更新入口（供定时任务调用）"""
    log("\n" + "="*60)
    log("🔄 Hyperliquid 排行榜地址库 - 每周自动更新")
    log(f"   执行时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log("="*60)
    
    # 检查缓存（7天内有效）
    if not force and os.path.exists(LEADERBOARD_CACHE_PATH):
        try:
            with open(LEADERBOARD_CACHE_PATH) as f:
                cache = json.load(f)
            age_days = (time.time() - cache.get("updated_timestamp", 0)) / 86400
            if age_days < 7:
                log(f"  📦 缓存有效（{age_days:.1f}天前更新），跳过")
                return {"status": "cached", "count": cache["total_count"],
                        "message": f"{age_days:.1f}天前已更新"}
        except Exception:
            pass
    
    # 执行抓取
    addresses = asyncio.run(scrape_leaderboard_with_click(top_n=20))
    
    if not addresses:
        log("  ❌ 抓取失败")
        return {"status": "failed", "count": 0}
    
    update_seed_addresses(addresses)
    save_cache(addresses)
    
    result = {
        "status": "success",
        "count": len(addresses),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "top5": [a["address"] for a in addresses[:5]],
    }
    log(f"\n  🎉 完成！{len(addresses)} 个地址已更新")
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="强制更新")
    parser.add_argument("--test", action="store_true", help="测试模式（只抓取前3个）")
    args = parser.parse_args()
    
    if args.test:
        log("🧪 测试模式：抓取前3个地址")
        addrs = asyncio.run(scrape_leaderboard_with_click(top_n=3))
        log(f"\n结果（{len(addrs)} 个）:")
        for a in addrs:
            log(f"  #{a['rank']} {a['address']}  PnL=${a['pnl']/1e6:.1f}M")
    else:
        result = run_weekly_update(force=args.force)
        log(f"\n执行结果: {json.dumps(result, indent=2, ensure_ascii=False)}")
