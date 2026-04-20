#!/usr/bin/env python3
"""
调试脚本：等待 Hyperliquid 排行榜 WebSocket 数据注入完成
"""
import asyncio
from playwright.async_api import async_playwright

async def debug():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        page = await browser.new_page(viewport={'width': 1920, 'height': 1080})
        
        # 捕获 WebSocket 消息
        ws_messages = []
        
        async def on_ws(ws):
            print(f"  📡 WS 连接: {ws.url}")
            
            def on_frame(frame):
                try:
                    import json
                    data = json.loads(frame.payload if hasattr(frame, 'payload') else frame)
                    msg_str = str(data)[:300]
                    if any(k in msg_str.lower() for k in ['leaderboard', 'pnl', 'trader', 'rank']):
                        ws_messages.append(data)
                        print(f"  📨 WS 热点消息: {msg_str[:200]}")
                except Exception:
                    pass
            
            ws.on("framereceived", on_frame)
        
        page.on("websocket", on_ws)
        
        print("  🌐 访问排行榜...")
        await page.goto('https://app.hyperliquid.xyz/leaderboard', 
                       wait_until='domcontentloaded', timeout=30000)
        
        # 等待 skeleton 消失（等待真实内容出现）
        print("  ⏳ 等待 WebSocket 数据注入（最多15秒）...")
        
        for i in range(15):
            await page.wait_for_timeout(1000)
            
            # 检查 tbody 行数和内容
            check = await page.evaluate('''
                () => {
                    const tbody = document.querySelector("table tbody");
                    if (!tbody) return {rows: 0, hasSkeleton: false, hasAddr: false};
                    const rows = tbody.querySelectorAll("tr");
                    const html = tbody.innerHTML;
                    const hasSkeleton = html.includes("jtPqEj") || html.includes("skeleton");
                    const hasAddr = /0x[a-fA-F0-9]{40}/.test(html);
                    // 检查是否有真实文字内容
                    const text = tbody.innerText.trim();
                    return {
                        rows: rows.length, 
                        hasSkeleton: hasSkeleton, 
                        hasAddr: hasAddr,
                        textLen: text.length,
                        textPreview: text.substring(0, 200)
                    };
                }
            ''')
            
            print(f"  [{i+1}s] rows={check['rows']} skeleton={check['hasSkeleton']} "
                  f"hasAddr={check['hasAddr']} textLen={check['textLen']}")
            
            if check['hasAddr'] or (check['rows'] > 1 and not check['hasSkeleton']):
                print(f"  ✅ 数据已加载！")
                print(f"  文本预览: {check['textPreview']}")
                break
        
        # 最终提取
        final = await page.evaluate('''
            () => {
                const html = document.body.innerHTML;
                const addrs = [...html.matchAll(/0x[a-fA-F0-9]{40}/g)].map(m => m[0].toLowerCase());
                const unique = [...new Set(addrs)];
                
                // 获取表格文本
                const tbody = document.querySelector("table tbody");
                const text = tbody ? tbody.innerText : "";
                
                return {
                    addresses: unique.slice(0, 25),
                    tbodyText: text.substring(0, 500),
                    wsMessages: window.__wsMessages || []
                };
            }
        ''')
        
        print(f"\n  最终结果:")
        print(f"  找到地址: {len(final['addresses'])} 个")
        for addr in final['addresses'][:10]:
            print(f"    {addr}")
        print(f"  tbody 文本: {final['tbodyText'][:300]}")
        
        await browser.close()
        return final

if __name__ == "__main__":
    result = asyncio.run(debug())
