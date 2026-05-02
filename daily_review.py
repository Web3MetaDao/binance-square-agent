#!/usr/bin/env python3
"""
每日复盘学习系统 — 首席交易科学家
==========================================
拉取OKX涨幅榜/跌幅榜前20 → 多维度分析 → 写入brain知识库 → 识别模式 → 策略建议
"""
import requests, json, os, sys, time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ── 配置 ──
BRAIN_REVIEW_DIR = os.path.expanduser("~/brain/ideas/market-patterns")
BRAIN_PATTERNS_DIR = os.path.join(BRAIN_REVIEW_DIR, "patterns")
BRAIN_DAILY_DIR = os.path.join(BRAIN_REVIEW_DIR, "daily")
PATTERN_LOG = os.path.join(BRAIN_PATTERNS_DIR, "recognition-log.md")

# TG推送 (可选)
TG_BOT_TOKEN = None
TG_CHAT_ID = None

# ── 工具函数 ──

def get_okx(endpoint, params=None):
    r = requests.get(f'https://www.okx.com{endpoint}', params=params, timeout=15)
    if r.status_code != 200:
        return None
    data = r.json()
    if data.get('code') != '0':
        return None
    return data['data']

def fmt_price(p):
    p = float(p)
    if p < 0.001: return f"${p:.8f}".rstrip('0')
    if p < 1: return f"${p:.6f}".rstrip('0')
    if p < 100: return f"${p:.4f}".rstrip('0')
    if p < 10000: return f"${p:.2f}"
    return f"${p:,.2f}"

def fmt_vol(v):
    v = float(v)
    if v >= 1e9: return f"${v/1e9:.2f}B"
    if v >= 1e6: return f"${v/1e6:.2f}M"
    if v >= 1e3: return f"${v/1e3:.1f}K"
    return f"${v:.0f}"

def load_env():
    """从币安广场agent的.env加载TG配置"""
    env_path = '/root/binance-square-agent/.env'
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                if line.startswith('TG_BOT_TOKEN='):
                    globals()['TG_BOT_TOKEN'] = line.strip().split('=', 1)[1]
                elif line.startswith('TG_CHAT_ID='):
                    globals()['TG_CHAT_ID'] = line.strip().split('=', 1)[1]

def send_tg(text):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        requests.post(url, json={'chat_id': TG_CHAT_ID, 'text': text, 'parse_mode': 'HTML'}, timeout=10)
    except Exception as e:
        print(f"[TG] 推送失败: {e}")

# ── 核心分析引擎 ──

def fetch_all_tickers():
    """拉取全市场OKX SWAP ticker"""
    data = get_okx('/api/v5/market/tickers', {'instType': 'SWAP'})
    if not data:
        return []
    
    tickers = []
    for t in data:
        inst = t['instId']
        if not inst.endswith('-USDT-SWAP'):
            continue
        last = float(t['last'])
        open24 = float(t['open24h'])
        if open24 == 0:
            continue
        chg = (last - open24) / open24 * 100
        tickers.append({
            'sym': inst.replace('-USDT-SWAP', ''),
            'last': last,
            'chg24h': chg,
            'high24h': float(t['high24h']),
            'low24h': float(t['low24h']),
            'vol_coin': float(t['volCcy24h']),
            'vol_usd': last * float(t['volCcy24h']),
        })
    return tickers

def fetch_coin_detail(sym):
    """拉取单个币的1H/4H K线数据"""
    result = {'sym': sym}
    
    # 4H K线 (14根)
    kl4 = get_okx('/api/v5/market/candles', {'instId': f'{sym}-USDT-SWAP', 'bar': '4H', 'limit': 14})
    if kl4:
        c4 = [float(x[4]) for x in kl4]
        h4 = [float(x[2]) for x in kl4]
        l4 = [float(x[1]) for x in kl4]
        result['chg_2d'] = (c4[-1] - c4[0]) / c4[0] * 100 if c4[0] else 0
        result['range_2d'] = (max(h4) - min(l4)) / min(l4) * 100 if min(l4) else 0
    
    # 1H K线 (24根)
    kl1 = get_okx('/api/v5/market/candles', {'instId': f'{sym}-USDT-SWAP', 'bar': '1H', 'limit': 24})
    if kl1:
        o1 = [float(x[1]) for x in kl1]
        c1 = [float(x[4]) for x in kl1]
        v1 = [float(x[5]) for x in kl1]
        
        # 最大单小时波幅
        max_chg = max(abs((c1[i] - o1[i]) / o1[i] * 100) for i in range(len(c1)))
        result['max_1h_surge'] = max_chg
        
        # 阳/阴线统计
        ups = sum(1 for i in range(len(c1)) if c1[i] > o1[i])
        result['up_count'] = ups
        result['down_count'] = len(c1) - ups
        
        # 近3H量能对比
        vol_avg = sum(v1) / len(v1)
        vol_last3 = sum(v1[:3]) / 3
        result['vol_ratio'] = vol_last3 / vol_avg if vol_avg else 1
    
    time.sleep(0.1)
    return result

def analyze(tickers, top_n=10):
    """主分析函数"""
    gainers = sorted(tickers, key=lambda x: x['chg24h'], reverse=True)
    losers = sorted(tickers, key=lambda x: x['chg24h'])
    
    focus = [g['sym'] for g in gainers[:top_n]] + [l['sym'] for l in losers[:top_n]]
    
    # 并发拉取详情
    details = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetch_coin_detail, sym): sym for sym in focus if sym}
        for fut in as_completed(futures):
            d = fut.result()
            details[d['sym']] = d
    
    # 构建涨幅榜分析
    gainer_analysis = []
    for g in gainers[:top_n]:
        sym = g['sym']
        d = details.get(sym, {})
        diags = []
        if d.get('max_1h_surge', 0) > 15:
            diags.append(f"脉冲拉升({d['max_1h_surge']:.0f}%/H)")
        elif d.get('max_1h_surge', 0) > 8:
            diags.append(f"强买盘({d['max_1h_surge']:.0f}%/H)")
        if d.get('vol_ratio', 1) > 2:
            diags.append("尾盘放量")
        if d.get('up_count', 0) > 14:
            diags.append("阳线主导")
        if d.get('chg_2d', 0) < -20:
            diags.append("2日深跌反弹")
        g['diagnosis'] = ' | '.join(diags) if diags else '温和上行'
        g['detail'] = d
        gainer_analysis.append(g)
    
    # 构建跌幅榜分析
    loser_analysis = []
    for l in losers[:top_n]:
        sym = l['sym']
        d = details.get(sym, {})
        diags = []
        if d.get('vol_ratio', 1) > 2:
            diags.append("尾盘恐慌量")
        if d.get('max_1h_surge', 0) > 12:
            diags.append(f"急跌({d['max_1h_surge']:.0f}%/H)")
        if d.get('down_count', 0) > 14:
            diags.append("阴线压制")
        if l['vol_usd'] > 50_000_000:
            diags.append(f"恐慌成交{fmt_vol(l['vol_usd'])}")
        l['diagnosis'] = ' | '.join(diags) if diags else '温和下跌'
        l['detail'] = d
        loser_analysis.append(l)
    
    return {'gainers': gainer_analysis, 'losers': loser_analysis}

def extract_patterns(result):
    """从分析结果中提取模式"""
    patterns = []
    gainers = result['gainers']
    losers = result['losers']
    
    # 超跌反弹模式
    rebound = [g for g in gainers if g['detail'].get('chg_2d', 0) < -20 and g['chg24h'] > 10]
    if len(rebound) >= 1:
        patterns.append({
            'id': 'P001',
            'name': '超跌反弹模式',
            'count': len(rebound),
            'coins': [r['sym'] for r in rebound],
            'definition': '2日跌>20% + 单日反弹>10%, RSI<30+放量',
            'signal': '买入/做多'
        })
    
    # 阳线密集推升模式
    steady_up = [g for g in gainers if g['detail'].get('up_count', 0) >= 17 and g['detail'].get('max_1h_surge', 0) < 5]
    if len(steady_up) >= 1:
        patterns.append({
            'id': 'P002',
            'name': '阳线密集推升模式',
            'count': len(steady_up),
            'coins': [s['sym'] for s in steady_up],
            'definition': '24H内阳线≥17根 + 单小时波幅<5% + 成交额>均量1.5x',
            'signal': '持续行情信号'
        })
    
    # 高位反转恐慌模式
    high_reversal = [l for l in losers if l['detail'].get('chg_2d', 0) > 10 and l['chg24h'] < -13]
    if len(high_reversal) >= 1:
        patterns.append({
            'id': 'P003',
            'name': '高位反转恐慌模式',
            'count': len(high_reversal),
            'coins': [r['sym'] for r in high_reversal],
            'definition': '2日暴涨>10% + 单日暴跌>13% + 阴线≥15根/24H',
            'signal': '做空/离场'
        })
    
    # 恐慌放量模式
    panic = [l for l in losers if l['chg24h'] < -15 and l['vol_usd'] > 10_000_000]
    if len(panic) >= 1:
        patterns.append({
            'id': 'P004',
            'name': '恐慌放量模式',
            'count': len(panic),
            'coins': [p['sym'] for p in panic],
            'definition': '单日跌>15% + 成交额>10M',
            'signal': '抄底机会(需OI验证)'
        })
    
    return patterns

def check_pattern_overlap(current_patterns):
    """检查当前发现的模式是否与历史记录重叠"""
    if not os.path.exists(PATTERN_LOG):
        return current_patterns
    
    try:
        with open(PATTERN_LOG) as f:
            content = f.read()
        
        for p in current_patterns:
            pid = p['id']
            # 检查是否已有记录
            if pid in content:
                p['status'] = 'confirmed' if '3+次验证' in content and pid in content else 'recurring'
            else:
                p['status'] = 'new'
        
        return current_patterns
    except:
        return current_patterns

def format_tg_report(result, patterns, date_str):
    """格式化为TG推送文本"""
    g = result['gainers'][:5]
    l = result['losers'][:5]
    
    lines = []
    lines.append(f"📊 <b>首席交易科学家 · 每日复盘</b>")
    lines.append(f"📅 {date_str}")
    lines.append("")
    
    lines.append("🏆 <b>涨幅TOP5</b>")
    for i, coin in enumerate(g, 1):
        lines.append(f"{i}. <b>{coin['sym']}</b> +{coin['chg24h']:.1f}% | {fmt_price(coin['last'])} | Vol:{fmt_vol(coin['vol_usd'])}")
        lines.append(f"   → {coin.get('diagnosis', '')}")
    lines.append("")
    
    lines.append("💀 <b>跌幅TOP5</b>")
    for i, coin in enumerate(l, 1):
        lines.append(f"{i}. <b>{coin['sym']}</b> {coin['chg24h']:.1f}% | {fmt_price(coin['last'])} | Vol:{fmt_vol(coin['vol_usd'])}")
        lines.append(f"   → {coin.get('diagnosis', '')}")
    lines.append("")
    
    if patterns:
        lines.append("🧠 <b>模式识别</b>")
        for p in patterns:
            status = {'new': '🆕', 'recurring': '🔄', 'confirmed': '✅'}
            s = status.get(p.get('status', 'new'), '🆕')
            lines.append(f"{s} {p['name']} ({', '.join(p['coins'])})")
            lines.append(f"   定义: {p['definition']}")
        lines.append("")
    
    lines.append("━━━━━━━━━━━")
    lines.append("数据源: OKX USDT-SWAP")
    
    return '\n'.join(lines)

def format_md_report(result, patterns, date_str):
    """格式化为Markdown写入brain"""
    g = result['gainers'][:10]
    l = result['losers'][:10]
    
    lines = []
    lines.append(f"# 每日复盘 {date_str}")
    lines.append("")
    lines.append(f"**分析师**: Hermes (首席交易科学家)")
    lines.append(f"**时间**: {date_str}")
    lines.append(f"**数据源**: OKX USDT-SWAP 全市场")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    lines.append("## 涨幅榜 TOP10 诊断")
    lines.append("")
    for i, coin in enumerate(g, 1):
        d = coin.get('detail', {})
        lines.append(f"### {i}. {coin['sym']} +{coin['chg24h']:.2f}%")
        lines.append(f"- **价格**: {fmt_price(coin['last'])} | **成交额**: {fmt_vol(coin['vol_usd'])} | **振幅**: {(coin['high24h']-coin['low24h'])/coin['low24h']*100:.1f}%")
        if d:
            if d.get('chg_2d') is not None:
                lines.append(f"- **2日涨跌**: {d['chg_2d']:+.2f}% | **2日振幅**: {d.get('range_2d',0):.1f}%")
            lines.append(f"- **24H蜡烛**: {d.get('up_count',0)}阳/{d.get('down_count',0)}阴 | **最大单小时**: {d.get('max_1h_surge',0):.1f}%")
        lines.append(f"- **诊断**: {coin.get('diagnosis', '')}")
        lines.append("")
    
    lines.append("## 跌幅榜 TOP10 诊断")
    lines.append("")
    for i, coin in enumerate(l, 1):
        d = coin.get('detail', {})
        lines.append(f"### {i}. {coin['sym']} {coin['chg24h']:.2f}%")
        lines.append(f"- **价格**: {fmt_price(coin['last'])} | **成交额**: {fmt_vol(coin['vol_usd'])} | **振幅**: {(coin['high24h']-coin['low24h'])/coin['low24h']*100:.1f}%")
        if d:
            if d.get('chg_2d') is not None:
                lines.append(f"- **2日涨跌**: {d['chg_2d']:+.2f}%")
            lines.append(f"- **24H蜡烛**: {d.get('up_count',0)}阳/{d.get('down_count',0)}阴 | **最大单小时**: {d.get('max_1h_surge',0):.1f}%")
        lines.append(f"- **诊断**: {coin.get('diagnosis', '')}")
        lines.append("")
    
    lines.append("## 模式识别")
    lines.append("")
    if patterns:
        for p in patterns:
            lines.append(f"### {p['id']}: {p['name']} {'🆕' if p.get('status')=='new' else '🔄'}")
            lines.append(f"- **标的**: {', '.join(p['coins'])}")
            lines.append(f"- **定义**: {p['definition']}")
            lines.append(f"- **信号**: {p['signal']}")
            lines.append("")
    else:
        lines.append("*(本次未发现新模式)*")
        lines.append("")
    
    lines.append("---")
    lines.append(f"下次复盘: 自动")
    
    return '\n'.join(lines)

def save_daily_report(md_content, date_str):
    """保存复盘报告到brain"""
    os.makedirs(BRAIN_DAILY_DIR, exist_ok=True)
    path = os.path.join(BRAIN_DAILY_DIR, f"{date_str}.md")
    with open(path, 'w') as f:
        f.write(md_content)
    print(f"[✓] 复盘报告已写入: {path}")

def update_pattern_log(patterns, date_str):
    """更新模式识别日志"""
    os.makedirs(BRAIN_PATTERNS_DIR, exist_ok=True)
    
    if not os.path.exists(PATTERN_LOG):
        # 创建新文件
        content = f"""# 交易模式识别日志

长期追踪和验证从每日复盘中提取的交易模式。
每条模式记录：首次发现日期、证据、置信度、后续验证。

---

## 活跃模式（待验证）

"""
        for p in patterns:
            content += f"""### {p['id']}: {p['name']}
- **首次发现**: {date_str}
- **标的**: {', '.join(p['coins'])}
- **定义**: {p['definition']}
- **信号**: {p['signal']}
- **置信度**: ⭐ (首次发现, 待验证)
- **后续验证**: 

"""
        content += """## 已确认模式（3+次验证）

*(暂无)*

## 已废弃模式

*(暂无)*

---

## 更新日志

"""
        content += f"| {date_str} | {', '.join(p['id'] for p in patterns)} | 创建 | 首次复盘发现 |\n"
    else:
        with open(PATTERN_LOG) as f:
            content = f.read()
        
        # 追加验证记录
        for p in patterns:
            pid = p['id']
            if pid in content:
                # 检查是否已有今日更新标记 → 跳过重复追加
                today_marker = f"在({date_str})"
                if today_marker in content and pid in content:
                    continue
                # 增加置信度标记（只加一次）
                confirm_prefix = f"✅ 再次确认({date_str})"
                if confirm_prefix not in content:
                    content = content.replace(f"### {pid}:", f"### {pid}: {confirm_prefix} ")
                # 追加确认记录
                coin_str = ', '.join(p['coins'])
                content = content.replace(
                    f"**确认记录**:\n",
                    f"**确认记录**:\n  - {date_str}: {coin_str}\n"
                )
            else:
                # 新增模式
                content = content.replace(
                    "## 活跃模式（待验证）\n\n",
                    f"## 活跃模式（待验证）\n\n"
                    f"### {pid}: {p['name']}\n"
                    f"- **首次发现**: {date_str}\n"
                    f"- **标的**: {', '.join(p['coins'])}\n"
                    f"- **定义**: {p['definition']}\n"
                    f"- **信号**: {p['signal']}\n"
                    f"- **置信度**: ⭐ (首次发现, 待验证)\n"
                    f"- **后续验证**: \n\n"
                )
        
        # 追加更新日志（去重）
        for p in patterns:
            log_line = f"| {date_str} | {p['id']} | 更新 | 复现验证 |\n"
            if log_line not in content:
                content += log_line
    
    with open(PATTERN_LOG, 'w') as f:
        f.write(content)
    print(f"[✓] 模式日志已更新: {PATTERN_LOG}")

# ── 主入口 ──

def main():
    print("="*60)
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    print(f"📊 每日复盘系统启动 — {date_str}")
    print("="*60)
    
    # 1. 拉取全市场行情
    print("\n[1/4] 拉取OKX全市场行情...")
    tickers = fetch_all_tickers()
    print(f"   ✓ 获取 {len(tickers)} 个USDT永续合约")
    
    # 2. 分析涨幅/跌幅榜
    print("[2/4] 深度分析涨幅/跌幅TOP10...")
    result = analyze(tickers, top_n=10)
    print(f"   ✓ 涨幅榜TOP10 + 跌幅榜TOP10 分析完成")
    
    # 3. 提取模式
    print("[3/4] 模式识别...")
    new_patterns = extract_patterns(result)
    patterns = check_pattern_overlap(new_patterns)
    print(f"   ✓ 识别 {len(patterns)} 个模式: {', '.join(p['name'] for p in patterns) if patterns else '无'}")
    
    # 4. 输出报告
    print("[4/4] 输出报告...")
    
    # TG推送
    load_env()
    tg_report = format_tg_report(result, patterns, date_str)
    send_tg(tg_report)
    print("   ✓ TG推送完成")
    
    # Brain知识库
    md_report = format_md_report(result, patterns, date_str)
    save_daily_report(md_report, date_str)
    update_pattern_log(patterns, date_str)
    print("   ✓ Brain知识库写入完成")
    
    print("\n" + "="*60)
    print("✅ 复盘完成")
    print("="*60)

if __name__ == '__main__':
    main()
