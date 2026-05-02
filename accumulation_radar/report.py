"""TG 报告格式化模块。

纯字符串格式化，不涉及 API 调用。
提供 build_pool_report 和 build_strategy_report 两个入口函数。
"""

from datetime import datetime, timezone, timedelta


# ── 辅助函数 ──────────────────────────────────────────────


def _fmt_vol(v: float) -> str:
    """用 B/M/K 格式显示成交量/市值。"""
    if v >= 1e9:
        return f"{v / 1e9:.1f}B"
    if v >= 1e6:
        return f"{v / 1e6:.1f}M"
    if v >= 1e3:
        return f"{v / 1e3:.0f}K"
    return f"{v:.0f}"


def _fmt_px(v: float) -> str:
    """智能价格格式：按价格自动调整精度。"""
    if v <= 0:
        return "0"
    if v >= 1000:
        return f"{v:.2f}"
    if v >= 100:
        return f"{v:.4f}"
    if v >= 1:
        return f"{v:.6f}"
    if v >= 0.01:
        return f"{v:.8f}"
    # 极小币：去掉前导零后保留 4 位有效数字
    s = f"{v:.10f}".rstrip("0")
    return s


def _fmt_time() -> str:
    """返回北京时间 YYYY-MM-DD HH:MM:ss。"""
    now = datetime.now(timezone(timedelta(hours=8)))
    return now.strftime("%Y-%m-%d %H:%M:%S")


def _fr_trend_icon(curr_fr: float, prev_fr: float | None = None) -> str:
    """费率趋势图标。
    - 无prev: 仅根据费率正负
    - 有prev: 🔥加速(更负) ⬆️回升(变正/减少负) ⬇️变负(正→负)
    """
    if prev_fr is None:
        return "🔥" if curr_fr < -0.0005 else ""
    diff = curr_fr - prev_fr
    if prev_fr >= 0 and curr_fr < 0:
        return "⬇️变负"
    if diff < -0.0001:
        return "🔥加速"
    if diff > 0.0001:
        if curr_fr < 0:
            return "⬆️回升"
        return "⬆️下降"
    if abs(diff) <= 0.0001:
        return "➡️持平"
    return ""


def _heat_status(
    cg_trending: bool, vol_surge: bool, sw_days: int, d6h: float
) -> tuple[str, str]:
    """返回 (emoji前缀, 描述后缀) 用于值得关注模块。
    🔥热度=CG热搜+放量  💤横盘收筹  ⚡OI正在涨
    组合: 🔥💤 热度+收筹预判  🔥⚡ 热度+OI正在发生
    """
    has_heat = cg_trending or vol_surge
    has_sideways = sw_days >= 10
    has_oi_up = d6h > 5

    if has_heat and has_sideways:
        return "🔥💤", "热度+收筹"
    if has_heat and has_oi_up:
        return "🔥⚡", "热度+OI双涨"
    if has_heat:
        return "🔥", "热度"
    if d6h < -0.5 and sw_days > 0:
        return "💤", "收筹"
    return "", ""


# ── Pool 报告 ────────────────────────────────────────────


def build_pool_report(results: list[dict]) -> str:
    """生成 pool 扫描的 TG 完整报告。

    Args:
        results: scan_accumulation_pool 返回的列表，每个元素含
            coin, score, status, sideways_days, range_pct,
            vol_breakout, avg_vol, current_price, low_price, high_price

    Returns:
        格式化的 Telegram 报告字符串。
    """
    if not results:
        return "🏦 池扫描完成，未发现收筹标的。"

    firing = [r for r in results if r["status"] == "firing"]
    warming = [r for r in results if r["status"] == "warming"]
    sleeping = [r for r in results if r["status"] == "sleeping"]

    lines = [
        "🏦 庄家收筹雷达 — Pool 扫描",
        "━━━━━━━━━━━━━━━━━━━━━",
        f"共 {len(firing)} 个 firing | {len(warming)} 个 warming | {len(sleeping)} 个 sleeping",
        "",
    ]

    # 🔥 FIRING
    if firing:
        lines.append("🔥 FIRING（放量启动）")
        for r in firing[:5]:
            coin = r["coin"].replace("USDT", "")
            score = r["score"]
            days = r["sideways_days"]
            rng = r["range_pct"]
            vol_bo = r["vol_breakout"]
            low = _fmt_px(r["low_price"])
            high = _fmt_px(r["high_price"])
            lines.append(
                f"  ${coin}  评分{score}/100  横{days}d  振幅{rng}%"
            )
            lines.append(
                f"  放量{vol_bo}x  区间${low}—${high}"
            )
            lines.append("")
        if lines[-1] == "":
            lines.pop()

    # ⚡ WARMING
    if warming:
        lines.append("")
        lines.append("⚡ WARMING（收筹迹象）")
        for r in warming[:5]:
            coin = r["coin"].replace("USDT", "")
            score = r["score"]
            days = r["sideways_days"]
            rng = r["range_pct"]
            vol_bo = r["vol_breakout"]
            low = _fmt_px(r["low_price"])
            high = _fmt_px(r["high_price"])
            lines.append(
                f"  ${coin}  评分{score}/100  横{days}d  振幅{rng}%"
            )
            lines.append(
                f"  放量{vol_bo}x  区间${low}—${high}"
            )
            lines.append("")
        if lines[-1] == "":
            lines.pop()

    # 💤 SLEEPING
    if sleeping:
        lines.append("")
        lines.append("💤 SLEEPING（横盘观望期）")
        for r in sleeping[:5]:
            coin = r["coin"].replace("USDT", "")
            score = r["score"]
            days = r["sideways_days"]
            rng = r["range_pct"]
            low = _fmt_px(r["low_price"])
            high = _fmt_px(r["high_price"])
            lines.append(
                f"  ${coin}  评分{score}/100  横{days}d  振幅{rng}%"
            )
            lines.append(
                f"  区间${low}—${high}  均量{_fmt_vol(r['avg_vol'])}"
            )
            lines.append("")
        if lines[-1] == "":
            lines.pop()

    # 时间戳 + cashtag
    lines.append("")
    lines.append(f"⏰ {_fmt_time()}")
    lines.append("$BSB $BTC #庄家收筹")

    return "\n".join(lines)


# ── 三策略报告（新排版） ─────────────────────────────────


def build_strategy_report(
    coin_data: dict[str, dict],
    chase: list[dict],
    combined: list[dict],
    ambush: list[dict],
    prev_fr_map: dict | None = None,
    heat_map: dict | None = None,
    new_heat_entries: list | None = None,
) -> str:
    """生成三策略评分的 TG + Square 完整报告。

    排版风格参考：
    - 费率列表：紧凑一行，带趋势图标
    - 综合榜：费率/市值/横盘/OI 各25分
    - 埋伏榜：市值35+OI30+横盘20+费率15
    - 值得关注：热度+收筹组合预判
    - 图例说明收尾

    Args:
        coin_data: build_coin_data 返回的全量数据 dict
        chase: score_chase 返回的追多列表
        combined: score_combined 返回的综合列表
        ambush: score_ambush 返回的埋伏列表
        prev_fr_map: 上一轮费率快照 {sym: fr}
        heat_map: 热度榜 {coin_name: heat_score}
        new_heat_entries: 首次上榜列表

    Returns:
        格式化的报告字符串。
    """
    lines = [
        "📡 抓庄雷达 — 三策略分析",
        "━━━━━━━━━━━━━━━━━━━━━",
        f"监控 {len(coin_data)} 个标的",
        "",
    ]

    # ── 费率列表 ─────────────────────────────────
    # 按费率负值排序，紧凑单行带趋势
    fr_list = []
    for sym, d in coin_data.items():
        fr = d.get("fr", 0.0)
        if fr < -0.0001:
            coin = d.get("coin", sym.replace("USDT", ""))
            px_chg = d.get("px_chg", 0.0)
            mcap = d.get("est_mcap", 0.0)
            vol = d.get("vol", 0.0)
            oi_usd = d.get("oi_usd", 0.0)
            # 费率趋势（如果prev有值）
            fr_trend = ""
            if prev_fr_map and sym in prev_fr_map:
                fr_trend = _fr_trend_icon(fr, prev_fr_map[sym])
            chg_str = f"+{px_chg:.0f}%" if px_chg > 0 else f"{px_chg:.0f}%"
            vol_str = f"~${_fmt_vol(mcap)}" if mcap else f"~${_fmt_vol(oi_usd)}" if oi_usd else ""
            fr_list.append((fr, coin, px_chg, mcap, fr_trend, chg_str, vol_str))

    fr_list.sort(key=lambda x: x[0])  # 最负在前
    if fr_list:
        lines.append("费率异动（空头燃料）")
        for fr, coin, px_chg, mcap, trend, chg_str, vol_str in fr_list[:8]:
            line = f"{coin} 费率{fr*100:+.3f}% "
            if trend:
                line += f"{trend} "
            line += f"| 涨{chg_str} | {vol_str}"
            lines.append(line)
        lines.append("")

    # ── 综合榜 ──────────────────────────────────
    lines.append("📊 综合(费率+市值+横盘+OI各25)")
    if combined:
        for s in combined[:8]:
            coin = s.get("coin", "?")
            total = s.get("total", 0)
            fr_pct = s.get("fr_pct", 0)
            mcap = s.get("est_mcap", 0)
            sw_days = s.get("sw_days", 0)
            d6h = s.get("d6h", 0)
            # 个子分
            f_sc = s.get("f_sc", 0)
            m_sc = s.get("m_sc", 0)
            s_sc = s.get("s_sc", 0)
            o_sc = s.get("o_sc", s.get("o_sc", 0))
            oi_mark = f"⚡OI{'' if d6h>=0 else ''}{d6h:+.0f}%" if abs(d6h) > 0.5 else ""
            lines.append(
                f"{coin} {total}分 | 💎{fr_pct:+.2f}% 💎{_fmt_vol(mcap)} "
                f"💤{sw_days}d {oi_mark}"
            )
    else:
        lines.append("  暂无信号")
    lines.append("")

    # ── 埋伏榜 ──────────────────────────────────
    lines.append("🎯 埋伏(市值35+OI30+横盘20+费率15)")
    if ambush:
        for s in ambush[:8]:
            coin = s.get("coin", "?")
            dc_sc = s.get("dc_sc", 0)
            mcap = s.get("est_mcap", 0)
            d6h = s.get("d6h", 0)
            sw_days = s.get("sw_days", 0)
            fr_pct = s.get("fr_pct", 0)
            px_chg = s.get("px_chg", 0)
            # 暗流标记
            ambush_tag = "🎯暗流" if abs(px_chg) < 2 else ""
            lines.append(
                f"{coin} {dc_sc:.0f}分 | ~${_fmt_vol(mcap)} "
                f"OI{'' if d6h>=0 else ''}{d6h:+.0f}% "
                f"{ambush_tag} "
                f"横盘{sw_days}d "
                f"费率{fr_pct:+.1f}%"
            )
    else:
        lines.append("  暂无信号")
    lines.append("")

    # ── 值得关注 ──────────────────────────────────
    lines.append("💡 值得关注")
    watch_items = []

    # 从combined中筛选热点+收筹组合
    for s in combined:
        coin = s.get("coin", "")
        if not coin:
            continue
        sym = f"{coin}USDT"
        d_entry = coin_data.get(sym, {})
        cg_t = d_entry.get("cg_trending", False)
        vs = d_entry.get("vol_surge", False)
        sw = d_entry.get("sw_days", 0)
        d6h = d_entry.get("d6h", 0)
        fr = d_entry.get("fr", 0)
        total = s.get("total", 0)

        emoji, desc = _heat_status(cg_t, vs, sw, d6h)

        # 有热度才进关注
        if not emoji and total < 65:
            continue

        items_list = []

        # 热度+收筹预判 (🔥💤)
        if cg_t or vs:
            if sw >= 20:
                items_list.append(f"🔥💤 {coin} 热度({'CG热搜' if cg_t else ''}{'+放量' if vs else ''})+收筹{sw}d=OI将涨")
            elif d6h > 5:
                items_list.append(f"🔥⚡ {coin} 热度+OI{'' if d6h>=0 else ''}{d6h:+.0f}%双涨!")

        # 费率加速恶化
        if fr < -0.005:
            items_list.append(f"🔥 {coin} 费率{fr*100:+.3f}%加速恶化，空头涌入中")

        watch_items.extend(items_list)

        # 双榜上榜标记
        coin_in_combined = any(c.get("coin") == coin for c in combined[:5])
        coin_in_ambush = any(a.get("coin") == coin for a in ambush[:5])
        if coin_in_combined and coin_in_ambush:
            items_list.append(f"⭐ {coin} 追多+综合双榜上榜")

    if not watch_items:
        lines.append("  暂无值得关注的信号")
    else:
        for item in watch_items[:10]:
            lines.append(item)

    lines.append("")

    # ── 图例 ──────────────────────────────────
    lines.append("📖 图例")
    lines.append("🔥热度=CG热搜+成交量暴增(OI领先指标)")
    lines.append("费率负=空头燃料 | 💎市值 | 💤横盘(收筹)")
    lines.append("🔥💤热度+收筹=最强预判 | 🔥⚡热度+OI=正在发生")

    # 时间戳
    lines.append("")
    lines.append(f"⏰ {_fmt_time()}")
    lines.append("$BSB $BTC #抓庄雷达")

    return "\n".join(lines)


# ── 涨跌幅异动报告 ─────────────────────────────────────


def build_swing_report(data: dict) -> str:
    """生成机会评分 TG/Square 报告。

    排版优化（2026-04）：
    - 去掉等宽表格（Square 上无效）
    - 每币单独一行，数据紧凑
    - 5m/24h 涨跌、费率、成交额、评分、信号源都在一行

    Args:
        data: score_opportunities 返回值
            {"surge": [{coin, score, sym, swing, chg24h, fr, vol, px, mcap, reasons}, ...],
             "dump":  [{...}, ...],
             "btc_chg24h": float,
             "timestamp": "..."}

    Returns:
        格式化的报告字符串。
    """
    surge = data.get("surge", [])
    dump = data.get("dump", [])
    btc_chg = data.get("btc_chg24h", 0)
    ts = data.get("timestamp", _fmt_time())

    if not surge and not dump:
        return ""

    scanned = data.get("candidates_scanned", 0)
    if scanned:
        lines = [
            "📈 异动机会评分",
            "━━━━━━━━━━━━━━━━━━━━━",
            f"扫描 {scanned} 个候选币  |  BTC 24h: {btc_chg:+.2f}%",
            "",
        ]
    else:
        lines = [
            "📈 异动机会评分",
            "━━━━━━━━━━━━━━━━━━━━━",
            "",
        ]

    # 🟢 做多机会
    if surge:
        lines.append(f"🟢 做多机会（Top{len(surge)}）")
        for i, item in enumerate(surge, 1):
            coin = item["coin"]
            sc = item["score"]
            sw = f"+{item['swing']:.2f}%"
            chg = f"+{item['chg24h']:.2f}%" if item['chg24h'] > 0 else f"{item['chg24h']:.2f}%"
            fr = f"{item['fr']:+.4f}"
            vol = _fmt_vol(item["vol"])
            reasons = item.get("reasons", "")
            lines.append(f"{i}. ${coin}  评分{sc:.1f}  5m:{sw}  24h:{chg}  费{fr}  量{vol}  信号{reasons}")
        lines.append("")

    # 🔴 做空/规避机会
    if dump:
        lines.append(f"🔴 做空/规避机会（Top{len(dump)}）")
        for i, item in enumerate(dump, 1):
            coin = item["coin"]
            sc = item["score"]
            sw = f"{item['swing']:.2f}%"
            chg = f"+{item['chg24h']:.2f}%" if item['chg24h'] > 0 else f"{item['chg24h']:.2f}%"
            fr = f"{item['fr']:+.4f}"
            vol = _fmt_vol(item["vol"])
            reasons = item.get("reasons", "")
            lines.append(f"{i}. ${coin}  评分{sc:.1f}  5m:{sw}  24h:{chg}  费{fr}  量{vol}  信号{reasons}")
        lines.append("")

    # 尾注
    lines.append("💡 评分综合 成交量/异动幅度/24h趋势/费率/CG热度/市值")
    lines.append(f"⏰ {ts}")
    lines.append("$BSB #异动评分")

    return "\n".join(lines)


# ── Swing 静默报告（零候选时推送） ─────────────────────


def build_swing_silent_report(result_info: dict) -> str:
    """生成 Swing 扫描零候选时的摘要推送。

    Args:
        result_info: 包含扫描统计信息的 dict
            {"candidates_scanned": int, "btc_chg24h": float, "timestamp": str}

    Returns:
        格式化的 Telegram 摘要字符串。
    """
    scanned = result_info.get("candidates_scanned", 0)
    btc_chg = result_info.get("btc_chg24h", 0.0)
    ts = result_info.get("timestamp", _fmt_time())

    lines = [
        "📡 庄家收筹雷达 | Swing 扫描",
        f"⏰ {ts}",
        f"📊 扫描 {scanned} 个候选币",
        f"📈 BTC 24h: {btc_chg:+.2f}%",
        "💤 当前盘面平静，无显著短线异动",
    ]
    return "\n".join(lines)
