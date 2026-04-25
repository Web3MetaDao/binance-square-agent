"""
感知层（Perception Layer）
===========================
负责从 Twitter/X 和币安广场双端抓取热点数据，
输出标准化的 market_context.json 供内容层使用。

数据源：
  1. Twitter/X 内置 GraphQL API（无需官方 Key）
  2. 币安广场 news/list 接口
  3. 币安 Web3 Social Hype Leaderboard
  4. 币安 Web3 Topic Rush Rank List
"""

import json
import time
import requests
from collections import Counter
from datetime import datetime, timezone

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    KOL_LIST, FUTURES_MAP, MARKET_FILE, DATA_DIR,
    SCAN_INTERVAL_M
)
from core.state import save_state

# Twitter Bearer Token（公开固定值）
BEARER_TOKEN = (
    "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs"
    "%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
)

BINANCE_BASE = "https://www.binance.com"
WEB3_BASE    = "https://web3.binance.com"

COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/json",
    "lang": "zh-CN",
    "clienttype": "web",
}


# ──────────────────────────────────────────────
# A. Twitter/X KOL 扫描器
# ──────────────────────────────────────────────
class TwitterScanner:
    """
    自动获取 guest_token，通过 GraphQL 接口批量抓取
    KOL 最新推文，并计算各代币的加权热度得分。
    """

    def __init__(self, state: dict):
        self.state = state

    def _refresh_guest_token(self) -> str:
        now = time.time()
        cached = self.state.get("guest_token")
        cached_time = self.state.get("guest_token_time", 0)
        if cached and (now - cached_time) < 3000:
            return cached
        r = requests.post(
            "https://api.twitter.com/1.1/guest/activate.json",
            headers={
                "Authorization": f"Bearer {BEARER_TOKEN}",
                "User-Agent": "Mozilla/5.0",
            },
            timeout=12,
        )
        token = r.json()["guest_token"]
        self.state["guest_token"] = token
        self.state["guest_token_time"] = now
        save_state(self.state)
        return token

    def _api_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {BEARER_TOKEN}",
            "x-guest-token": self._refresh_guest_token(),
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "x-twitter-client-language": "en",
            "x-twitter-active-user": "yes",
        }

    def _fetch_kol_tweets(self, kol: dict, count: int = 10) -> list:
        variables = json.dumps({
            "userId": kol["rest_id"],
            "count": count,
            "includePromotedContent": False,
            "withVoice": True,
            "withV2Timeline": True,
        })
        features = json.dumps({
            "responsive_web_graphql_exclude_directive_enabled": True,
            "verified_phone_label_enabled": False,
            "creator_subscriptions_tweet_preview_api_enabled": True,
            "responsive_web_graphql_timeline_navigation_enabled": True,
            "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
            "tweetypie_unmention_optimization_enabled": True,
            "view_counts_everywhere_api_enabled": True,
            "longform_notetweets_consumption_enabled": True,
            "freedom_of_speech_not_reach_fetch_enabled": True,
            "standardized_nudges_misinfo": True,
            "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
            "longform_notetweets_rich_text_read_enabled": True,
            "responsive_web_enhance_cards_enabled": False,
        })
        try:
            r = requests.get(
                "https://twitter.com/i/api/graphql/XicnWRbyQ3WgVY__VataBQ/UserTweets",
                params={"variables": variables, "features": features},
                headers=self._api_headers(),
                timeout=15,
            )
            data = r.json()
            tweets = []
            timeline = (
                data["data"]["user"]["result"]
                    ["timeline_v2"]["timeline"]
            )
            for inst in timeline.get("instructions", []):
                for entry in inst.get("entries", []):
                    item = entry.get("content", {}).get("itemContent", {})
                    result = item.get("tweet_results", {}).get("result", {})
                    legacy = result.get("legacy", {})
                    text = legacy.get("full_text", "")
                    if text and not text.startswith("RT @"):
                        tweets.append({
                            "text": text,
                            "likes": legacy.get("favorite_count", 0),
                            "retweets": legacy.get("retweet_count", 0),
                            "username": kol["username"],
                            "weight": kol["weight"],
                        })
            return tweets[:count]
        except Exception as e:
            print(f"  [Twitter] @{kol['username']} 抓取失败: {e}")
            return []

    def scan(self) -> dict:
        """扫描所有 KOL，返回代币加权热度与原始推文。"""
        coin_scores = Counter()
        raw_tweets = []
        print(f"  [感知层-Twitter] 扫描 {len(KOL_LIST)} 位 KOL...")
        for kol in KOL_LIST:
            tweets = self._fetch_kol_tweets(kol, count=10)
            for t in tweets:
                text_upper = t["text"].upper()
                for coin in FUTURES_MAP:
                    if coin.upper() in text_upper:
                        score = (
                            1
                            + t["likes"] / 1000
                            + t["retweets"] / 200
                        ) * kol["weight"]
                        coin_scores[coin] += score
                raw_tweets.append(t)
            time.sleep(0.8)
        print(f"  [感知层-Twitter] 获取 {len(raw_tweets)} 条推文，识别 {len(coin_scores)} 个热点代币")
        return {
            "coin_scores": dict(coin_scores.most_common(20)),
            "raw_tweets": raw_tweets[:20],
        }


# ──────────────────────────────────────────────
# B. 币安广场热点扫描器
# ──────────────────────────────────────────────
class SquareScanner:
    """
    调用币安广场与 Web3 API，获取：
    - 广场最热帖子（news/list）
    - Social Hype Leaderboard
    - Topic Rush Rank List（热门叙事）
    """

    def _fetch_news_list(self) -> dict:
        coin_scores = Counter()
        hot_posts = []
        try:
            r = requests.post(
                f"{BINANCE_BASE}/bapi/composite/v1/friendly/pgc/news/list",
                headers=COMMON_HEADERS,
                json={"pageIndex": 1, "pageSize": 50},
                timeout=10,
            )
            data = r.json()
            if data.get("code") == "000000":
                for p in data.get("data", {}).get("vos", []):
                    text = (p.get("title") or p.get("content") or "").upper()
                    views = p.get("viewCount", 0)
                    likes = p.get("likeCount", 0)
                    score_base = 1 + views / 500 + likes / 50
                    for coin in FUTURES_MAP:
                        if coin.upper() in text:
                            coin_scores[coin] += score_base
                    if views > 1000:
                        hot_posts.append({
                            "title": (p.get("title") or p.get("content", ""))[:100],
                            "views": views,
                            "likes": likes,
                        })
        except Exception as e:
            print(f"  [感知层-广场] news/list 失败: {e}")
        return {"coin_scores": coin_scores, "hot_posts": hot_posts}

    def _fetch_hype_leaderboard(self) -> dict:
        coin_scores = Counter()
        hype_items = []
        try:
            r = requests.get(
                f"{WEB3_BASE}/bapi/defi/v1/public/social/hype/leaderboard",
                headers=COMMON_HEADERS,
                timeout=10,
            )
            data = r.json()
            if data.get("code") == "000000":
                for item in (data.get("data") or [])[:20]:
                    coin = item.get("symbol", "")
                    score = float(item.get("hypoScore") or 0)
                    sentiment = item.get("sentiment", "")
                    if coin in FUTURES_MAP:
                        coin_scores[coin] += score / 10
                    hype_items.append({
                        "coin": coin,
                        "score": score,
                        "sentiment": sentiment,
                        "summary": (item.get("summary") or "")[:80],
                    })
        except Exception as e:
            print(f"  [感知层-广场] hype/leaderboard 失败: {e}")
        return {"coin_scores": coin_scores, "hype_items": hype_items[:10]}

    def _fetch_topic_rush(self) -> list:
        topics = []
        try:
            r = requests.get(
                f"{WEB3_BASE}/bapi/defi/v1/public/meme/rush/topic/rank/list",
                headers=COMMON_HEADERS,
                params={"limit": 10},
                timeout=10,
            )
            data = r.json()
            if data.get("code") == "000000":
                for item in (data.get("data") or [])[:10]:
                    topics.append({
                        "topic": item.get("topicName", ""),
                        "inflow": item.get("netInflow", 0),
                        "summary": (item.get("summary") or "")[:80],
                    })
        except Exception as e:
            print(f"  [感知层-广场] topic/rush 失败: {e}")
        return topics

    def scan(self) -> dict:
        """综合扫描广场三大数据源，返回标准化结果。"""
        print("  [感知层-广场] 扫描广场热点...")
        news_result  = self._fetch_news_list()
        hype_result  = self._fetch_hype_leaderboard()
        topics       = self._fetch_topic_rush()

        # 合并代币得分
        combined = Counter(news_result["coin_scores"])
        combined.update(hype_result["coin_scores"])

        print(f"  [感知层-广场] 识别 {len(combined)} 个热点代币，{len(topics)} 个热门叙事")
        return {
            "coin_scores": dict(combined.most_common(20)),
            "hot_posts":   news_result["hot_posts"][:5],
            "hype_items":  hype_result["hype_items"],
            "topics":      topics,
        }


# ──────────────────────────────────────────────
# C. 双端共振分析器
# ──────────────────────────────────────────────
def analyze_resonance(tw: dict, sq: dict) -> list:
    """
    将 Twitter 与广场两端的代币热度合并，
    计算综合得分并标注热点等级（S/A/B）。
    """
    tw_scores = tw.get("coin_scores", {})
    sq_scores = sq.get("coin_scores", {})
    all_coins = set(tw_scores) | set(sq_scores)
    result = []
    for coin in all_coins:
        if coin not in FUTURES_MAP:
            continue
        tw_s = tw_scores.get(coin, 0)
        sq_s = sq_scores.get(coin, 0)
        if tw_s > 0 and sq_s > 0:
            combined = (tw_s + sq_s) * 1.5
            tier = "S"
        elif tw_s > 0:
            combined = tw_s
            tier = "A"
        else:
            combined = sq_s
            tier = "B"
        result.append({
            "coin":     coin,
            "futures":  FUTURES_MAP[coin],
            "score":    round(combined, 2),
            "tier":     tier,
            "tw_score": round(tw_s, 2),
            "sq_score": round(sq_s, 2),
        })
    return sorted(result, key=lambda x: x["score"], reverse=True)[:15]


# ──────────────────────────────────────────────
# D. 感知层主入口
# ──────────────────────────────────────────────
def run_perception(state: dict) -> dict:
    """
    执行一次完整的双端感知扫描，
    结果写入 market_context.json 并返回。
    同时触发 Write to Earn 排行榜爬虫（每30分钟刷新一次）。
    同时融合聪明钱信号（Hyperliquid 大户持仓），增强感知层数据。
    """
    print("\n[感知层] ══ 开始双端热点扫描 ══")
    tw_scanner = TwitterScanner(state)
    sq_scanner = SquareScanner()

    # ── Write to Earn 排行榜爬虫（异步触发，不阻塞主流程）──
    _maybe_refresh_w2e_leaderboard(state)

    tw_result = tw_scanner.scan()
    sq_result = sq_scanner.scan()
    resonance = analyze_resonance(tw_result, sq_result)

    # 加载最新的 W2E 排行榜数据
    w2e_data = _load_w2e_data()

    # ── 融合聪明钱信号（异步触发，不阻塞主流程）──
    smart_money_data = _load_smart_money_signals(state)

    market_context = {
        "scanned_at":  datetime.now(timezone.utc).isoformat(),
        "resonance":   resonance,
        "raw_tweets":  tw_result.get("raw_tweets", [])[:10],
        "hot_posts":   sq_result.get("hot_posts", []),
        "hype_items":  sq_result.get("hype_items", []),
        "topics":      sq_result.get("topics", []),
        "w2e_top_creators": w2e_data,  # 内容挖矿排行榜前10博主及其帖子
        "smart_money": smart_money_data,  # 聪明钱信号（Hyperliquid 大户持仓）
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(MARKET_FILE, "w") as f:
        json.dump(market_context, f, indent=2, ensure_ascii=False)

    print(f"[感知层] ══ 扫描完成，识别 {len(resonance)} 个共振代币 ══")
    s_tier = [r for r in resonance if r["tier"] == "S"]
    for item in resonance[:5]:
        print(f"  [{item['tier']}] {item['coin']:8s} → {item['futures']:12s} 综合热度: {item['score']:.1f}")

    # 打印聪明钱信号摘要
    if smart_money_data.get("status") == "ok" and smart_money_data.get("top_signals"):
        print(f"[感知层] 🐋 聪明钱信号: {len(smart_money_data['top_signals'])} 个信号")
        for sig in smart_money_data["top_signals"][:3]:
            icon = "🟢" if sig.get("net_direction") == "LONG" else ("🔴" if sig.get("net_direction") == "SHORT" else "⚪")
            print(f"  {icon} [{sig.get('confidence', '?')}] {sig.get('signal', '')}")

    return market_context


# ──────────────────────────────────────────────
# E. Write to Earn 排行榜集成辅助函数
# ──────────────────────────────────────────────

_W2E_REFRESH_INTERVAL = 30 * 60  # 30分钟（秒）
_w2e_last_refresh = 0.0


def _maybe_refresh_w2e_leaderboard(state: dict):
    """
    检查是否需要刷新 W2E 排行榜数据。
    每30分钟触发一次爬虫，使用独立线程避免阻塞感知层。
    """
    import time
    import threading
    global _w2e_last_refresh

    now = time.time()
    if now - _w2e_last_refresh < _W2E_REFRESH_INTERVAL:
        print(f"[感知层] W2E 排行榜数据未过期，跳过刷新")
        return

    _w2e_last_refresh = now
    print("[感知层] 🏆 触发 Write to Earn 排行榜爬虫（后台线程）...")

    def _crawl():
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
            from write_to_earn_crawler import run_crawler
            run_crawler()
        except Exception as e:
            print(f"[感知层] W2E 爬虫异常: {e}")

    t = threading.Thread(target=_crawl, daemon=True)
    t.start()


def _load_w2e_data() -> dict:
    """
    从 data/w2e_top_creators.json 加载排行榜数据。
    返回精简版数据供内容层参考（仅保留博主名称、收益和帖子文本）。
    """
    w2e_file = DATA_DIR / "w2e_top_creators.json"
    if not w2e_file.exists():
        return {"status": "no_data", "top_creators": []}

    try:
        with open(w2e_file, encoding="utf-8") as f:
            raw = json.load(f)

        # 精简数据，只保留内容层需要的字段
        creators_summary = []
        for c in raw.get("top_creators", []):
            posts_texts = [
                p.get("text", "")[:200]
                for p in c.get("recent_posts", [])[:3]
                if p.get("text")
            ]
            creators_summary.append({
                "rank":         c.get("rank"),
                "nickname":     c.get("nickname"),
                "earnings_usdc": c.get("earnings_usdc"),
                "top_posts":    posts_texts,
                "top_cashtags": list(set(
                    tag
                    for p in c.get("recent_posts", [])
                    for tag in p.get("cashtags", [])
                ))[:5],
            })

        return {
            "status":       "ok",
            "crawled_at":   raw.get("crawled_at", ""),
            "top_creators": creators_summary,
        }
    except Exception as e:
        print(f"[感知层] 加载 W2E 数据失败: {e}")
        return {"status": "error", "top_creators": []}


def load_market_context() -> dict:
    """从文件加载最新的市场上下文数据。"""
    if MARKET_FILE.exists():
        with open(MARKET_FILE) as f:
            return json.load(f)
    return {}


# ──────────────────────────────────────────────
# F. 聪明钱信号集成辅助函数
# ──────────────────────────────────────────────

_SM_REFRESH_INTERVAL = 15 * 60  # 15分钟（秒）
_sm_last_refresh = 0.0


def _load_smart_money_signals(state: dict) -> dict:
    """
    加载聪明钱信号数据，并在后台异步刷新（每15分钟一次）。
    优先使用缓存，避免阻塞主流程。
    """
    import threading
    global _sm_last_refresh

    # 尝试加载缓存信号
    sm_data = _read_smart_money_cache()

    # 检查是否需要后台刷新
    now = time.time()
    if now - _sm_last_refresh >= _SM_REFRESH_INTERVAL:
        _sm_last_refresh = now
        print("[感知层] 🐋 触发聪明钱信号扫描（后台线程）...")

        def _scan():
            try:
                import sys
                import os
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
                from smart_money.smart_money_monitor import aggregate_smart_money_signals
                aggregate_smart_money_signals()
            except Exception as e:
                print(f"[感知层] 聪明钱扫描异常: {e}")

        t = threading.Thread(target=_scan, daemon=True)
        t.start()
    else:
        remaining = (_SM_REFRESH_INTERVAL - (now - _sm_last_refresh)) / 60
        print(f"[感知层] 聪明钱信号未过期，跳过刷新（{remaining:.1f}分钟后刷新）")

    return sm_data


def _read_smart_money_cache() -> dict:
    """
    读取聪明钱信号缓存文件，返回精简版数据供内容层使用。
    """
    sm_cache_file = DATA_DIR / "smart_money_signal.json"
    if not sm_cache_file.exists():
        return {"status": "no_data", "top_signals": [], "content_hints": []}

    try:
        with open(sm_cache_file, encoding="utf-8") as f:
            raw = json.load(f)

        # 检查数据新鲜度（30分钟内有效）
        ts_str = raw.get("timestamp", "")
        if ts_str:
            from datetime import datetime, timezone
            ts = datetime.fromisoformat(ts_str)
            age_min = (datetime.now(timezone.utc) - ts).total_seconds() / 60
            if age_min > 30:
                return {"status": "stale", "age_min": round(age_min, 1),
                        "top_signals": [], "content_hints": []}

        # 精简数据，只保留内容层需要的字段
        top_signals = []
        for sig in raw.get("top_signals", [])[:5]:
            if sig.get("confidence") in ["HIGH", "MEDIUM"]:
                top_signals.append({
                    "coin": sig["coin"],
                    "signal": sig["signal"],
                    "confidence": sig["confidence"],
                    "net_direction": sig["net_direction"],
                    "long_ratio": sig.get("long_ratio", 50),
                    "whale_count": sig.get("whale_count", 0),
                    "total_size_usd": sig.get("total_size_usd", 0),
                    "mark_px": sig.get("mark_px", 0),
                    "change_24h": sig.get("change_24h", 0),
                    "funding_rate": sig.get("funding_rate", 0),
                })

        return {
            "status": "ok",
            "scanned_at": raw.get("timestamp", ""),
            "top_signals": top_signals,
            "content_hints": raw.get("content_hints", [])[:5],
            "scan_summary": raw.get("scan_summary", {}),
        }

    except Exception as e:
        print(f"[感知层] 加载聪明钱信号失败: {e}")
        return {"status": "error", "top_signals": [], "content_hints": []}
