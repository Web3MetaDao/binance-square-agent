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
import uuid
import requests
from collections import Counter
from datetime import datetime, timezone

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    KOL_LIST, FUTURES_MAP, MARKET_FILE, DATA_DIR,
    SCAN_INTERVAL_M
)
from core.state import update_state

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
        latest = update_state(lambda current: {
            **current,
            "guest_token": token,
            "guest_token_time": now,
        })
        if latest is not self.state:
            self.state.clear()
            self.state.update(latest)
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

    def _parse_trading_pairs_from_news(self) -> dict:
        """
        从 news/list 的 tradingPairsV2 字段提取热门交易对。
        同一交易对出现次数越多、帖子热度（viewCount/likeCount）越高，得分越高。

        返回:
        {
            "coin_scores": {symbol: score, ...},
            "pair_details": {symbol: {price, change, chain, ...}, ...}
        }
        """
        coin_scores = Counter()
        pair_details = {}
        try:
            r = requests.post(
                f"{BINANCE_BASE}/bapi/composite/v1/friendly/pgc/news/list",
                headers=COMMON_HEADERS,
                json={"pageIndex": 1, "pageSize": 50},
                timeout=10,
            )
            data = r.json()
            if data.get("code") != "000000":
                print(f"  [SquareScanner._parse_trading_pairs_from_news] API 返回异常: {data.get('code')}")
                return {"coin_scores": {}, "pair_details": {}}

            for post in data.get("data", {}).get("vos", []):
                # 帖子热度权重
                views = post.get("viewCount", 0)
                likes = post.get("likeCount", 0)
                post_weight = 1.0 + views / 500.0 + likes / 50.0

                pairs_v2 = post.get("tradingPairsV2")
                if not pairs_v2 or not isinstance(pairs_v2, list):
                    continue

                for pair in pairs_v2:
                    code = pair.get("code", "").upper().strip()
                    if not code:
                        continue
                    # 累加得分：同一帖子内同一币种只计一次（用集合去重）
                    coin_scores[code] += post_weight

                    # 保留首次出现的详细信息（通常最新帖子数据更准确）
                    if code not in pair_details:
                        pair_details[code] = {
                            "symbol": code,
                            "price": pair.get("price"),
                            "price_change": pair.get("priceChange"),
                            "price_change_percent": pair.get("priceChangePercent"),
                            "chain": pair.get("chain"),
                            "contract_address": pair.get("contractAddress"),
                            "high_24h": pair.get("high"),
                            "low_24h": pair.get("low"),
                            "volume": pair.get("volume"),
                        }

            print(f"  [SquareScanner._parse_trading_pairs_from_news] "
                  f"提取 {len(coin_scores)} 个交易对，来自 {len(data.get('data', {}).get('vos', []))} 条帖子")
        except Exception as e:
            print(f"  [SquareScanner._parse_trading_pairs_from_news] 提取失败: {e}")

        return {
            "coin_scores": dict(coin_scores.most_common(30)),
            "pair_details": pair_details,
        }

    def scan(self) -> dict:
        """综合扫描广场数据源，返回标准化结果。"""
        print("  [感知层-广场] 扫描广场热点...")
        news_result  = self._fetch_news_list()
        hype_result  = self._fetch_hype_leaderboard()
        topics       = self._fetch_topic_rush()
        pairs_result = self._parse_trading_pairs_from_news()  # 新增：从tradingPairs提取热门交易对

        # 合并代币得分：传统关键词 + hype榜 + tradingPairs得分
        combined = Counter(news_result["coin_scores"])
        combined.update(hype_result["coin_scores"])
        combined.update(pairs_result.get("coin_scores", {}))

        print(f"  [感知层-广场] 识别 {len(combined)} 个热点代币（含{len(pairs_result.get('pair_details',{}))}个tradingPair），{len(topics)} 个热门叙事")
        return {
            "coin_scores": dict(combined.most_common(20)),
            "hot_posts":   news_result["hot_posts"][:5],
            "hype_items":  hype_result["hype_items"],
            "topics":      topics,
            "trading_pairs": pairs_result.get("pair_details", {}),  # 新增：tradingPairs详细信息
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
# D1. 全市场热门交易对扫描器（多交易所备用）
# ──────────────────────────────────────────────
class MarketHotScanner:
    """全市场热门交易对扫描器
    聚合 OKX (主力) + Gate.io (备用) 的 24h 成交量排行 + 涨跌幅异动，
    提供"市场热点交易对"视角。
    OKX 每批 100 个 ticker，volCcy24h 可换算为成交额。
    """

    EXCHANGES = {
        "okx": {
            "url": "https://www.okx.com/api/v5/market/tickers?instType=SPOT",
            "label": "OKX",
        },
        "gate": {
            "url": "https://api.gateio.ws/api/v4/spot/tickers",
            "label": "Gate.io",
        },
    }

    def _parse_okx(self, data: list) -> list:
        """解析 OKX tickers → 统一格式"""
        pairs = []
        for ticker in data:
            inst_id = ticker.get("instId", "")
            if not inst_id.endswith("-USDT"):
                continue
            symbol = inst_id.replace("-", "")
            base = inst_id.replace("-USDT", "")
            if base in ("USDC", "BUSD", "FDUSD", "TUSD", "DAI", "USD"):
                continue
            try:
                price = float(ticker.get("last", 0))
                open24h = float(ticker.get("open24h", 0))
                chg = ((price - open24h) / open24h * 100) if open24h > 0 else 0.0
                vol_coin = float(ticker.get("volCcy24h", 0))
                vol_usd = vol_coin * price if price > 0 else float(ticker.get("vol24h", 0))
            except (TypeError, ValueError):
                continue
            if price <= 0:
                continue
            pairs.append({
                "symbol": symbol,
                "price": price,
                "price_change_24h": round(chg, 2),
                "volume_usd": vol_usd,
                "source": "OKX",
            })
        return pairs

    def _parse_gate(self, data: list) -> list:
        """解析 Gate.io tickers → 统一格式"""
        pairs = []
        for ticker in data:
            symbol = ticker.get("currency_pair", "")
            if not symbol.endswith("_USDT"):
                continue
            usdt_symbol = symbol.replace("_USDT", "USDT")
            base = symbol.replace("_USDT", "")
            if base in ("USDC", "BUSD", "FDUSD", "TUSD", "DAI", "USD"):
                continue
            try:
                price = float(ticker.get("last", 0))
                chg = float(ticker.get("change_percentage", 0)) if ticker.get("change_percentage") else 0.0
                base_vol = float(ticker.get("base_volume", 0))
                vol_usd = base_vol * price if price > 0 else 0
            except (TypeError, ValueError):
                continue
            if price <= 0:
                continue
            pairs.append({
                "symbol": usdt_symbol,
                "price": price,
                "price_change_24h": round(chg, 2),
                "volume_usd": vol_usd,
                "source": "Gate.io",
            })
        return pairs

    def scan(self) -> dict:
        """
        从多交易所获取全市场 24hr ticker 数据，
        返回成交量 Top20、涨幅 Top10、跌幅 Top10。

        返回:
        {
            "by_volume": [...],
            "by_gainers": [...],
            "by_losers": [...],
            "exchange": "okx",
            "scanned_at": "..."
        }
        """
        result = {
            "by_volume": [],
            "by_gainers": [],
            "by_losers": [],
            "exchange": "",
            "scanned_at": datetime.now(timezone.utc).isoformat(),
        }

        # 按优先级尝试各交易所
        for exch_name, exch_cfg in self.EXCHANGES.items():
            try:
                r = requests.get(exch_cfg["url"], timeout=10)
                if r.status_code != 200:
                    print(f"  [MarketHotScanner] {exch_cfg['label']} HTTP {r.status_code}，尝试备用..")
                    continue

                raw = r.json()
                if exch_name == "okx":
                    if raw.get("code") != "0":
                        continue
                    pairs = self._parse_okx(raw.get("data", []))
                elif exch_name == "gate":
                    pairs = self._parse_gate(raw)
                else:
                    continue

                if not pairs:
                    continue

                print(f"  [MarketHotScanner] {exch_cfg['label']}: {len(pairs)} USDT pairs")

                # 按成交量降序排列
                sorted_vol = sorted(pairs, key=lambda x: x["volume_usd"], reverse=True)
                result["by_volume"] = sorted_vol[:20]

                # 筛选成交量 >= 1M USDT 的交易对，按涨跌幅排序
                liquid = [p for p in pairs if p["volume_usd"] >= 1_000_000]
                if liquid:
                    result["by_gainers"] = sorted(liquid, key=lambda x: x["price_change_24h"], reverse=True)[:10]
                    result["by_losers"] = sorted(liquid, key=lambda x: x["price_change_24h"])[:10]

                result["exchange"] = exch_cfg["label"]
                print(f"  [MarketHotScanner] ✅ {exch_cfg['label']}: volTop={len(result['by_volume'])}, gainers={len(result['by_gainers'])}, losers={len(result['by_losers'])}")
                break  # 成功即跳出

            except Exception as e:
                print(f"  [MarketHotScanner] {exch_cfg['label']} 异常: {e}，尝试备用..")
                continue

        if not result["exchange"]:
            print("  [MarketHotScanner] ⚠️ 所有交易所均失败，返回空结果")

        return result


# ──────────────────────────────────────────────
# D2. 感知层主入口 — 增强版舆情扫描
# ──────────────────────────────────────────────

def run_perception(state: dict) -> dict:
    """
    执行一次完整的双端感知扫描（增强版）：
    - Twitter KOL 扫描（25人扩增版）
    - 币安广场扫描（含 tradingPairs 解析）
    - 全市场热门交易对排行（Binance API v3 24hr ticker）
    - 推特情感分析（正负面判定）
    - TG 信号融合 + 聪明钱信号
    结果写入 market_context.json 并返回。
    """
    print("\n[感知层] ══ 开始双端热点扫描（增强版） ══")
    tw_scanner = TwitterScanner(state)
    sq_scanner = SquareScanner()
    market_scanner = MarketHotScanner()

    # ── Write to Earn 排行榜爬虫（异步触发）──
    _maybe_refresh_w2e_leaderboard(state)

    # ── 并行扫描三大数据源 ──
    tw_result = tw_scanner.scan()
    sq_result = sq_scanner.scan()
    market_result = market_scanner.scan()

    # ── 原始共振分析（Twitter + 广场）──
    resonance = analyze_resonance(tw_result, sq_result)
    resonance_map = {r["coin"]: r for r in resonance}

    # ── 增强维度1：币安全市场热门交易对（成交量排行）──
    market_vol_coins = {}
    for item in market_result.get("by_volume", []):
        symbol = item.get("symbol", "").replace("USDT", "")
        if symbol:
            market_vol_coins[symbol] = {
                "rank_volume": len(market_vol_coins) + 1,
                "volume_usd": item.get("volume_usd", 0),
                "price_change_24h": item.get("price_change_24h", 0),
                "price": item.get("price", 0),
            }

    # 构建市场热度得分（按成交量排名加权：第1名加500分，逐级递减）
    for i, item in enumerate(market_result.get("by_volume", [])):
        symbol = item.get("symbol", "").replace("USDT", "")
        if symbol and symbol in resonance_map:
            vol_boost = max(50, 500 - i * 25)
            resonance_map[symbol]["score"] += vol_boost
            resonance_map[symbol]["volume_rank"] = i + 1
            resonance_map[symbol]["volume_usd"] = item.get("volume_usd", 0)
            resonance_map[symbol]["market_price_change_24h"] = item.get("price_change_24h", 0)

    # ── 增强维度2：推特情感分析 ──
    tweet_sentiments = {}
    for tweet in tw_result.get("raw_tweets", []):
        text = tweet.get("text", "")
        sentiment = analyze_tweet_sentiment(text)
        # 提取推文中提及的代币
        text_upper = text.upper()
        for coin in FUTURES_MAP:
            if coin.upper() in text_upper:
                if coin not in tweet_sentiments:
                    tweet_sentiments[coin] = {"positive": 0, "negative": 0, "neutral": 0, "total": 0}
                tweet_sentiments[coin][sentiment["label"]] += 1
                tweet_sentiments[coin]["total"] += 1

    # 将情感分析结果注入共振列表
    for coin, sent_data in tweet_sentiments.items():
        if coin in resonance_map:
            total = sent_data["total"]
            pos_ratio = sent_data["positive"] / total if total > 0 else 0
            neg_ratio = sent_data["negative"] / total if total > 0 else 0
            resonance_map[coin]["tw_sentiment"] = {
                "positive_count": sent_data["positive"],
                "negative_count": sent_data["negative"],
                "neutral_count": sent_data["neutral"],
                "pos_ratio": round(pos_ratio, 3),
                "neg_ratio": round(neg_ratio, 3),
                "overall": "positive" if pos_ratio > neg_ratio else ("negative" if neg_ratio > pos_ratio else "neutral"),
            }

    # ── 增强维度3：MarketHotScanner 涨幅/跌幅异动 ──
    gainer_coins = {}
    for item in market_result.get("by_gainers", []):
        sym = item.get("symbol", "").replace("USDT", "")
        gainer_coins[sym] = {"price_change": item.get("price_change_24h", 0), "volume": item.get("volume_usd", 0)}
    loser_coins = {}
    for item in market_result.get("by_losers", []):
        sym = item.get("symbol", "").replace("USDT", "")
        loser_coins[sym] = {"price_change": item.get("price_change_24h", 0), "volume": item.get("volume_usd", 0)}

    for coin in resonance_map:
        if coin in gainer_coins:
            resonance_map[coin]["market_signal"] = "surge_gainer"
            resonance_map[coin]["market_price_change"] = gainer_coins[coin]["price_change"]
        elif coin in loser_coins:
            resonance_map[coin]["market_signal"] = "surge_loser"
            resonance_map[coin]["market_price_change"] = loser_coins[coin]["price_change"]

    # ── 融合 TG 信号（同步扫描，增强共振列表）──
    tg_coins = {}
    try:
        from smart_money.telegram_scanner import scan_telegram_signals
        tg_signals = scan_telegram_signals()
        for sig in tg_signals:
            coin = sig.get("coin", "").upper()
            if coin:
                boost = sig.get("priority", 3) * 200
                if coin not in tg_coins or boost > tg_coins[coin]:
                    tg_coins[coin] = boost
        if tg_coins:
            print(f"[感知层] 📡 TG 信号增强: {len(tg_coins)} 个币 +共振权重")
            for coin, boost in sorted(tg_coins.items(), key=lambda x: -x[1])[:5]:
                print(f"       {coin:8s} +{boost:.0f} (priority={(boost//200):.0f})")
    except Exception as e:
        print(f"[感知层] TG 信号扫描失败: {e}")

    # ── 融合 TG 信号币种到共振列表 ──
    if tg_coins:
        for coin, boost in tg_coins.items():
            if coin in resonance_map:
                resonance_map[coin]["score"] += boost
                resonance_map[coin]["tg_boost"] = boost
                resonance_map[coin]["tg_score"] = boost
            else:
                futures = FUTURES_MAP.get(coin, f"{coin}USDT")
                resonance_map[coin] = {
                    "coin": coin,
                    "futures": futures,
                    "score": boost,
                    "tier": "S" if boost >= 500 else "A",
                    "tw_score": 0,
                    "sq_score": 0,
                    "tg_score": boost,
                }

    # ── 重新排序并输出增强版共振列表 ──
    enhanced_resonance = sorted(resonance_map.values(), key=lambda x: x["score"], reverse=True)[:20]

    # ── 加载 W2E / 聪明钱 / Binance Skills ──
    w2e_data = _load_w2e_data()
    smart_money_data = _load_smart_money_signals(state)
    try:
        binance_skill_context = _load_binance_skill_context(state)
    except Exception as e:
        print(f"[感知层] Binance skills 上下文加载失败: {e}")
        binance_skill_context = _empty_binance_skill_context(enabled=False, error=str(e))

    # ── 组装最终输出 ──
    market_context = {
        "scanned_at":  datetime.now(timezone.utc).isoformat(),
        "resonance":   enhanced_resonance,
        # 新增强字段：市场热门交易对数据
        "market_hot": {
            "by_volume_top20": market_result.get("by_volume", []),
            "by_gainers_top10": market_result.get("by_gainers", []),
            "by_losers_top10": market_result.get("by_losers", []),
            "scanned_at": market_result.get("scanned_at", ""),
        },
        # 新增强字段：推特情感汇总
        "tweet_sentiment_summary": {
            coin: data
            for coin, data in sorted(tweet_sentiments.items(), key=lambda x: -x[1]["total"])
        },
        # 原始数据（向后兼容）
        "raw_tweets":  tw_result.get("raw_tweets", [])[:10],
        "hot_posts":   sq_result.get("hot_posts", []),
        "hype_items":  sq_result.get("hype_items", []),
        "topics":      sq_result.get("topics", []),
        "w2e_top_creators": w2e_data,
        "smart_money": smart_money_data,
        "binance_skill_context": binance_skill_context,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(MARKET_FILE, "w") as f:
        json.dump(market_context, f, indent=2, ensure_ascii=False)

    print(f"[感知层] ══ 增强扫描完成 ══")
    print(f"  📊 共振代币: {len(enhanced_resonance)} 个")
    s_tier = [r for r in enhanced_resonance if r.get("tier") == "S"]
    for item in enhanced_resonance[:5]:
        coin = item["coin"]
        score = item["score"]
        tier = item.get("tier", "?")
        signal = item.get("market_signal", "")
        sent = item.get("tw_sentiment", {}).get("overall", "")
        sig_str = f" | 📈涨跌异动" if signal else ""
        sent_str = f" | 💬{sent}" if sent else ""
        print(f"  [{tier}] {coin:8s} → {item.get('futures','?'):12s} 综合热度: {score:.0f}{sig_str}{sent_str}")

    vol_count = len(market_result.get("by_volume", []))
    print(f"  📈 市场成交量Top20: {vol_count} 个交易对")
    if market_result.get("by_gainers"):
        top_gainer = market_result["by_gainers"][0]
        print(f"  🟢 最大涨幅: {top_gainer['symbol']} +{top_gainer['price_change_24h']:.2f}%")
    if tweet_sentiments:
        print(f"  💬 推特情感分析: {len(tweet_sentiments)} 个代币有情感数据")

    if smart_money_data.get("status") == "ok" and smart_money_data.get("top_signals"):
        print(f"[感知层] 🐋 聪明钱信号: {len(smart_money_data['top_signals'])} 个信号")
        for sig in smart_money_data["top_signals"][:3]:
            icon = "🟢" if sig.get("net_direction") == "LONG" else ("🔴" if sig.get("net_direction") == "SHORT" else "⚪")
            print(f"  {icon} [{sig.get('confidence', '?')}] {sig.get('signal', '')}")

    return market_context


# ──────────────────────────────────────────────
# E. Write to Earn 排行榜集成辅助函数
# ──────────────────────────────────────────────

_W2E_REFRESH_INTERVAL = 20 * 60  # 20分钟（秒）
_w2e_last_refresh = 0.0


def _maybe_refresh_w2e_leaderboard(state: dict):
    """
    检查是否需要刷新 W2E 排行榜数据。
    每20分钟触发一次爬虫，使用独立线程避免阻塞感知层。
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
        raw_creators = raw.get("top_creators") or raw.get("creators", [])
        for c in raw_creators:
            posts_texts = [
                p.get("text", "")[:200]
                for p in c.get("recent_posts", c.get("posts", []))[:3]
                if p.get("text")
            ]
            creators_summary.append({
                "rank":         c.get("rank"),
                "nickname":     c.get("nickname"),
                "earnings_usdc": c.get("earnings_usdc", c.get("earn_usdc", 0)),
                "top_posts":    posts_texts,
                "top_cashtags": list(set(
                    tag
                    for p in c.get("recent_posts", c.get("posts", []))
                    for tag in p.get("cashtags", p.get("hashtags", []))
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


def _empty_binance_skill_context(enabled: bool = False, error: str = "") -> dict:
    context = {
        "enabled": bool(enabled),
        "rankings": [],
        "smart_money_signals": [],
        "token_info": {},
        "safety": {},
    }
    if error:
        context["error"] = str(error)
    return context


_BINANCE_SKILL_TIMEOUT = 8
_BINANCE_SKILL_HEADERS = {
    "Accept-Encoding": "identity",
    "User-Agent": "binance-web3/phase1-readonly (Hermes)",
}
_BINANCE_RANK_ENDPOINT = (
    f"{WEB3_BASE}/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/pulse/social/hype/rank/leaderboard/ai"
)
_BINANCE_SIGNAL_ENDPOINT = (
    f"{WEB3_BASE}/bapi/defi/v1/public/wallet-direct/buw/wallet/web/signal/smart-money/ai"
)
_BINANCE_TOKEN_SEARCH_ENDPOINT = (
    f"{WEB3_BASE}/bapi/defi/v5/public/wallet-direct/buw/wallet/market/token/search/ai"
)
_BINANCE_AUDIT_ENDPOINT = (
    f"{WEB3_BASE}/bapi/defi/v1/public/wallet-direct/security/token/audit"
)
_BINANCE_DEFAULT_RANK_CHAIN = "56"
_BINANCE_DEFAULT_SIGNAL_CHAIN = "CT_501"
_BINANCE_TOKEN_SEARCH_CHAINS = "56,8453,CT_501"
_BINANCE_SIGNAL_PAGE_SIZE = 10
_BINANCE_RANK_LIMIT = 5
_BINANCE_PREFERRED_TOKEN_CHAINS = ("56", "8453", "CT_501")


def _safe_float(value, default=0.0):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default



def _safe_int(value, default=0):
    try:
        if value in (None, ""):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default



def _validate_binance_skill_response(response, *, operation: str) -> dict:
    status_code = getattr(response, "status_code", 200)
    if status_code >= 400:
        raise RuntimeError(f"{operation} HTTP {status_code}")

    try:
        payload = response.json()
    except Exception as exc:
        raise RuntimeError(f"{operation} invalid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"{operation} returned non-object payload")

    code = str(payload.get("code") or "")
    if code and code != "000000":
        message = payload.get("message") or payload.get("msg") or "unknown error"
        raise RuntimeError(f"{operation} business error {code}: {message}")

    return payload



def _binance_skill_get(url: str, *, params: dict) -> dict:
    response = requests.get(
        url,
        headers=_BINANCE_SKILL_HEADERS,
        params=params,
        timeout=_BINANCE_SKILL_TIMEOUT,
    )
    return _validate_binance_skill_response(response, operation=f"GET {url}")



def _binance_skill_post(url: str, *, payload: dict, extra_headers: dict | None = None) -> dict:
    headers = dict(_BINANCE_SKILL_HEADERS)
    headers["Content-Type"] = "application/json"
    if extra_headers:
        headers.update(extra_headers)
    response = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=_BINANCE_SKILL_TIMEOUT,
    )
    return _validate_binance_skill_response(response, operation=f"POST {url}")



def _fetch_crypto_market_rank() -> list:
    payload = _binance_skill_get(
        _BINANCE_RANK_ENDPOINT,
        params={
            "chainId": _BINANCE_DEFAULT_RANK_CHAIN,
            "sentiment": "All",
            "socialLanguage": "ALL",
            "targetLanguage": "zh",
            "timeRange": 1,
        },
    )
    leaderboards = (payload.get("data") or {}).get("leaderBoardList", [])
    rankings = []
    for item in leaderboards[:_BINANCE_RANK_LIMIT]:
        meta = item.get("metaInfo") or {}
        market = item.get("marketInfo") or {}
        social = item.get("socialHypeInfo") or {}
        symbol = (meta.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        rankings.append({
            "symbol": symbol,
            "chain_id": str(meta.get("chainId") or ""),
            "contract_address": meta.get("contractAddress") or "",
            "market_cap": _safe_float(market.get("marketCap")),
            "price_change": _safe_float(market.get("priceChange")),
            "social_hype": _safe_float(social.get("socialHype")),
            "sentiment": social.get("sentiment") or "",
            "summary": (
                social.get("socialSummaryBriefTranslated")
                or social.get("socialSummaryBrief")
                or ""
            )[:160],
        })
    return rankings



def _fetch_trading_signal() -> list:
    payload = _binance_skill_post(
        _BINANCE_SIGNAL_ENDPOINT,
        payload={
            "smartSignalType": "",
            "page": 1,
            "pageSize": _BINANCE_SIGNAL_PAGE_SIZE,
            "chainId": _BINANCE_DEFAULT_SIGNAL_CHAIN,
        },
    )
    signals = payload.get("data") or []
    normalized = []
    for item in signals[:_BINANCE_RANK_LIMIT]:
        symbol = (item.get("ticker") or "").upper().strip()
        if not symbol:
            continue
        normalized.append({
            "symbol": symbol,
            "chain_id": str(item.get("chainId") or ""),
            "contract_address": item.get("contractAddress") or "",
            "direction": (item.get("direction") or "").upper(),
            "smart_money_count": _safe_int(item.get("smartMoneyCount"), default=0),
            "signal_count": _safe_int(item.get("signalCount"), default=0),
            "status": item.get("status") or "",
            "max_gain": _safe_float(item.get("maxGain")),
            "exit_rate": _safe_float(item.get("exitRate")),
        })
    return normalized



def _normalize_token_match(match: dict, symbol: str) -> dict:
    return {
        "symbol": (match.get("symbol") or symbol).upper().strip(),
        "name": match.get("name") or "",
        "chain_id": str(match.get("chainId") or ""),
        "contract_address": match.get("contractAddress") or "",
        "price": _safe_float(match.get("price")),
        "percent_change_24h": _safe_float(match.get("percentChange24h")),
        "volume_24h": _safe_float(match.get("volume24h")),
        "market_cap": _safe_float(match.get("marketCap")),
        "liquidity": _safe_float(match.get("liquidity")),
        "holders_top10_percent": _safe_float(match.get("holdersTop10Percent")),
        "links": match.get("links") or [],
    }



def _fetch_token_info(symbol: str) -> dict:
    payload = _binance_skill_get(
        _BINANCE_TOKEN_SEARCH_ENDPOINT,
        params={
            "keyword": symbol,
            "chainIds": _BINANCE_TOKEN_SEARCH_CHAINS,
            "orderBy": "volume24h",
        },
    )
    requested_symbol = (symbol or "").upper().strip()
    matches = payload.get("data") or []
    if not requested_symbol or not matches:
        return {}

    exact_matches = []
    for match in matches:
        normalized = _normalize_token_match(match, requested_symbol)
        if normalized["symbol"] == requested_symbol and normalized["contract_address"]:
            exact_matches.append(normalized)

    if len(exact_matches) != 1:
        return {}

    selected = exact_matches[0]
    if selected["chain_id"] not in _BINANCE_PREFERRED_TOKEN_CHAINS:
        return {}
    return selected



def _evaluate_audit_status(audit_data: dict) -> dict:
    if not audit_data.get("hasResult") or not audit_data.get("isSupported"):
        return {
            "status": "WARN_MANUAL_REVIEW",
            "risk_level": None,
            "risk_level_enum": "UNAVAILABLE",
            "buy_tax": None,
            "sell_tax": None,
            "is_verified": None,
            "risk_items": [],
        }

    extra = audit_data.get("extraInfo") or {}
    risk_level = _safe_int(audit_data.get("riskLevel"), default=None)
    buy_tax = _safe_float(extra.get("buyTax"), default=None)
    sell_tax = _safe_float(extra.get("sellTax"), default=None)
    status = "PASS"
    if risk_level in (4, 5):
        status = "BLOCK"
    elif risk_level in (2, 3):
        status = "WARN_MANUAL_REVIEW"
    if (buy_tax is not None and buy_tax > 10) or (sell_tax is not None and sell_tax > 10):
        status = "BLOCK"
    elif status == "PASS" and (
        (buy_tax is not None and buy_tax >= 5) or (sell_tax is not None and sell_tax >= 5)
    ):
        status = "WARN_MANUAL_REVIEW"

    risk_items = []
    for item in audit_data.get("riskItems") or []:
        for detail in item.get("details") or []:
            if detail.get("isHit"):
                risk_items.append({
                    "category": item.get("id") or "",
                    "title": detail.get("title") or "",
                    "risk_type": detail.get("riskType") or "",
                })
                if detail.get("riskType") == "RISK":
                    status = "BLOCK"
                elif status == "PASS":
                    status = "WARN_MANUAL_REVIEW"

    return {
        "status": status,
        "risk_level": risk_level,
        "risk_level_enum": audit_data.get("riskLevelEnum") or "",
        "buy_tax": buy_tax,
        "sell_tax": sell_tax,
        "is_verified": extra.get("isVerified"),
        "risk_items": risk_items,
    }



def _fetch_token_audit(token_details: dict) -> dict:
    if not token_details.get("contract_address") or not token_details.get("chain_id"):
        return {}
    payload = _binance_skill_post(
        _BINANCE_AUDIT_ENDPOINT,
        payload={
            "binanceChainId": token_details["chain_id"],
            "contractAddress": token_details["contract_address"],
            "requestId": str(uuid.uuid4()),
        },
        extra_headers={"source": "agent"},
    )
    return _evaluate_audit_status(payload.get("data") or {})



def _load_binance_skill_context(state: dict) -> dict:
    """
    真实只读 Binance skills 上下文入口。
    仅调用白名单公共只读接口；任何单点失败都降级，不阻断主链路。
    """
    del state
    context = _empty_binance_skill_context(enabled=True)
    errors = []

    try:
        context["rankings"] = _fetch_crypto_market_rank()
    except Exception as e:
        errors.append(f"rankings: {e}")

    try:
        context["smart_money_signals"] = _fetch_trading_signal()
    except Exception as e:
        errors.append(f"smart_money_signals: {e}")

    symbols = []
    for item in context["rankings"]:
        symbol = item.get("symbol")
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    for item in context["smart_money_signals"]:
        symbol = item.get("symbol")
        if symbol and symbol not in symbols:
            symbols.append(symbol)

    for symbol in symbols[:_BINANCE_RANK_LIMIT]:
        try:
            token_details = _fetch_token_info(symbol)
            if not token_details:
                continue
            context["token_info"][symbol] = token_details
        except Exception as e:
            errors.append(f"token_info:{symbol}: {e}")
            continue

        try:
            audit = _fetch_token_audit(token_details)
            if audit:
                context["safety"][symbol] = audit
        except Exception as e:
            errors.append(f"safety:{symbol}: {e}")

    if errors:
        context["error"] = "; ".join(errors)
    if not context["rankings"] and not context["smart_money_signals"]:
        context["enabled"] = False
    return context


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


# ──────────────────────────────────────────────
# G. 推文情感分析函数
# ──────────────────────────────────────────────

def analyze_tweet_sentiment(text: str) -> dict:
    """
    基于关键词的推文情感分析。
    支持中文 + 英文关键词。

    返回:
    {
        "label": "positive" / "negative" / "neutral",
        "score": float(0-1)  # 积极概率（positive时为正值，negative时为负值偏移，neutral时为0.5附近）
    }
    """
    if not text or not isinstance(text, str):
        return {"label": "neutral", "score": 0.5}

    text_lower = text.lower()

    # ── 积极关键词库（中英文） ──
    positive_keywords = [
        # 英文
        "bullish", "buy", "long", "pump", "moon", "rocket", "breakout",
        "uptrend", "accumulate", "strong", "green", "profit", "gain",
        "ath", "all-time high", "soar", "surge", "explode", "whale",
        "hodl", "diamond hands", "support", "oversold", "bottom",
        "reversal", "partnership", "adoption", "mainnet", "upgrade",
        "positive", "growth", "outperform", "beat", "win", "free",
        "airdrop", "reward", "stake", "yield", "passive income",
        # 中文
        "看涨", "买入", "做多", "暴涨", "起飞", "突破", "反弹",
        "底部", "支撑", "利好", "利多", "看多", "抄底", "追涨",
        "牛市", "牛回", "大涨", "涨停", "拉升", "主力买", "资金流入",
        "增持", "回购", "分红", "空投", "质押", "挖矿", "收益",
        "合作", "上线", "主网", "升级", "通过", "批准", "超预期",
        "增长", "盈利", "新高", "历史新高", "百倍", "千倍",
    ]

    # ── 消极关键词库（中英文） ──
    negative_keywords = [
        # 英文
        "bearish", "sell", "short", "dump", "crash", "drop", "fall",
        "downtrend", "distribution", "weak", "red", "loss", "decline",
        "liquidation", "rug", "scam", "hack", "exploit", "attack",
        "panic", "fud", "fear", "uncertainty", "doubt", "risk",
        "overbought", "resistance", "reject", "bear", "correction",
        "blood", "capitulation", "insolvent", "bankrupt", "freeze",
        "withdraw", "suspend", "delist", "ban", "regulate", "crackdown",
        "fine", "penalty", "lawsuit", "fraud", "manipulation",
        # 中文
        "看跌", "卖出", "做空", "暴跌", "崩盘", "下跌", "回调",
        "顶部", "压力", "利空", "看空", "割肉", "止损", "爆仓",
        "熊市", "熊来了", "大跌", "跌停", "砸盘", "主力卖", "资金流出",
        "减持", "套现", "跑路", "骗局", "黑客", "攻击", "盗币",
        "漏洞", "暂停", "下架", "禁止", "监管", "打击", "罚款",
        "诉讼", "欺诈", "风险", "警告", "破产", "清算", "归零",
        "缩水", "蒸发", "腰斩", "破发",
    ]

    # 计算匹配数
    pos_count = sum(1 for kw in positive_keywords if kw in text_lower)
    neg_count = sum(1 for kw in negative_keywords if kw in text_lower)

    total = pos_count + neg_count
    if total == 0:
        return {"label": "neutral", "score": 0.5}

    # 积极比率映射到 0~1：积极倾向越强越接近1，消极倾向越强越接近0
    pos_ratio = pos_count / total

    if pos_ratio > 0.6:
        label = "positive"
        score = 0.5 + 0.5 * (pos_ratio - 0.6) / 0.4  # 0.6→0.5, 1.0→1.0
    elif neg_count / total > 0.6:
        label = "negative"
        score = 0.5 - 0.5 * (neg_count / total - 0.6) / 0.4  # 0.6→0.5, 1.0→0.0
    else:
        label = "neutral"
        score = 0.5

    # 确保边界
    score = max(0.0, min(1.0, score))

    return {"label": label, "score": round(score, 4)}
