#!/usr/bin/env python3
"""
square_sentiment.py — 币安广场舆情第四维
=========================================
功能：从币安广场采集创作者帖文数据，统计币种热度和舆情趋势
纯只读模式，不涉及任何发帖操作。

数据流：
  1. W2E排行榜（Top 10）+ 帖子中的@提及 → 扩展创作者池至200+人
  2. 每30分钟轮询所有创作者最新帖子（每页5-10条）
  3. 从帖文中提取 $cashtag + 文本币种提及 → 统计30分钟热度
  4. 输出: data/square_hot_topics.json + data/square_creators_pool.json

依赖: requests (无头浏览器不需要)

输出格式:
  data/square_hot_topics.json:
  {
    "scanned_at": "2026-05-02T17:30:00Z",
    "time_window_min": 30,
    "total_posts_scanned": 850,
    "total_creators_scanned": 200,
    "hot_topics": [
      {"coin": "BTC", "mention_count": 45, "sentiment": "bullish", "source_creators": ["拉哪", "摩托BTC"]},
      ...
    ]
  }
"""

import json, os, sys, time, re, logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── 路径 ──
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
HOT_TOPICS_FILE = DATA_DIR / "square_hot_topics.json"
CREATORS_POOL_FILE = DATA_DIR / "square_creators_pool.json"
W2E_CACHED_FILE = DATA_DIR / "w2e_top_creators.json"

# ── 日志 ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SquareSentiment] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("SquareSentiment")

# ── API 配置 ──
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Referer": "https://www.binance.com/zh-CN/square/WritetoEarn",
    "Content-Type": "application/json",
}

LEADERBOARD_API = "https://www.binance.com/bapi/composite/v2/public/pgc/w2e/earn/lastWeek/leaderboard"
USER_CLIENT_API = "https://www.binance.com/bapi/composite/v3/friendly/pgc/user/client"
POSTS_API_TPL = ("https://www.binance.com/bapi/composite/v2/friendly/pgc/content/"
                 "queryUserProfilePageContentsWithFilter"
                 "?targetSquareUid={uid}&timeOffset=-1&filterType=ALL&pageSize=10")

# 已知用户列表（nickname -> username），用于 user/client API
# 初始种子来自 write_to_earn_crawler.py
SEED_USERNAMES = {
    "拉哪":                          "lanaai",
    "摩托BTC":                       "Square-Creator-1d148bbce7461",
    "محترف عملات رقميه":             "momomomo7171",
    "Crypto_Hu":                     "Square-Creator-4881f759f60a",
    "612 Ceros":                     "hmnghia0612",
    "BlockchainBaller":              "blockchainballer",
    "Anh Tú Jr":                     "Shubeo510",
    "nốt lần này bỏ futures":        "BiBi",
    "Bit_Guru":                      "BiBi",
    "Mike On The Move":              "PANews",
}

# 已知 squareUid 缓存
SEED_UIDS = {
    "拉哪":                          "F6QfEPQTzGQwzplw9tILlA",
    "摩托BTC":                       "IT9eHC2eeo5nPxzWpqIkpg",
    "محترف عملات رقميه":             "BGzelAbjfOwj01wOvfmP5g",
    "Crypto_Hu":                     "FSX1bYijCR_Ri78NUIIZcQ",
    "612 Ceros":                     "qih2C3lk-sCtVX2814j52g",
    "BlockchainBaller":              "ySOOnCzUy7Y_y5YDKU5R8w",
    "Anh Tú Jr":                     "iX4urX4jyPaIR5SOk2qeqg",
    "nốt lần này bỏ futures":        "LjvcB8N40YewlsRXkPb_MA",
    "Bit_Guru":                      "LjvcB8N40YewlsRXkPb_MA",
    "Mike On The Move":              "Wee_Ko-dJVus8ZyYCyP4OA",
}

# 常见币种关键词（用于文本匹配）
COMMON_COINS = {
    "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "DOT", "AVAX", "LINK",
    "MATIC", "ATOM", "UNI", "LTC", "BCH", "TRX", "NEAR", "OP", "ARB", "APT",
    "SUI", "TIA", "INJ", "WIF", "BONK", "PEPE", "FLOKI", "ORDI", "SATS",
    "BSB", "BLUR", "SEI", "STRK", "ZRO", "ENA", "ETHFI", "AEVO", "OMNI",
    "REZ", "NOT", "TON", "DOGS", "HMSTR", "CATI", "NEIRO", "GOAT", "ACT",
    "PNUT", "SPACE", "TAG", "BIO", "LAB", "TRUMP", "MELANIA", "MAGA",
}

CASHTAG_PATTERN = re.compile(r'\$([A-Za-z0-9]{2,10})')
MENTION_PATTERN = re.compile(r'@(\w[\w.]{2,30})')


# ══════════════════════════════════════════════════════════
# 创作者池管理
# ══════════════════════════════════════════════════════════

def load_creators_pool() -> dict:
    """加载已有创作者池"""
    if CREATORS_POOL_FILE.exists():
        try:
            return json.loads(CREATORS_POOL_FILE.read_text())
        except Exception as e:
            log.warning(f"创作者池加载失败: {e}")
    return {"creators": [], "seed_version": 1}


def save_creators_pool(pool: dict):
    """保存创作者池"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CREATORS_POOL_FILE.write_text(json.dumps(pool, ensure_ascii=False, indent=2))


def get_creator_map(pool: dict) -> dict[str, dict]:
    """创作者池 → {nickname: {nickname, uid, username, posts_count, last_scanned, source}}"""
    return {c["nickname"]: c for c in pool.get("creators", [])}


def fetch_leaderboard() -> list[dict]:
    """获取 W2E 周榜（最多10人，不带uid）"""
    import requests
    try:
        r = requests.get(LEADERBOARD_API, headers=HEADERS, timeout=15)
        data = r.json()
        if data.get("code") == "000000":
            items = data["data"]["data"]
            log.info(f"排行榜: {len(items)} 人")
            return items
    except Exception as e:
        log.warning(f"排行榜API失败: {e}")
    return []


def resolve_uid(nickname: str, pool: dict) -> str | None:
    """获取或创建创作者的 squareUid"""
    import requests
    creator_map = get_creator_map(pool)
    if nickname in creator_map and creator_map[nickname].get("uid"):
        return creator_map[nickname]["uid"]
    if nickname in SEED_UIDS:
        return SEED_UIDS[nickname]
    
    # 尝试通过 user/client API 获取
    username = SEED_USERNAMES.get(nickname)
    if not username:
        # 没有 username 就无法通过API获取uid，跳过
        log.debug(f"无 username: {nickname}")
        return None
    
    try:
        r = requests.post(USER_CLIENT_API, headers=HEADERS,
                         json={"username": username, "getFollowCount": True}, timeout=10)
        data = r.json()
        if data.get("code") == "000000":
            uid = data.get("data", {}).get("squareUid")
            if uid:
                SEED_UIDS[nickname] = uid
                return uid
    except Exception as e:
        log.warning(f"user/client API失败 ({nickname}): {e}")
    return None


def fetch_creator_posts(uid: str, max_posts: int = 10) -> list[dict]:
    """拉取创作者的最新帖子"""
    import requests
    url = POSTS_API_TPL.format(uid=uid)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        data = r.json()
        if data.get("code") == "000000":
            contents = data.get("data", {}).get("contents", [])
            posts = []
            for c in contents[:max_posts]:
                body = (c.get("bodyTextOnly") or "").strip()
                if not body:
                    continue
                posts.append({
                    "id": c.get("id"),
                    "body": body,
                    "hashtags": [h.get("name", "") for h in c.get("hashtagList", [])],
                    "created_at": c.get("createTime", ""),
                    "like_count": c.get("likeCount", 0),
                    "view_count": c.get("viewCount", 0),
                    "comment_count": c.get("commentCount", 0),
                })
            return posts
    except Exception as e:
        log.warning(f"帖子API失败 (uid={uid}): {e}")
    return []


def extract_new_creators_from_posts(posts: list[dict]) -> set[str]:
    """从帖文中提取 @mention 的新创作者特征词"""
    mentions = set()
    for p in posts:
        body = p.get("body", "")
        for m in MENTION_PATTERN.findall(body):
            mentions.add(m)
    return mentions


def extract_coin_mentions(posts: list[dict]) -> dict[str, int]:
    """从帖文中提取币种提及频率"""
    coin_counter = Counter()
    for p in posts:
        body = p.get("body", "")
        hashtags = [h.lower() for h in p.get("hashtags", [])]
        
        # 1. $cashtag 模式
        for match in CASHTAG_PATTERN.findall(body):
            coin = match.upper()
            if coin in COMMON_COINS:
                coin_counter[coin] += 1
        
        # 2. 文本直接匹配
        body_upper = body.upper()
        for coin in COMMON_COINS:
            if coin in body_upper:
                coin_counter[coin] += 1
        
        # 3. hashtag 匹配（但不重复计入简写）
        for h in hashtags:
            h_upper = h.upper().lstrip('#')
            if h_upper in COMMON_COINS:
                coin_counter[h_upper] += 1
    
    return dict(coin_counter)


def estimate_sentiment(posts: list[dict], coin: str) -> str:
    """根据帖文内容估计币种情感倾向（简单关键词匹配）"""
    bullish_words = {"涨", "突破", "买入", "看涨", "抄底", "bullish", "buy", "long", "moon", "pump", "起飞"}
    bearish_words = {"跌", "破位", "卖出", "看跌", "割肉", "bearish", "sell", "short", "dump", "崩盘"}
    
    score = 0
    count = 0
    for p in posts:
        body = (p.get("body") or "").lower()
        if coin.lower() in body:
            count += 1
            for w in bullish_words:
                if w in body:
                    score += 1
            for w in bearish_words:
                if w in body:
                    score -= 1
    
    if count == 0:
        return "neutral"
    if score > 0:
        return "bullish"
    elif score < 0:
        return "bearish"
    return "neutral"


# ══════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════

def expand_creators_pool(pool: dict, max_creators: int = 200) -> dict:
    """
    扩展创作者池：
    1. 从排行榜获取新创作者
    2. 从已有创作者的最新帖子中提取 @mention
    """
    creator_map = get_creator_map(pool)
    now = datetime.now(timezone.utc).isoformat()
    
    # Step 1: 从排行榜补充
    leaderboard = fetch_leaderboard()
    if leaderboard:
        for item in leaderboard:
            nickname = item.get("nickname", "")
            if not nickname or nickname in creator_map:
                continue
            # 尝试获取 uid
            uid = resolve_uid(nickname, pool)
            if uid:
                creator_map[nickname] = {
                    "nickname": nickname,
                    "uid": uid,
                    "username": SEED_USERNAMES.get(nickname, ""),
                    "earn": item.get("earn", "0"),
                    "posts_count": 0,
                    "last_scanned": "",
                    "source": "leaderboard",
                    "added_at": now,
                }
                log.info(f"  ➕ 新增创作者(排行榜): {nickname}")
    
    log.info(f"排行榜后创作者池: {len(creator_map)} 人")
    
    # Step 2: 扫描已有创作者帖子，提取 @mention
    batch_size = min(30, max(10, 200 - len(creator_map)))
    creators_to_scan = [c for c in creator_map.values() 
                       if c.get("uid") and not c.get("username") == "BiBi"][:batch_size]
    
    new_mentions = set()
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        seen_uids = set()
        for c in creators_to_scan:
            uid = c.get("uid")
            if uid and uid not in seen_uids:
                seen_uids.add(uid)
                f = executor.submit(fetch_creator_posts, uid, 5)
                futures[f] = c.get("nickname", "?")
        
        for f in as_completed(futures):
            nickname = futures[f]
            try:
                posts = f.result()
                mentions = extract_new_creators_from_posts(posts)
                new_mentions.update(mentions)
            except Exception:
                pass
    
    log.info(f"从帖子中提取到 {len(new_mentions)} 个 @mention")
    
    # 提取到的 mention 目前只有 username 没有 nickname，无法直接加
    # 仅用于未来扩展时的线索
    if new_mentions:
        pool["_pending_mentions"] = list(new_mentions)[:200]
    
    # 整理最终池
    pool["creators"] = list(creator_map.values())[:max_creators]
    pool["updated_at"] = now
    pool["total_creators"] = len(pool["creators"])
    save_creators_pool(pool)
    
    log.info(f"创作者池最终大小: {len(pool['creators'])} 人")
    return pool


def scan_all_creators(pool: dict, max_workers: int = 8) -> tuple[list[dict], dict[str, Counter]]:
    """并行扫描所有创作者最新帖子"""
    creators = [c for c in pool.get("creators", []) if c.get("uid")]
    if not creators:
        log.warning("创作者池为空，无法扫描")
        return [], {}
    
    log.info(f"开始扫描 {len(creators)} 位创作者的最新帖子（{max_workers} 并发）...")
    
    all_posts = []
    creator_coin_map = defaultdict(list)
    seen_uids = set()
    
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for c in creators:
            uid = c.get("uid")
            if uid and uid not in seen_uids:
                seen_uids.add(uid)
                f = executor.submit(fetch_creator_posts, uid, 5)
            futures[f] = c
        
        for f in as_completed(futures):
            creator = futures[f]
            nickname = creator.get("nickname", "?")
            try:
                posts = f.result()
                if posts:
                    all_posts.extend(posts)
                    # 更新最后扫描时间
                    creator["last_scanned"] = datetime.now(timezone.utc).isoformat()
                    creator["posts_count"] = len(posts)
                    
                    # 关联币种
                    coins = extract_coin_mentions(posts)
                    for coin in coins:
                        creator_coin_map[coin].append(nickname)
            except Exception as e:
                log.debug(f"扫描失败 {nickname}: {e}")
    
    elapsed = time.time() - t0
    log.info(f"扫描完成: {len(creators)} 人 × {len(all_posts)} 帖 = {elapsed:.1f}s")
    
    # 保存更新后的池
    log.info(f"准备保存创作者池, 路径={CREATORS_POOL_FILE}")
    save_creators_pool(pool)
    log.info(f"保存后检查存在: {CREATORS_POOL_FILE.exists()}")
    
    return all_posts, dict(creator_coin_map)


def compute_hot_topics(all_posts: list[dict], creator_coin_map: dict) -> list[dict]:
    """计算币种热度排名"""
    total_coin_counter = Counter()
    for p in all_posts:
        coins = extract_coin_mentions([p])
        for coin, count in coins.items():
            total_coin_counter[coin] += count
    
    hot_topics = []
    for coin, count in total_coin_counter.most_common(50):
        sentiment = estimate_sentiment(all_posts, coin)
        source_creators = list(set(creator_coin_map.get(coin, [])))[:5]
        hot_topics.append({
            "coin": coin,
            "mention_count": count,
            "sentiment": sentiment,
            "source_creators": source_creators,
            "mention_share": round(count / max(len(all_posts), 1) * 100, 1),
        })
    
    return hot_topics


def run_sentiment_scan():
    """一次完整的舆情扫描"""
    log.info("=" * 50)
    log.info("🔄 币安广场舆情扫描开始")
    log.info("=" * 50)
    
    # 1. 加载/初始化创作者池
    pool = load_creators_pool()
    if not pool.get("creators"):
        log.info("创作者池为空，初始化种子...")
        pool = {
            "creators": [
                {"nickname": n, "uid": u, "username": SEED_USERNAMES.get(n, ""),
                 "posts_count": 0, "last_scanned": "", "source": "seed", "added_at": datetime.now(timezone.utc).isoformat()}
                for n, u in SEED_UIDS.items()
            ],
            "seed_version": 2,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "total_creators": len(SEED_UIDS),
        }
        save_creators_pool(pool)
    
    # 2. 尝试扩展创作者池
    pool = expand_creators_pool(pool, max_creators=200)
    
    # 3. 并行扫描所有创作者
    all_posts, creator_coin_map = scan_all_creators(pool)
    
    if not all_posts:
        log.warning("未扫到任何帖子，跳过热点统计")
        return {"scanned_at": datetime.now(timezone.utc).isoformat(), "hot_topics": [], "total_posts_scanned": 0}
    
    # 4. 计算热点
    hot_topics = compute_hot_topics(all_posts, creator_coin_map)
    
    # 5. 输出
    result = {
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "time_window_min": 30,
        "total_posts_scanned": len(all_posts),
        "total_creators_scanned": len(pool.get("creators", [])),
        "hot_topics": hot_topics[:50],
        "top10": hot_topics[:10],
    }
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    HOT_TOPICS_FILE.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    
    log.info(f"热点统计完成: {len(hot_topics)} 个币种 | {len(all_posts)} 篇帖子 | {len(pool['creators'])} 位创作者")
    log.info(f"Top 5:")
    for ht in hot_topics[:5]:
        log.info(f"  #{ht['coin']}: {ht['mention_count']}次提及 | 情感:{ht['sentiment']}")
    
    return result


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "loop"
    if mode == "once":
        run_sentiment_scan()
    elif mode == "expand":
        pool = load_creators_pool()
        expand_creators_pool(pool, max_creators=200)
    else:
        log.info("启动30分钟循环模式...")
        while True:
            try:
                run_sentiment_scan()
            except Exception as e:
                log.error(f"舆情扫描异常: {e}", exc_info=True)
            log.info(f"下次运行在 30 分钟后...")
            time.sleep(30 * 60)


if __name__ == "__main__":
    main()
