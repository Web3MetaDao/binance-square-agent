"""
write_to_earn_crawler.py v3 - 纯 HTTP API 方案
流程: 排行榜API -> user/client API 获取 squareUid -> 帖子 API 抓内容
无需 Playwright，速度快，稳定性高
"""
import json, logging, sys, time
from datetime import datetime, timezone
from pathlib import Path
import requests

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [W2E-Crawler] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("W2E-Crawler")

BASE_DIR    = Path(__file__).parent
DATA_DIR    = BASE_DIR / "data"
OUTPUT_FILE = DATA_DIR / "w2e_top_creators.json"
DATA_DIR.mkdir(exist_ok=True)

LEADERBOARD_API = "https://www.binance.com/bapi/composite/v2/public/pgc/w2e/earn/lastWeek/leaderboard"
USER_CLIENT_API = "https://www.binance.com/bapi/composite/v3/friendly/pgc/user/client"
POSTS_API_TPL   = ("https://www.binance.com/bapi/composite/v2/friendly/pgc/content/"
                   "queryUserProfilePageContentsWithFilter"
                   "?targetSquareUid={uid}&timeOffset=-1&filterType=ALL&pageSize=5")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Referer": "https://www.binance.com/zh-CN/square/WritetoEarn",
}

TOP_N    = 10
INTERVAL = 30 * 60

# 已知博主的 username（用于 user/client API）
# 格式: 昵称 -> username
KNOWN_USERNAMES = {
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

# 已知 squareUid 缓存（避免重复调用 user/client API）
KNOWN_UIDS = {
    "拉哪":                          "F6QfEPQTzGQwzplw9tILlA",
    "摩托BTC":                       "IT9eHC2eeo5nPxzWpqIkpg",
    "محترف عملات رقميه":             "BGzelAbjfOwj01wOvfmP5g",
    "Crypto_Hu":                     "FSX1bYijCR_Ri78NUIIZcQ",
    "612 Ceros":                     "qih2C3lk-sCtVX2814j52g",
    "BlockchainBaller":              "ySOOnCzUy7Y_y5YDKU5R8w",
    "Anh Tú Jr":                     "iX4urX4jyPaIR5SOk2qeqg",
    "nốt lần này bỏ futures":        "LjvcB8N40YewlsRXkPb_MA",
    "Bit_Guru":                       "LjvcB8N40YewlsRXkPb_MA",
    "Mike On The Move":              "Wee_Ko-dJVus8ZyYCyP4OA",
}


def fetch_leaderboard():
    resp = requests.get(LEADERBOARD_API, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != "000000":
        raise RuntimeError(f"排行榜API异常: {data}")
    creators = data["data"]["data"][:TOP_N]
    log.info(f"排行榜抓取完成，共 {len(creators)} 位博主")
    for i, c in enumerate(creators, 1):
        log.info(f"  #{i} {c['nickname']}: {c['earn']} USDC")
    return creators


def get_square_uid(nickname):
    """通过 user/client API 获取 squareUid，优先使用缓存"""
    # 优先使用已知 UID 缓存
    if nickname in KNOWN_UIDS:
        return KNOWN_UIDS[nickname]
    
    # 通过 username 调用 API
    username = KNOWN_USERNAMES.get(nickname)
    if not username:
        log.warning(f"  未知博主 {nickname}，无法获取 UID")
        return None
    
    try:
        resp = requests.post(USER_CLIENT_API, headers={**HEADERS, "Content-Type": "application/json"},
                            json={"username": username, "getFollowCount": True}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") == "000000":
            uid = data.get("data", {}).get("squareUid")
            if uid:
                KNOWN_UIDS[nickname] = uid  # 缓存
                return uid
    except Exception as e:
        log.warning(f"  user/client API 失败 ({nickname}): {e}")
    return None


def fetch_posts(uid):
    url = POSTS_API_TPL.format(uid=uid)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != "000000":
            return []
        contents = data.get("data", {}).get("contents", [])
        posts = []
        for c in contents[:5]:
            text = c.get("bodyTextOnly", "").strip()
            if not text:
                continue
            posts.append({
                "id":          c.get("id"),
                "text":        text,
                "like_count":  c.get("likeCount", 0),
                "view_count":  c.get("viewCount", 0),
                "share_count": c.get("shareCount", 0),
                "hashtags":    [h.get("name", "") for h in c.get("hashtagList", [])],
            })
        return posts
    except Exception as e:
        log.warning(f"  帖子API失败 (uid={uid}): {e}")
        return []


def run_crawler():
    log.info(f"=== Write to Earn 爬虫启动 [{datetime.now(timezone.utc).isoformat()}] ===")
    try:
        leaderboard = fetch_leaderboard()
    except Exception as e:
        log.error(f"排行榜抓取失败: {e}")
        return {"creators": [], "fetched_at": datetime.now(timezone.utc).isoformat()}

    result_creators = []
    for i, creator in enumerate(leaderboard, 1):
        nickname = creator["nickname"]
        earn     = creator["earn"]
        log.info(f"处理 #{i} {nickname}")

        uid = get_square_uid(nickname)
        if not uid:
            log.warning(f"  跳过 {nickname}：UID获取失败")
            continue
        log.info(f"  UID: {uid}")

        posts = fetch_posts(uid)
        log.info(f"  抓取到 {len(posts)} 条帖子")

        result_creators.append({
            "rank":      i,
            "nickname":  nickname,
            "earn_usdc": float(earn.replace(",", "")),
            "uid":       uid,
            "posts":     posts,
        })
        time.sleep(0.5)  # 礼貌延迟

    total_posts = sum(len(c["posts"]) for c in result_creators)
    log.info(f"=== 爬虫完成，共 {len(result_creators)} 位博主，{total_posts} 条帖子 ===")

    output = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "creators":   result_creators,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    log.info(f"数据已写入 {OUTPUT_FILE}")
    return output


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "loop"
    if mode == "once":
        run_crawler()
    else:
        log.info("启动30分钟循环调度...")
        while True:
            try:
                run_crawler()
            except Exception as e:
                log.error(f"爬虫异常: {e}")
            log.info(f"下次运行将在 {INTERVAL // 60} 分钟后...")
            time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
