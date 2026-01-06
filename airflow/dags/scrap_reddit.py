import requests
import pandas as pd
import time
from datetime import datetime
import os
import logging

# =========================
# LOGGING (IMPORTANT POUR AIRFLOW)
# =========================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =========================
# CONFIGURATION
# =========================
SUBREDDITS = [
    "Palestine",
    "IsraelPalestine",
    "IsraelUnderAttack"
]

QUERY = "palestine OR israel OR gaza"
LIMIT_POSTS = 20
SLEEP_TIME = 2
MAX_REPLIES = 3

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json"
}

# =========================
# SCRAPE POSTS
# =========================
def scrape_posts(subreddit):
    url = f"https://www.reddit.com/r/{subreddit}/search.json"
    params = {
        "q": QUERY,
        "restrict_sr": 1,
        "limit": LIMIT_POSTS,
        "sort": "new"
    }

    logger.info(f"➡️ Requesting posts from r/{subreddit}")

    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
    except Exception as e:
        logger.error(f"❌ Request error: {e}")
        return []

    logger.info(f"Status code: {r.status_code}")
    logger.info(f"Content-Type: {r.headers.get('Content-Type')}")

    if r.status_code != 200:
        logger.warning("❌ Non-200 response")
        return []

    if "application/json" not in r.headers.get("Content-Type", ""):
        logger.warning("❌ Reddit returned NON-JSON (HTML/blocking)")
        logger.warning(r.text[:300])
        return []

    data = r.json().get("data", {}).get("children", [])
    posts = []

    for item in data:
        p = item["data"]
        posts.append({
            "type": "post",
            "post_id": p["id"],
            "subreddit": subreddit,
            "title": p["title"],
            "selftext": p.get("selftext", ""),
            "author": p["author"],
            "score": p["score"],
            "num_comments": p["num_comments"],
            "created_utc": p["created_utc"],
            "created_at": datetime.utcfromtimestamp(
                p["created_utc"]
            ).isoformat()
        })

    logger.info(f"✅ Posts collected: {len(posts)}")
    return posts

# =========================
# SCRAPE COMMENTS
# =========================
def scrape_comments(post_id, subreddit):
    url = f"https://www.reddit.com/comments/{post_id}.json"
    results = []

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
    except Exception as e:
        logger.error(f"❌ Comment request error: {e}")
        return []

    if r.status_code != 200:
        return []

    if "application/json" not in r.headers.get("Content-Type", ""):
        return []

    children = r.json()[1]["data"]["children"]

    for c in children:
        if c["kind"] != "t1":
            continue

        d = c["data"]
        results.append({
            "type": "comment",
            "comment_id": d["id"],
            "post_id": post_id,
            "subreddit": subreddit,
            "author": d["author"],
            "body": d["body"],
            "score": d["score"],
            "parent_id": d["parent_id"],
            "created_utc": d["created_utc"],
            "created_at": datetime.utcfromtimestamp(
                d["created_utc"]
            ).isoformat()
        })

    return results

# =========================
# PIPELINE ENTRY POINT
# =========================
def scrape():
    all_posts = []
    all_comments = []

    logger.info("🚀 Reddit scraping started")

    for subreddit in SUBREDDITS:
        logger.info(f"📥 Scraping subreddit: r/{subreddit}")

        posts = scrape_posts(subreddit)
        all_posts.extend(posts)

        logger.info(f"🧮 Number of posts to process: {len(posts)}")

        for i, post in enumerate(posts, start=1):
            logger.info(
                f"📝 Processing post {i}/{len(posts)} "
                f"(id={post['post_id']})"
            )

            # Respect Reddit rate limits
            time.sleep(SLEEP_TIME)

            comments = scrape_comments(post["post_id"], subreddit)

            logger.info(f"💬 Comments fetched: {len(comments)}")

            all_comments.extend(comments)

        # Pause between subreddits
        time.sleep(SLEEP_TIME)

    logger.info(f"✅ TOTAL posts: {len(all_posts)}")
    logger.info(f"✅ TOTAL comments: {len(all_comments)}")

    return all_posts, all_comments