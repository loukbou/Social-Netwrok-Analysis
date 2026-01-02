import requests
import pandas as pd
import time
from datetime import datetime
import os

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
SLEEP_TIME = 8
MAX_REPLIES = 3

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "AcademicResearchBot/1.0 (contact: university-research)",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9"
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

    r = requests.get(url, headers=HEADERS, params=params, timeout=10)

    if r.status_code != 200:
        print(f"❌ r/{subreddit} posts failed ({r.status_code})")
        return []

    if "application/json" not in r.headers.get("Content-Type", ""):
        print(f"❌ r/{subreddit} non-JSON response")
        return []

    posts = []
    for item in r.json()["data"]["children"]:
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

    return posts

# =========================
# SCRAPE COMMENTS + REPLIES
# =========================
def scrape_comments(post_id, subreddit):
    url = f"https://www.reddit.com/comments/{post_id}.json"
    r = requests.get(url, headers=HEADERS, timeout=10)

    if r.status_code != 200:
        return []

    if "application/json" not in r.headers.get("Content-Type", ""):
        return []

    results = []
    children = r.json()[1]["data"]["children"]

    for c in children:
        if c["kind"] != "t1":
            continue

        d = c["data"]

        # --------
        # Top-level comment
        # --------
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

        # --------
        # Replies (max 3)
        # --------
        replies = d.get("replies")
        if replies and isinstance(replies, dict):
            reply_children = replies["data"]["children"][:MAX_REPLIES]

            for rpl in reply_children:
                if rpl["kind"] != "t1":
                    continue

                rd = rpl["data"]

                results.append({
                    "type": "reply",
                    "comment_id": rd["id"],
                    "post_id": post_id,
                    "subreddit": subreddit,
                    "author": rd["author"],
                    "body": rd["body"],
                    "score": rd["score"],
                    "parent_id": rd["parent_id"],
                    "created_utc": rd["created_utc"],
                    "created_at": datetime.utcfromtimestamp(
                        rd["created_utc"]
                    ).isoformat()
                })

    return results

# =========================
# PIPELINE FUNCTION (FOR AIRFLOW)
# =========================
def scrape():
    """
    Main entry point for Airflow / Kafka.
    Returns:
        posts: list[dict]
        comments: list[dict]
    """
    all_posts = []
    all_comments = []

    print("🚀 Reddit scraping started...\n")

    for subreddit in SUBREDDITS:
        print(f"📥 Scraping r/{subreddit}")
        posts = scrape_posts(subreddit)
        all_posts.extend(posts)

        for post in posts:
            time.sleep(SLEEP_TIME)
            comments = scrape_comments(post["post_id"], subreddit)
            all_comments.extend(comments)

        time.sleep(SLEEP_TIME)

    return all_posts, all_comments

# =========================
# OPTIONAL: STANDALONE RUN
# =========================
def run_and_save():
    """
    Keeps your original behavior when running the script manually.
    """
    posts, comments = scrape()

    df_posts = pd.DataFrame(posts)
    df_comments = pd.DataFrame(comments)

    df_posts.to_json(
        f"{DATA_DIR}/posts.json",
        orient="records",
        lines=True,
        force_ascii=False
    )

    df_comments.to_json(
        f"{DATA_DIR}/comments.json",
        orient="records",
        lines=True,
        force_ascii=False
    )

    print("\n✅ Scraping finished")
    print(f"Posts collected: {len(df_posts)}")
    print(f"Comments + replies collected: {len(df_comments)}")

# Only executed if run directly, NOT when imported by Airflow
if __name__ == "__main__":
    run_and_save()
