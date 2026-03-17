"""
北美女性消费者洞察 - 自动数据采集脚本
Auto-collector for NA Women Consumer Insights Dashboard

使用方式:
  python collector.py              # 运行一次采集
  python collector.py --schedule   # 每日定时采集 (每天早上9点)

无需API Key，使用Reddit公开JSON接口 + 新闻RSS源
"""

import json
import os
import time
import re
import sys
import argparse
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import quote

# ============================================================
# 配置
# ============================================================
DATA_DIR = Path(__file__).parent
DATA_FILE = DATA_DIR / "data.json"
ARCHIVE_DIR = DATA_DIR / "archive"

# Reddit子版块配置 - 与目标人群相关
SUBREDDITS = [
    # 女性社群
    {"name": "AskWomenOver30", "category_hints": ["topic", "pain", "hobby"]},
    {"name": "AskWomen", "category_hints": ["topic", "hobby"]},
    {"name": "TwoXChromosomes", "category_hints": ["topic", "pain"]},
    {"name": "TheGirlSurvivalGuide", "category_hints": ["topic", "pay"]},
    # 职场妈妈
    {"name": "workingmoms", "category_hints": ["pain", "topic"]},
    {"name": "Mommit", "category_hints": ["pain", "pay"]},
    {"name": "beyondthebump", "category_hints": ["pain"]},
    # 美容护肤
    {"name": "30PlusSkinCare", "category_hints": ["pay", "brand"]},
    {"name": "SkincareAddiction", "category_hints": ["pay", "brand"]},
    # 健康健身
    {"name": "xxfitness", "category_hints": ["hobby", "pay"]},
    {"name": "WellnessOver30", "category_hints": ["premium", "topic"]},
    # 消费与生活方式
    {"name": "FrugalFemaleFashion", "category_hints": ["pay", "brand"]},
    {"name": "femalefashionadvice", "category_hints": ["emotional", "brand"]},
    {"name": "SubscriptionBoxes", "category_hints": ["emotional", "brand"]},
]

# 搜索关键词 - 用于发现更精准的内容
SEARCH_QUERIES = [
    "worth the money women",
    "best purchase self care",
    "favorite subscription box",
    "new brand discovery",
    "splurge worth it",
    "pain point frustration",
    "wish there was a product",
    "changed my life product",
    "guilty pleasure shopping",
    "treat yourself",
    "luxury worth it",
    "wellness routine",
    "mom hack",
    "work life balance struggle",
]

# 分类关键词映射 (重新平衡: 减少topic通用词, 增强其他分类的精准词)
CATEGORY_KEYWORDS = {
    "pain": [
        "frustrated", "struggle", "exhausted", "burnout", "guilt",
        "overwhelmed", "pain point", "rant", "vent",
        "can't afford", "childcare cost", "mental load", "discrimination",
        "unfair", "anxiety", "depressed", "crying", "impossible",
        "divorce", "toxic", "gaslighting", "burden", "debt"
    ],
    "topic": [
        "trend", "discussion", "opinion", "thoughts on", "what do you think",
        "anyone else", "is it just me", "hot take", "unpopular opinion",
        "economy", "inflation", "social media", "AI"
    ],
    "hobby": [
        "hobby", "started", "learning", "class", "craft", "garden",
        "cooking", "baking", "reading", "book club", "fitness", "workout",
        "yoga", "hiking", "knitting", "sewing", "painting", "pottery",
        "travel", "camping", "photography", "music", "dance", "running",
        "meditation", "journaling", "embroidery", "crochet", "sourdough",
        "pickleball", "pilates", "climbing", "cycling", "concert"
    ],
    "pay": [
        "worth it", "best purchase", "invest in", "splurge", "recommend",
        "game changer", "changed my life", "must have", "holy grail",
        "subscription", "willing to pay", "spend money on", "buy",
        "worth every penny", "save money", "budget", "affordable",
        "dupe", "amazon find", "money well spent", "repurchase",
        "cost effective", "value for money", "deal", "sale"
    ],
    "brand": [
        "brand", "new product", "just launched", "discovered", "startup",
        "indie brand", "small business", "DTC", "direct to consumer",
        "alternative to", "switch to", "replaced", "company",
        "founder", "kickstarter", "new line", "collab", "collection",
        "limited edition", "drop", "restock", "sold out", "waitlist"
    ],
    "emotional": [
        "treat yourself", "guilty pleasure", "retail therapy", "unboxing",
        "haul", "shopping spree", "joy", "happy", "mood", "self care",
        "pamper", "indulge", "aesthetic", "vibe", "comfort buy",
        "dopamine", "serotonin", "cozy", "comfort", "nostalgia",
        "reward", "deserve", "pick me up", "feel good", "obsessed",
        "addicted to", "can't stop buying", "impulse", "splurge worthy"
    ],
    "premium": [
        "premium", "high end", "quality over quantity", "upgrade",
        "investment piece", "spa", "wellness", "retreat", "organic",
        "artisan", "handmade", "custom", "personalized", "bespoke",
        "concierge", "first class", "five star", "michelin",
        "members only", "exclusive", "curated", "elevated", "luxe",
        "boutique", "fine dining", "private", "VIP", "platinum"
    ]
}

# 分类互斥优先级 (当多个分类得分相同时, 优先选择排在前面的)
CATEGORY_PRIORITY = ["pay", "brand", "emotional", "premium", "pain", "hobby", "topic"]

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


# ============================================================
# Reddit 数据采集 (无需API Key)
# ============================================================
def fetch_reddit_json(url, retries=3):
    """通过Reddit的公开JSON接口获取数据"""
    if not url.endswith(".json"):
        url = url.rstrip("/") + ".json"

    headers = {"User-Agent": USER_AGENT}
    req = Request(url, headers=headers)

    for attempt in range(retries):
        try:
            with urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data
        except HTTPError as e:
            if e.code == 429:  # Rate limited
                wait = 5 * (attempt + 1)
                print(f"  [限速] 等待 {wait}s 后重试...")
                time.sleep(wait)
            elif e.code == 403:
                print(f"  [403] 被拒绝: {url}")
                return None
            else:
                print(f"  [HTTP {e.code}] {url}")
                return None
        except (URLError, Exception) as e:
            print(f"  [错误] {e}")
            if attempt < retries - 1:
                time.sleep(3)
            else:
                return None
    return None


def fetch_subreddit_posts(subreddit, sort="hot", limit=15):
    """获取子版块的热门帖子"""
    url = f"https://www.reddit.com/r/{subreddit}/{sort}.json?limit={limit}"
    print(f"  采集 r/{subreddit} ({sort})...")
    data = fetch_reddit_json(url)

    if not data or "data" not in data:
        return []

    posts = []
    for child in data["data"].get("children", []):
        post = child.get("data", {})
        if post.get("stickied"):
            continue
        if post.get("score", 0) < 5:
            continue

        posts.append({
            "title": post.get("title", ""),
            "selftext": post.get("selftext", "")[:1000],
            "score": post.get("score", 0),
            "num_comments": post.get("num_comments", 0),
            "url": f"https://www.reddit.com{post.get('permalink', '')}",
            "created_utc": post.get("created_utc", 0),
            "subreddit": subreddit,
        })

    time.sleep(1.5)  # 避免限速
    return posts


def fetch_subreddit_search(subreddit, query, limit=10):
    """在子版块中搜索特定关键词"""
    encoded_query = quote(query)
    url = f"https://www.reddit.com/r/{subreddit}/search.json?q={encoded_query}&restrict_sr=1&sort=relevance&t=month&limit={limit}"
    data = fetch_reddit_json(url)

    if not data or "data" not in data:
        return []

    posts = []
    for child in data["data"].get("children", []):
        post = child.get("data", {})
        if post.get("score", 0) < 3:
            continue
        posts.append({
            "title": post.get("title", ""),
            "selftext": post.get("selftext", "")[:1000],
            "score": post.get("score", 0),
            "num_comments": post.get("num_comments", 0),
            "url": f"https://www.reddit.com{post.get('permalink', '')}",
            "created_utc": post.get("created_utc", 0),
            "subreddit": subreddit,
        })

    time.sleep(1.5)
    return posts


# ============================================================
# 内容分类与摘要
# ============================================================
def classify_post(post, subreddit_hints=None):
    """根据关键词对帖子进行分类 (使用优先级打破平局)"""
    text = (post["title"] + " " + post.get("selftext", "")).lower()
    scores = {}

    for category, keywords in CATEGORY_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in text)
        if subreddit_hints and category in subreddit_hints:
            score += 1.5  # 子版块暗示加权 (降低以减少偏差)
        scores[category] = score

    if not scores or max(scores.values()) == 0:
        return subreddit_hints[0] if subreddit_hints else "topic"

    max_score = max(scores.values())
    # 如果有多个分类得分相同, 用优先级打破平局 (避免全部归入topic)
    tied = [c for c, s in scores.items() if s == max_score]
    if len(tied) > 1:
        for c in CATEGORY_PRIORITY:
            if c in tied:
                return c
    return max(scores, key=scores.get)


def extract_key_quote(text, max_len=200):
    """提取帖子中最有洞察力的句子作为引用"""
    if not text:
        return ""

    sentences = re.split(r'[.!?]+', text)
    # 优先选择包含情感/观点的句子
    opinion_words = ["I think", "I feel", "I wish", "I hate", "I love",
                     "worth", "changed", "best", "worst", "never", "always",
                     "honestly", "seriously", "literally"]

    for sent in sentences:
        sent = sent.strip()
        if len(sent) > 30 and any(w.lower() in sent.lower() for w in opinion_words):
            return sent[:max_len]

    # 返回最长的有意义句子
    meaningful = [s.strip() for s in sentences if len(s.strip()) > 30]
    if meaningful:
        return meaningful[0][:max_len]

    return text[:max_len]


def generate_title_zh(post, category):
    """生成简短的中文标题"""
    title = post["title"]
    # 基于分类给出前缀
    prefix_map = {
        "pain": "痛点发现",
        "topic": "热议话题",
        "hobby": "兴趣趋势",
        "pay": "消费洞察",
        "brand": "品牌发现",
        "emotional": "情绪消费",
        "premium": "优质体验",
    }
    prefix = prefix_map.get(category, "洞察")
    # 截取英文标题核心
    short_title = title[:80] if len(title) > 80 else title
    return f"[{prefix}] {short_title}"


def generate_summary_zh(post, category):
    """生成中文摘要 (基于规则的简要翻译提示)"""
    title = post["title"]
    text = post.get("selftext", "")[:500]
    subreddit = post.get("subreddit", "")
    score = post.get("score", 0)
    comments = post.get("num_comments", 0)

    summary = f"来自r/{subreddit}社区（{score}赞, {comments}评论）。"

    # 基于内容添加摘要
    if category == "pain":
        summary += f"用户分享了她们面临的困扰和挑战。"
    elif category == "topic":
        summary += f"社区热议话题，引发大量讨论。"
    elif category == "hobby":
        summary += f"用户分享了她们的兴趣爱好和新发现。"
    elif category == "pay":
        summary += f"用户讨论了值得投资的产品或服务。"
    elif category == "brand":
        summary += f"用户推荐或讨论了新品牌和新产品。"
    elif category == "emotional":
        summary += f"用户分享了带来情绪价值的消费体验。"
    elif category == "premium":
        summary += f"用户讨论了优质体验和高端产品。"

    summary += f" 原帖标题：\"{title[:100]}\""
    if text:
        summary += f" 摘要：{text[:200]}..."

    return summary


def post_to_insight(post, category, idx):
    """将Reddit帖子转换为洞察数据格式"""
    quote = extract_key_quote(post.get("selftext", ""))
    created = datetime.fromtimestamp(post.get("created_utc", time.time()))

    # 生成唯一ID
    url_hash = hashlib.md5(post["url"].encode()).hexdigest()[:8]

    return {
        "id": f"auto_{url_hash}",
        "category": category,
        "title_zh": generate_title_zh(post, category),
        "title_en": post["title"][:120],
        "summary_zh": generate_summary_zh(post, category),
        "summary_en": post.get("selftext", "")[:300] or post["title"],
        "quote_en": quote,
        "source_type": "reddit",
        "source_url": post["url"],
        "tags": generate_tags(post, category),
        "date": created.strftime("%Y-%m-%d"),
    }


def generate_tags(post, category):
    """为帖子生成标签 (增强: 从所有分类中提取匹配关键词 + 提取名词短语)"""
    tags = []
    text = (post["title"] + " " + post.get("selftext", "")).lower()

    # 从子版块名生成标签
    sub = post.get("subreddit", "")
    if sub:
        tags.append(f"r/{sub}")

    # 从所有分类的关键词中匹配 (不只是当前分类)
    all_matched = []
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in text and len(kw) > 3 and kw not in all_matched:
                all_matched.append(kw)

    # 优先选当前分类的, 再选其他的
    primary = [kw for kw in all_matched if kw in CATEGORY_KEYWORDS.get(category, [])]
    secondary = [kw for kw in all_matched if kw not in primary]
    tags.extend(primary[:3])
    tags.extend(secondary[:2])

    # 补充: 从标题中提取简短关键词 (2-3个词的短语)
    title_words = post["title"].split()
    if len(tags) < 5 and len(title_words) >= 2:
        # 取标题前几个有意义的词
        stop = {"the", "a", "an", "is", "are", "was", "to", "in", "of", "for", "and", "or", "my", "i", "me", "you", "it", "do", "how", "what", "why"}
        meaningful = [w for w in title_words if w.lower() not in stop and len(w) > 2][:2]
        if meaningful:
            tags.append(" ".join(meaningful))

    return tags[:6]


# ============================================================
# 数据管理
# ============================================================
def load_existing_data():
    """加载现有数据"""
    if DATA_FILE.exists():
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"insights": [], "personas": [], "forecasts": [], "last_updated": ""}


def save_data(data):
    """保存数据"""
    data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\n已保存 {len(data['insights'])} 条洞察到 {DATA_FILE}")


def archive_data(data):
    """归档每日数据"""
    ARCHIVE_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    archive_file = ARCHIVE_DIR / f"data_{date_str}.json"
    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"已归档到 {archive_file}")


def deduplicate(insights):
    """去重：根据source_url和标题去重（手动数据始终保留）"""
    seen_urls = set()
    seen_titles = set()
    unique = []

    for item in insights:
        is_manual = not str(item.get("id", "")).startswith("auto_")
        url = item.get("source_url", "")
        title = item.get("title_en", "").lower().strip()

        # 手动数据始终保留
        if is_manual:
            unique.append(item)
            seen_urls.add(url)
            seen_titles.add(title)
            continue

        # 自动数据按URL去重（只对具体帖子URL去重，不对子版块主页去重）
        if url and "/comments/" in url and url in seen_urls:
            continue
        if title and title in seen_titles:
            continue

        if url and "/comments/" in url:
            seen_urls.add(url)
        seen_titles.add(title)
        unique.append(item)

    return unique


# ============================================================
# 主采集流程
# ============================================================
def run_collection():
    """执行一次完整的数据采集"""
    print("=" * 60)
    print(f"北美女性消费者洞察 - 数据采集")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    all_posts = []

    # 1. 采集各子版块热门帖子
    print("\n[第1步] 采集子版块热门帖子...")
    for sub_config in SUBREDDITS:
        sub_name = sub_config["name"]
        posts = fetch_subreddit_posts(sub_name, sort="hot", limit=10)
        for p in posts:
            p["_hints"] = sub_config["category_hints"]
        all_posts.extend(posts)
        print(f"    r/{sub_name}: 获取 {len(posts)} 篇")

    # 2. 搜索特定关键词
    print("\n[第2步] 搜索关键词...")
    search_subs = ["AskWomenOver30", "AskWomen", "workingmoms", "30PlusSkinCare"]
    for query in SEARCH_QUERIES[:8]:  # 限制搜索量避免限速
        sub = search_subs[SEARCH_QUERIES.index(query) % len(search_subs)]
        posts = fetch_subreddit_search(sub, query, limit=5)
        for p in posts:
            p["_hints"] = ["pay", "topic"]
        all_posts.extend(posts)
        print(f"    搜索 \"{query}\" in r/{sub}: {len(posts)} 结果")

    # 3. 分类和转换
    print(f"\n[第3步] 分类和处理 {len(all_posts)} 篇帖子...")
    new_insights = []
    for idx, post in enumerate(all_posts):
        hints = post.pop("_hints", None)
        category = classify_post(post, hints)
        insight = post_to_insight(post, category, idx)
        new_insights.append(insight)

    # 4. 合并现有数据
    print("\n[第4步] 合并数据...")
    existing_data = load_existing_data()

    # 加载seed数据（手动编写的洞察+画像+趋势）
    seed_file = DATA_DIR / "seed_data.json"
    seed_data = {}
    if seed_file.exists():
        with open(seed_file, "r", encoding="utf-8") as f:
            seed_data = json.load(f)

    # 保留手动编写的数据(id不以auto_开头的)
    manual_insights = [i for i in existing_data.get("insights", [])
                       if not str(i.get("id", "")).startswith("auto_")]

    # 合并自动采集的数据
    all_insights = manual_insights + new_insights
    all_insights = deduplicate(all_insights)

    # 按日期排序(最新在前)
    all_insights.sort(key=lambda x: x.get("date", ""), reverse=True)

    # 限制总数(保留最新的150条)
    all_insights = all_insights[:150]

    # 更新数据，保留seed中的画像和趋势
    existing_data["insights"] = all_insights
    if seed_data.get("personas"):
        existing_data["personas"] = seed_data["personas"]
    if seed_data.get("forecasts"):
        existing_data["forecasts"] = seed_data["forecasts"]
    existing_data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    # 5. 保存
    save_data(existing_data)
    archive_data(existing_data)

    # 统计
    cat_counts = {}
    for i in all_insights:
        cat = i.get("category", "unknown")
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    print("\n分类统计:")
    for cat, count in sorted(cat_counts.items()):
        print(f"  {cat}: {count}")
    print(f"\n总计: {len(all_insights)} 条洞察")
    print(f"其中手动编写: {len(manual_insights)} 条")
    print(f"自动采集: {len(all_insights) - len(manual_insights)} 条")
    print("\n采集完成!")


def schedule_daily():
    """每日定时采集"""
    import sched
    import threading

    print("启动每日定时采集模式...")
    print("每天 09:00 自动运行一次采集")
    print("按 Ctrl+C 停止\n")

    while True:
        now = datetime.now()
        # 计算下次运行时间 (明天9:00)
        next_run = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if now >= next_run:
            next_run += timedelta(days=1)

        wait_seconds = (next_run - now).total_seconds()
        print(f"下次采集时间: {next_run.strftime('%Y-%m-%d %H:%M')}")
        print(f"等待 {wait_seconds/3600:.1f} 小时...")

        # 先立即运行一次
        if "--now" in sys.argv:
            run_collection()
            sys.argv.remove("--now")

        time.sleep(wait_seconds)
        try:
            run_collection()
        except Exception as e:
            print(f"采集出错: {e}")


# ============================================================
# 入口
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="北美女性消费者洞察 - 自动数据采集")
    parser.add_argument("--schedule", action="store_true", help="启动每日定时采集")
    parser.add_argument("--now", action="store_true", help="配合--schedule使用，先立即运行一次")
    args = parser.parse_args()

    if args.schedule:
        if args.now:
            sys.argv.append("--now")
        schedule_daily()
    else:
        run_collection()
