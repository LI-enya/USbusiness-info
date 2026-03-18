"""
北美女性消费者洞察 - 自动数据采集脚本 (多平台版)
Auto-collector for NA Women Consumer Insights Dashboard

支持平台: Reddit / Pinterest / Quora / Threads

使用方式:
  python collector.py              # 运行一次采集 (所有平台)
  python collector.py --reddit     # 仅采集Reddit
  python collector.py --pinterest  # 仅采集Pinterest
  python collector.py --quora      # 仅采集Quora
  python collector.py --threads    # 仅采集Threads
  python collector.py --schedule   # 每日定时采集
"""

import json
import os
import time
import re
import sys
import argparse
import hashlib
import html as html_module
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
import xml.etree.ElementTree as ET

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

# Pinterest 来源RSS配置 - 用 /source/ 格式获取优质网站的Pin
PINTEREST_SOURCES = [
    # 格式: source域名 → Pinterest自动聚合该网站的所有Pin
    {"source": "self.com", "hints": ["pay", "premium"]},        # Self Magazine
    {"source": "byrdie.com", "hints": ["pay", "brand"]},        # 美容护肤
    {"source": "theeverygirl.com", "hints": ["topic", "hobby"]}, # 女性生活方式
    {"source": "purewow.com", "hints": ["emotional", "pay"]},   # 女性消费
    {"source": "wellandgood.com", "hints": ["premium", "hobby"]}, # 健康wellness
    {"source": "motherly.com", "hints": ["pain", "pay"]},       # 妈妈群体
]

# Pinterest 搜索关键词
PINTEREST_QUERIES = [
    "women self care products 2026",
    "best skincare routine over 30",
    "working mom essentials",
    "home organization must haves",
    "wellness gifts for women",
    "clean beauty brands",
    "postpartum recovery products",
    "women fitness gear",
]

# Quora 搜索话题 - 与目标人群匹配
QUORA_QUERIES = [
    "What products are worth splurging on for women over 30",
    "What do working mothers wish existed",
    "What skincare products actually work for women over 35",
    "What subscription boxes are worth it for women",
    "What self care routines do successful women follow",
    "What are the biggest pain points for working moms in America",
    "What new brands are disrupting the beauty industry",
    "What premium experiences are worth paying for",
    "What wellness trends are women investing in 2026",
    "What hobby do women pick up in their 30s and 40s",
]

# Threads 用户/话题配置 - 高互动女性话题账号
THREADS_TARGETS = [
    # 话题标签 (高互动讨论帖)
    {"type": "tag", "value": "workingmom", "hints": ["pain", "topic"]},
    {"type": "tag", "value": "selfcare", "hints": ["emotional", "premium"]},
    {"type": "tag", "value": "skincare", "hints": ["pay", "brand"]},
    {"type": "tag", "value": "momlife", "hints": ["pain", "topic"]},
    {"type": "tag", "value": "womenempowerment", "hints": ["topic", "hobby"]},
    {"type": "tag", "value": "cleanbeauty", "hints": ["brand", "pay"]},
    {"type": "tag", "value": "wellnessjourney", "hints": ["premium", "hobby"]},
    {"type": "tag", "value": "girlmath", "hints": ["emotional", "pay"]},
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
# Pinterest 数据采集 (RSS feeds + 搜索页解析)
# ============================================================
def fetch_url(url, retries=2):
    """通用HTTP GET，返回文本内容"""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    req = Request(url, headers=headers)
    for attempt in range(retries):
        try:
            with urlopen(req, timeout=15) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(3)
            else:
                print(f"  [错误] {url}: {e}")
                return None
    return None


def fetch_pinterest_via_bing(query, domain_hint=""):
    """通过Bing搜索 site:pinterest.com 获取公开Pin页面"""
    search_q = f"site:pinterest.com {query}"
    if domain_hint:
        search_q += f" {domain_hint}"
    bing_url = f"https://www.bing.com/search?q={quote(search_q)}&count=10&setlang=en&mkt=en-US"
    print(f"  搜索 Pinterest (via Bing): \"{query[:50]}\"...")

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)",
        "Accept": "text/html",
    }
    req = Request(bing_url, headers=headers)
    try:
        with urlopen(req, timeout=15) as resp:
            content = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"    [错误] {e}")
        return []

    pins = []
    parts = content.split('class="b_algo"')

    for part in parts[1:8]:
        block = part[:3000]

        # Extract URL - prefer pinterest.com/pin/ URLs
        href_match = re.search(r'href="[^"]*pinterest\.com/pin/(\d+)', block)
        if href_match:
            pin_url = f"https://www.pinterest.com/pin/{href_match.group(1)}/"
        else:
            # Accept any pinterest URL from cite
            cite_match = re.search(r'<cite[^>]*>(.*?)</cite>', block, re.DOTALL)
            if cite_match:
                cite_text = re.sub(r'<[^>]+>', '', cite_match.group(1)).strip()
                if 'pinterest.com' in cite_text:
                    pin_url = "https://" + cite_text.split()[0]
                else:
                    continue
            else:
                continue

        # Extract title
        h2_match = re.search(r'<h2[^>]*>.*?<a[^>]*>(.*?)</a>', block, re.DOTALL)
        title = ""
        if h2_match:
            title = html_module.unescape(re.sub(r'<[^>]+>', '', h2_match.group(1))).strip()
            title = title.replace(" | Pinterest", "").replace(" - Pinterest", "").strip()

        # Extract snippet
        desc = ""
        p_match = re.search(r'<p[^>]*>(.*?)</p>', block, re.DOTALL)
        if p_match:
            desc = html_module.unescape(re.sub(r'<[^>]+>', '', p_match.group(1))).strip()
            desc = re.sub(r'^\w{3}\s+\d+,\s+\d{4}\s*[-·]\s*', '', desc)  # Remove date prefix

        if title and len(title) > 5:
            pins.append({
                "title": title[:200],
                "selftext": desc[:500],
                "url": pin_url,
                "score": 0,
                "num_comments": 0,
                "subreddit": "pinterest/search",
                "created_utc": time.time(),
            })

    time.sleep(1.5)
    return pins


def fetch_pinterest_search(query):
    """通过Bing搜索获取Pinterest相关Pin"""
    return fetch_pinterest_via_bing(query)


def pinterest_post_to_insight(post, category):
    """将Pinterest数据转换为洞察格式"""
    url_hash = hashlib.md5(post["url"].encode()).hexdigest()[:8]
    prefix_map = {
        "pain": "痛点发现", "topic": "消费趋势", "hobby": "兴趣发现",
        "pay": "消费推荐", "brand": "品牌发现", "emotional": "情绪消费", "premium": "优质体验",
    }
    prefix = prefix_map.get(category, "消费洞察")

    return {
        "id": f"auto_pin_{url_hash}",
        "category": category,
        "title_zh": f"[{prefix}] {post['title'][:80]}",
        "title_en": post["title"][:120],
        "summary_zh": f"来自Pinterest。{post.get('selftext', '')[:200]}" if post.get('selftext') else f"来自Pinterest的消费趋势发现。标题：\"{post['title'][:100]}\"",
        "summary_en": post.get("selftext", "")[:300] or post["title"],
        "quote_en": "",
        "source_type": "pinterest",
        "source_url": post["url"],
        "tags": generate_tags(post, category),
        "date": datetime.now().strftime("%Y-%m-%d"),
    }


# ============================================================
# Quora 数据采集 (公开页面解析)
# ============================================================
def fetch_quora_question(query):
    """通过Bing搜索获取女性消费洞察 (Q&A + 编辑内容)

    Quora直接访问返回403，从Bing搜索结果中提取
    高质量女性消费相关的Q&A和编辑内容。
    """
    bing_url = f"https://www.bing.com/search?q={quote(query + ' women recommendation')}&count=10&setlang=en&mkt=en-US"
    print(f"  搜索 Q&A: \"{query[:50]}\"...")
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)",
        "Accept": "text/html",
    }
    req = Request(bing_url, headers=headers)
    try:
        with urlopen(req, timeout=15) as resp:
            content = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"    [错误] {e}")
        return []

    posts = []
    skip_domains = ["amazon.com", "ebay.com", "walmart.com", "target.com", "reddit.com",
                    "bing.com", "microsoft.com", "google.com", "youtube.com",
                    "zhihu.com", "baidu.com", "bilibili.com", "csdn.net", "jianshu.com",
                    "douban.com", "sogou.com", "163.com", "qq.com", "toutiao.com",
                    "hatena.ne.jp", "yahoo.co.jp", "naver.com", "wikipedia.org",
                    # 不相关的技术/医疗/娱乐站点
                    "stackoverflow.com", "stackexchange.com", "github.com",
                    "mayoclinic.org", "webmd.com", "nih.gov", "medlineplus.gov",
                    "xbox.com", "support.xbox.com", "spotify.com", "netflix.com",
                    "weforum.org", "imf.org", "worldbank.org",
                    "apple.com", "developer.apple.com", "docs.python.org"]

    # Split by b_algo class to get individual results
    parts = content.split('class="b_algo"')

    for part in parts[1:11]:  # Skip first split (before first result), take up to 10
        block = part[:3000]

        # Extract URL from h2 > a href
        href_match = re.search(r'<h2[^>]*>.*?<a[^>]*href="(https?://[^"]+)"', block, re.DOTALL)
        if not href_match:
            # Try any first link
            href_match = re.search(r'href="(https?://(?!r\.bing|www\.bing)[^"]+)"', block)
        if not href_match:
            continue

        actual_url = html_module.unescape(href_match.group(1))

        # Resolve Bing redirect URLs - extract real URL from cite tag
        if "bing.com/ck/" in actual_url:
            cite_match = re.search(r'<cite[^>]*>(.*?)</cite>', block, re.DOTALL)
            if cite_match:
                cite_text = re.sub(r'<[^>]+>', '', cite_match.group(1)).strip()
                # cite usually shows the real domain/path
                if cite_text and '.' in cite_text:
                    actual_url = "https://" + cite_text.split()[0]
                else:
                    continue
            else:
                continue

        # Skip unwanted domains
        if any(d in actual_url.lower() for d in skip_domains):
            continue

        # Extract title from h2 > a
        h2_match = re.search(r'<h2[^>]*>.*?<a[^>]*>(.*?)</a>', block, re.DOTALL)
        title_clean = ""
        if h2_match:
            title_clean = html_module.unescape(re.sub(r'<[^>]+>', '', h2_match.group(1))).strip()

        # Extract snippet from <p>
        snippet_clean = ""
        p_match = re.search(r'<p[^>]*>(.*?)</p>', block, re.DOTALL)
        if p_match:
            snippet_clean = html_module.unescape(re.sub(r'<[^>]+>', '', p_match.group(1))).strip()
            # Remove date prefix like "2025年7月22日 · "
            snippet_clean = re.sub(r'^\d{4}年\d+月\d+日\s*·?\s*', '', snippet_clean)

        # Remove common suffixes
        for suffix in [" - Quora", " | Quora", " - BuzzFeed", " - HuffPost", " | Refinery29",
                       " - Vogue", " - Allure", " - Self", " | Byrdie"]:
            title_clean = title_clean.replace(suffix, "")

        if not title_clean or len(title_clean) < 10:
            continue

        # Skip non-English content (CJK characters in title = wrong language results)
        cjk_ratio = sum(1 for c in title_clean if '\u4e00' <= c <= '\u9fff' or '\u3040' <= c <= '\u30ff' or '\uac00' <= c <= '\ud7af') / max(len(title_clean), 1)
        if cjk_ratio > 0.15:
            continue

        posts.append({
            "title": title_clean[:200],
            "selftext": snippet_clean[:1000] if snippet_clean else title_clean,
            "url": actual_url,
            "score": 10,
            "num_comments": 0,
            "subreddit": "quora/search",
            "created_utc": time.time(),
        })

    time.sleep(1.5)
    return posts


def quora_post_to_insight(post, category):
    """将Quora数据转换为洞察格式"""
    url_hash = hashlib.md5(post["url"].encode()).hexdigest()[:8]
    prefix_map = {
        "pain": "痛点分析", "topic": "专业讨论", "hobby": "兴趣推荐",
        "pay": "消费建议", "brand": "品牌评价", "emotional": "情绪洞察", "premium": "优质推荐",
    }
    prefix = prefix_map.get(category, "专业观点")
    answer_preview = post.get("selftext", "")[:200]

    return {
        "id": f"auto_qra_{url_hash}",
        "category": category,
        "title_zh": f"[{prefix}] {post['title'][:80]}",
        "title_en": post["title"][:120],
        "summary_zh": f"来自Quora专业问答。{answer_preview}" if answer_preview else f"来自Quora的专业讨论。问题：\"{post['title'][:100]}\"",
        "summary_en": post.get("selftext", "")[:300] or post["title"],
        "quote_en": extract_key_quote(post.get("selftext", "")),
        "source_type": "quora",
        "source_url": post["url"],
        "tags": generate_tags(post, category),
        "date": datetime.now().strftime("%Y-%m-%d"),
    }


# ============================================================
# Threads 数据采集 (公开页面 + 嵌入JSON)
# ============================================================
def fetch_threads_tag(tag):
    """获取Threads话题标签下的热门帖子"""
    url = f"https://www.threads.net/search?q={quote(tag)}&serp_type=default"
    print(f"  采集 Threads 话题: #{tag}...")
    # Threads 对完整的Chrome UA返回JS-only shell (无数据)
    # 使用简化UA获取SSR完整页面
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html",
    }
    req = Request(url, headers=headers)
    try:
        with urlopen(req, timeout=20) as resp:
            content = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"    [错误] {e}")
        return []
    if not content:
        return []

    posts = []

    # 直接正则匹配帖子文本 + 互动数据 (Threads是SPA，所有数据嵌入在JSON中)
    text_matches = re.findall(r'"text":"([^"]{30,800})"', content)
    like_matches = re.findall(r'"like_count":(\d+)', content)

    for i, text in enumerate(text_matches[:15]):
        # 反转义Unicode (处理 \\uXXXX 序列)
        if r'\u' in text:
            try:
                text = text.encode('utf-8', errors='replace').decode('unicode_escape')
            except (UnicodeDecodeError, UnicodeError):
                pass
        # 清除无效的surrogate字符
        text = text.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
        text = html_module.unescape(text)

        # 过滤太短的
        if len(text) < 25:
            continue
        # 粗略判断是否英文 (放宽阈值，因为emoji也算非ASCII)
        ascii_ratio = sum(1 for c in text if ord(c) < 128) / max(len(text), 1)
        if ascii_ratio < 0.4:
            continue

        likes = int(like_matches[i]) if i < len(like_matches) else 0
        posts.append({
            "title": text[:120],
            "selftext": text,
            "url": f"https://www.threads.net/search?q={quote(tag)}",
            "score": likes,
            "num_comments": 0,
            "subreddit": f"threads/#{tag}",
            "created_utc": time.time(),
        })

    time.sleep(2)
    return posts[:10]


def _extract_threads_posts(data, tag, depth=0):
    """递归搜索JSON数据中的帖子内容"""
    posts = []
    if depth > 8:
        return posts

    if isinstance(data, dict):
        # 检查是否有帖子文本字段
        text = data.get("text", "") or data.get("caption", "") or data.get("body", "")
        if isinstance(text, str) and len(text) > 30:
            likes = data.get("like_count", 0) or data.get("likes", {}).get("count", 0)
            posts.append({
                "title": text[:120],
                "selftext": text[:800],
                "url": data.get("url", f"https://www.threads.net/search?q={quote(tag)}"),
                "score": likes if isinstance(likes, int) else 0,
                "num_comments": data.get("reply_count", 0) or data.get("comments", {}).get("count", 0),
                "subreddit": f"threads/#{tag}",
                "created_utc": data.get("taken_at", time.time()),
            })

        for key, val in data.items():
            if isinstance(val, (dict, list)):
                posts.extend(_extract_threads_posts(val, tag, depth + 1))
    elif isinstance(data, list):
        for item in data[:30]:
            if isinstance(item, (dict, list)):
                posts.extend(_extract_threads_posts(item, tag, depth + 1))

    return posts


def threads_post_to_insight(post, category):
    """将Threads数据转换为洞察格式"""
    hash_input = (post.get("url", "") + post.get("title", "")).encode("utf-8", errors="replace")
    url_hash = hashlib.md5(hash_input).hexdigest()[:8]
    prefix_map = {
        "pain": "社区心声", "topic": "热门讨论", "hobby": "兴趣分享",
        "pay": "消费讨论", "brand": "品牌热议", "emotional": "情绪分享", "premium": "品质生活",
    }
    prefix = prefix_map.get(category, "社区讨论")

    return {
        "id": f"auto_thr_{url_hash}",
        "category": category,
        "title_zh": f"[{prefix}] {post['title'][:80]}",
        "title_en": post["title"][:120],
        "summary_zh": f"来自Threads社区。{post.get('selftext', '')[:200]}",
        "summary_en": post.get("selftext", "")[:300] or post["title"],
        "quote_en": post.get("selftext", "")[:200],
        "source_type": "threads",
        "source_url": post["url"],
        "tags": generate_tags(post, category),
        "date": datetime.now().strftime("%Y-%m-%d"),
    }


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
def run_collection(platforms=None):
    """执行一次完整的数据采集"""
    if platforms is None:
        platforms = ["reddit", "pinterest", "quora", "threads"]

    print("=" * 60)
    print(f"北美女性消费者洞察 - 多平台数据采集")
    print(f"平台: {', '.join(platforms)}")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    new_insights = []

    # ========== Reddit ==========
    if "reddit" in platforms:
        print("\n" + "=" * 40)
        print("[Reddit] 采集开始...")
        print("=" * 40)
        reddit_posts = []

        # 1. 采集各子版块热门帖子
        print("\n  [1/2] 子版块热门帖子...")
        for sub_config in SUBREDDITS:
            sub_name = sub_config["name"]
            posts = fetch_subreddit_posts(sub_name, sort="hot", limit=10)
            for p in posts:
                p["_hints"] = sub_config["category_hints"]
            reddit_posts.extend(posts)
            print(f"    r/{sub_name}: 获取 {len(posts)} 篇")

        # 2. 搜索特定关键词
        print("\n  [2/2] 搜索关键词...")
        search_subs = ["AskWomenOver30", "AskWomen", "workingmoms", "30PlusSkinCare"]
        for query in SEARCH_QUERIES[:8]:
            sub = search_subs[SEARCH_QUERIES.index(query) % len(search_subs)]
            posts = fetch_subreddit_search(sub, query, limit=5)
            for p in posts:
                p["_hints"] = ["pay", "topic"]
            reddit_posts.extend(posts)
            print(f"    搜索 \"{query}\" in r/{sub}: {len(posts)} 结果")

        # 分类和转换
        print(f"\n  分类处理 {len(reddit_posts)} 篇Reddit帖子...")
        for idx, post in enumerate(reddit_posts):
            hints = post.pop("_hints", None)
            category = classify_post(post, hints)
            insight = post_to_insight(post, category, idx)
            new_insights.append(insight)
        print(f"  [OK]Reddit: {len(reddit_posts)} 条洞察")

    # ========== Pinterest ==========
    if "pinterest" in platforms:
        print("\n" + "=" * 40)
        print("[Pinterest] 采集开始...")
        print("=" * 40)
        pin_posts = []

        # 1. 基于来源域名搜索 (替代已失效的RSS)
        print("\n  [1/2] 来源域名搜索...")
        for src in PINTEREST_SOURCES[:4]:  # 限制数量避免限速
            pins = fetch_pinterest_via_bing("women", src["source"])
            for p in pins:
                p["_hints"] = src["hints"]
            pin_posts.extend(pins)
            print(f"    {src['source']}: {len(pins)} pins")

        # 2. 关键词搜索
        print("\n  [2/2] 搜索关键词...")
        for query in PINTEREST_QUERIES[:6]:
            pins = fetch_pinterest_search(query)
            for p in pins:
                p["_hints"] = ["pay", "brand"]
            pin_posts.extend(pins)
            print(f"    \"{query}\": {len(pins)} pins")

        # 分类和转换
        print(f"\n  分类处理 {len(pin_posts)} 个Pinterest Pins...")
        for post in pin_posts:
            hints = post.pop("_hints", None)
            category = classify_post(post, hints)
            insight = pinterest_post_to_insight(post, category)
            new_insights.append(insight)
        print(f"  [OK]Pinterest: {len(pin_posts)} 条洞察")

    # ========== Quora ==========
    if "quora" in platforms:
        print("\n" + "=" * 40)
        print("[Quora] 采集开始 (via Bing搜索)...")
        print("=" * 40)
        quora_posts = []

        for query in QUORA_QUERIES[:8]:
            posts = fetch_quora_question(query)
            for p in posts:
                p["_hints"] = ["pay", "topic"]
            quora_posts.extend(posts)
            print(f"    \"{query[:40]}\": {len(posts)} 结果")

        # 分类和转换
        print(f"\n  分类处理 {len(quora_posts)} 条Quora内容...")
        for post in quora_posts:
            hints = post.pop("_hints", None)
            category = classify_post(post, hints)
            insight = quora_post_to_insight(post, category)
            new_insights.append(insight)
        print(f"  [OK]Quora: {len(quora_posts)} 条洞察")

    # ========== Threads ==========
    if "threads" in platforms:
        print("\n" + "=" * 40)
        print("[Threads] 采集开始...")
        print("=" * 40)
        threads_posts = []

        for target in THREADS_TARGETS[:6]:
            if target["type"] == "tag":
                posts = fetch_threads_tag(target["value"])
                for p in posts:
                    p["_hints"] = target["hints"]
                threads_posts.extend(posts)
                print(f"    #{target['value']}: {len(posts)} 帖子")

        # 分类和转换
        print(f"\n  分类处理 {len(threads_posts)} 个Threads帖子...")
        for post in threads_posts:
            hints = post.pop("_hints", None)
            category = classify_post(post, hints)
            insight = threads_post_to_insight(post, category)
            new_insights.append(insight)
        print(f"  [OK]Threads: {len(threads_posts)} 条洞察")

    # ========== 合并 ==========
    print("\n" + "=" * 40)
    print(f"[合并] 新采集 {len(new_insights)} 条洞察...")
    print("=" * 40)
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

    # 智能合并: 仅替换本次采集平台的auto数据，保留其他平台的
    # 平台ID前缀映射
    platform_prefixes = {
        "reddit": "auto_",       # auto_ (不含 auto_pin_, auto_qra_, auto_thr_)
        "pinterest": "auto_pin_",
        "quora": "auto_qra_",
        "threads": "auto_thr_",
    }
    # 计算本次采集的平台前缀集合
    active_prefixes = set()
    for p in (platforms or ["reddit", "pinterest", "quora", "threads"]):
        active_prefixes.add(platform_prefixes.get(p, ""))

    # 保留未采集平台的旧auto数据
    preserved_auto = []
    for i in existing_data.get("insights", []):
        iid = str(i.get("id", ""))
        if not iid.startswith("auto_"):
            continue  # manual, handled above
        # 判断这条数据属于哪个平台
        item_platform = "reddit"  # default
        if iid.startswith("auto_pin_"):
            item_platform = "pinterest"
        elif iid.startswith("auto_qra_"):
            item_platform = "quora"
        elif iid.startswith("auto_thr_"):
            item_platform = "threads"
        # 只保留不在本次采集范围内的平台数据
        if item_platform not in (platforms or ["reddit", "pinterest", "quora", "threads"]):
            preserved_auto.append(i)

    # 合并: 手动 + 保留的其他平台auto + 本次新采集
    all_insights = manual_insights + preserved_auto + new_insights
    all_insights = deduplicate(all_insights)

    # 按日期排序(最新在前)
    all_insights.sort(key=lambda x: x.get("date", ""), reverse=True)

    # 限制总数(保留最新的150条)
    all_insights = all_insights[:300]  # 提高上限，支持多平台

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
    parser = argparse.ArgumentParser(description="北美女性消费者洞察 - 多平台自动数据采集")
    parser.add_argument("--schedule", action="store_true", help="启动每日定时采集")
    parser.add_argument("--now", action="store_true", help="配合--schedule使用，先立即运行一次")
    parser.add_argument("--reddit", action="store_true", help="仅采集Reddit")
    parser.add_argument("--pinterest", action="store_true", help="仅采集Pinterest")
    parser.add_argument("--quora", action="store_true", help="仅采集Quora")
    parser.add_argument("--threads", action="store_true", help="仅采集Threads")
    args = parser.parse_args()

    # 确定要采集的平台
    platforms = []
    if args.reddit: platforms.append("reddit")
    if args.pinterest: platforms.append("pinterest")
    if args.quora: platforms.append("quora")
    if args.threads: platforms.append("threads")
    if not platforms:
        platforms = None  # 全部采集

    if args.schedule:
        if args.now:
            sys.argv.append("--now")
        schedule_daily()
    else:
        run_collection(platforms)
