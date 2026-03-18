"""
批量为自动采集的洞察生成中文标题和摘要
策略: 基于英文关键词识别话题, 然后生成自然的中文描述 (非逐词翻译)
"""
import json
import re
import hashlib
from pathlib import Path

DATA_FILE = Path(__file__).parent / "data.json"

# ============================================================
# 话题识别规则: 英文关键词 → 中文话题描述
# ============================================================
TOPIC_RULES = [
    # --- 人生/关系 ---
    (r'\b(friend|friendship)\b.*\b(30s|40s|over 30|lost|losing|no|few|little)\b', '30+女性友谊流失：为什么越长大朋友越少？'),
    (r'\b(friend|friendship)\b', '女性友谊与社交关系的变化'),
    (r'\bchildfree\b.*\b(end of life|old age|retire|plan)\b', '丁克女性的晚年规划：没有孩子的老后生活'),
    (r'\bchildfree\b', '丁克/不生孩子的女性生活选择'),
    (r'\b(partner|husband|spouse)\b.*\b(benefit|appreciate|love)\b', '长期伴侣关系中的相互支持与受益'),
    (r'\b(partner|husband|spouse)\b.*\b(toxic|leave|divorce|unhappy)\b', '不健康的伴侣关系：何时应该离开？'),
    (r'\b(healthy|normal)\b.*\brelationship\b', '健康的亲密关系应该是什么样的？'),
    (r'\brelationship\b.*\b(red flag|toxic|abuse)\b', '感情中的危险信号：识别有害关系'),
    (r'\b(dating|date)\b.*\b(30s|40s|over 30|after)\b', '30岁以后重新进入约会市场的体验'),
    (r'\b(dating|date|tinder|bumble|hinge)\b', '现代女性的约会体验与困惑'),
    (r'\bdivorce\b', '离婚后的生活重建与心路历程'),
    (r'\b(marry|marriage|wedding)\b', '关于婚姻的思考与讨论'),
    (r'\b(single|alone)\b.*\b(30s|40s|happy|content)\b', '单身也可以很幸福：30+女性的独处智慧'),
    (r'\b(lonely|loneliness|isolated)\b', '女性孤独感：被忽视的情感需求'),
    (r'\b(boundary|boundaries)\b', '学会设立边界：女性自我保护的必修课'),
    (r'\b(sex|intimacy|libido)\b', '女性性健康与亲密关系讨论'),
    (r'\bfamily\b.*\b(toxic|cut off|estranged|no contact)\b', '与原生家庭断联：女性的艰难抉择'),

    # --- 职场/事业 ---
    (r'\b(career|job)\b.*\b(change|switch|pivot|new)\b', '职业转型：女性的勇敢跨界之路'),
    (r'\b(career|job)\b.*\b(hate|quit|leave|burnout|toxic)\b', '职场困境：想辞职又不敢的纠结'),
    (r'\b(career|job)\b.*\b(advice|tip|help)\b', '职场女性的发展建议与经验分享'),
    (r'\b(salary|pay|raise|negotiate)\b', '薪资谈判：女性如何争取应得的报酬'),
    (r'\b(remote|wfh|work from home|hybrid)\b', '远程工作对女性生活的影响'),
    (r'\b(side hustle|freelance|business|entrepreneur)\b', '女性副业与创业：追求财务自由'),
    (r'\b(work.?life|work.*balance)\b', '工作与生活的平衡之道'),
    (r'\bmanager\b', '职场管理经验与挑战'),

    # --- 育儿/母亲 ---
    (r'\b(mental load|invisible labor|emotional labor)\b', '\"心理负荷\"：妈妈们看不见的第二份工'),
    (r'\b(childcare|daycare|nanny)\b.*\b(cost|expensive|afford)\b', '天价育儿费：中产家庭的核心焦虑'),
    (r'\b(childcare|daycare|nanny)\b', '育儿资源与选择的讨论'),
    (r'\b(mom guilt|guilt.*mom|guilty.*mother)\b', '妈妈的内疚感：永远觉得做得不够好'),
    (r'\b(breastfeed|nursing|pump|formula)\b', '母乳喂养的压力与选择自由'),
    (r'\b(sleep train|sleep.*baby|baby.*sleep)\b', '宝宝睡眠问题与妈妈的疲惫'),
    (r'\b(sahm|stay.at.home)\b', '全职妈妈的价值认同与社会困境'),
    (r'\b(screen time|ipad|tablet)\b.*\b(kid|child|toddler)\b', '孩子的屏幕时间：现代父母的两难'),
    (r'\b(mom|mother)\b.*\b(tired|exhaust|burnout|overwhelm)\b', '妈妈的疲惫：被掏空的日常'),
    (r'\b(postpartum|post.partum)\b', '产后恢复与心理健康挑战'),
    (r'\bpregnant|pregnancy\b', '怀孕期间的身心变化与应对'),

    # --- 健康/护肤 ---
    (r'\b(menopause|perimenopause|hot flash)\b', '更年期健康管理：女性不再沉默的话题'),
    (r'\b(anti.aging|wrinkle|retinol|botox|filler)\b', '抗衰老：女性护肤与医美的真实体验'),
    (r'\b(skincare|skin care|routine)\b.*\b(over 30|30s|40s)\b', '30+女性的护肤心得与产品推荐'),
    (r'\b(skincare|skin care|moisturizer|serum|sunscreen|SPF)\b', '护肤产品讨论与好物分享'),
    (r'\b(acne|breakout|pimple)\b', '成人痘的困扰与护理方案'),
    (r'\b(hair|hair loss|hair care|gray hair)\b', '女性头发护理与白发/脱发困扰'),
    (r'\b(weight|lose weight|weight loss|fat|obesity)\b', '体重管理：女性的身体焦虑与健康追求'),
    (r'\b(fitness|workout|exercise|gym|run|strength)\b', '女性健身运动的分享与讨论'),
    (r'\b(yoga|pilates|barre)\b', '瑜伽/普拉提：身心平衡的运动选择'),
    (r'\b(therapy|therapist|counseling|mental health)\b', '心理健康：寻求专业帮助的经验分享'),
    (r'\b(anxiety|anxious|panic)\b', '焦虑症：女性心理健康的隐形杀手'),
    (r'\b(depression|depressed)\b', '抑郁情绪：女性如何走出心理低谷'),
    (r'\b(sleep|insomnia|tired)\b', '睡眠问题：疲惫女性的求助与建议'),
    (r'\b(supplement|vitamin|probiotic|magnesium)\b', '保健品讨论：哪些营养补充剂真正有效？'),
    (r'\b(hormone|HRT|estrogen|progesterone)\b', '荷尔蒙疗法与女性健康选择'),
    (r'\b(pelvic floor|pcos|endometriosis|period|menstrual)\b', '女性专属健康问题的讨论与求助'),
    (r'\b(self.care|selfcare)\b', '自我关怀：女性犒赏自己的方式'),

    # --- 消费/产品 ---
    (r'\b(best|favorite|holy grail)\b.*\b(purchase|buy|product|item)\b', '最值得买的好物推荐'),
    (r'\b(recommend|recommendation)\b.*\b(product|brand|buy)\b', '产品推荐与种草清单'),
    (r'\b(subscription|box)\b', '订阅盒子/会员服务体验分享'),
    (r'\b(amazon|target|costco|sephora|ulta)\b', '购物渠道与消费体验分享'),
    (r'\b(worth it|worth the money|worth the price)\b', '值不值得买？真实消费评价'),
    (r'\b(splurge|treat yourself|indulge)\b', '偶尔奢侈一下：值得的大手笔消费'),
    (r'\b(budget|frugal|save money|cheap|affordable)\b', '省钱达人的消费智慧与好物'),
    (r'\b(dupe|alternative|knock.?off)\b', '大牌平替：性价比之选'),
    (r'\b(haul|unbox|try.on)\b', '购物开箱与试用分享'),
    (r'\b(fashion|outfit|style|clothes|dress|wardrobe)\b', '女性穿搭与时尚讨论'),
    (r'\b(capsule wardrobe|minimalist)\b', '极简衣橱：少即是多的穿衣哲学'),
    (r'\b(shoe|sneaker|boots)\b', '鞋子推荐与穿搭分享'),
    (r'\b(bag|purse|handbag|tote)\b', '包包讨论与推荐'),
    (r'\b(makeup|cosmetic|foundation|mascara|lipstick)\b', '彩妆产品讨论与推荐'),
    (r'\b(perfume|fragrance|scent)\b', '香水与香氛体验分享'),

    # --- 生活方式 ---
    (r'\b(book|reading|read)\b.*\b(recommend|favorite|best|club)\b', '好书推荐：女性的阅读清单'),
    (r'\b(hobby|hobbies)\b.*\b(new|start|30s|40s)\b', '30+岁新爱好：重新发现自己的乐趣'),
    (r'\b(hobby|hobbies)\b', '女性爱好与兴趣分享'),
    (r'\b(travel|vacation|trip)\b', '旅行目的地与度假体验分享'),
    (r'\b(cook|cooking|recipe|baking|meal)\b', '美食与烹饪：女性的厨房创意'),
    (r'\b(garden|gardening|plant|houseplant)\b', '园艺与植物：女性的绿色爱好'),
    (r'\b(home|house|apartment|decor|furniture)\b.*\b(buy|rent|organize|decorate)\b', '居家生活与家居装饰讨论'),
    (r'\b(pet|dog|cat|rescue)\b', '宠物生活：毛孩子带来的幸福'),
    (r'\b(podcast|audiobook)\b', '播客/有声书推荐：女性的精神食粮'),

    # --- 社会/年龄 ---
    (r'\b(aging|getting older|ageism|age)\b.*\b(30|40|50|society|fear)\b', '面对衰老：女性对年龄的思考与焦虑'),
    (r'\b(body image|body positive|insecure)\b', '身体形象：女性的自我接纳之路'),
    (r'\b(confidence|self.esteem|self.worth)\b', '自信心建设：女性的自我成长'),
    (r'\b(feminism|feminist|equality|patriarchy)\b', '女权与平等：当代女性的声音'),
    (r'\b(social media|instagram|tiktok)\b.*\b(toxic|quit|detox|compare)\b', '社交媒体的焦虑：比较陷阱与信息茧房'),
    (r'\b(social media|instagram|tiktok)\b', '社交媒体上的女性生活与趋势'),
    (r'\b(money|financial|finance|invest|save)\b', '女性理财与财务独立的讨论'),
    (r'\b(politics|political|election|policy)\b', '女性关注的社会政策与政治议题'),
    (r'\b(safety|harassment|assault|creep)\b', '女性安全：日常生活中的隐患与应对'),
    (r'\b(therapy|healing|trauma|ptsd)\b', '创伤疗愈：女性的心理重建之路'),

    # --- Pinterest/Quora常见内容 ---
    (r'\b(ponytail|hairstyle|braid|updo|hair look|blowout)\b', '发型灵感与造型推荐'),
    (r'\b(gift|gifts)\b.*\b(women|her|mom|wife|girlfriend)\b', '送给女性的礼物推荐清单'),
    (r'\b(gift|gifts)\b', '礼物推荐与送礼灵感'),
    (r'\b(beauty routine|beauty ritual|beauty tip|beauty secret)\b', '美容秘诀与日常护理'),
    (r'\b(night.*routine|morning.*routine|bedtime)\b', '日常作息与护理流程分享'),
    (r'\b(DIY|handmade|homemade|craft)\b', 'DIY手工与创意生活'),
    (r'\b(basket|care package|care kit)\b', '关怀礼包与生活好物组合'),
    (r'\b(organization|organize|declutter|tidy|clean)\b', '收纳整理与居家生活'),
    (r'\b(wellness|well.being|holistic)\b', '身心健康与全面养生'),
    (r'\b(trend|trending)\b.*\b(product|2026|2025)\b', '热门消费趋势与新品发现'),
    (r'\b(best|top|must.have)\b.*\b(product|brand|item|tool|gadget)\b', '必备好物与产品推荐'),
    (r'\b(broke|break|struggle|overcome|resilience|strong)\b.*\b(women|woman|life)\b', '女性力量：逆境中的成长故事'),
    (r'\b(empower|empowerment|inspire|inspiration|motivat)\b', '女性赋能与励志故事'),
    (r'\b(mompreneur|girl boss|boss babe|women.*business)\b', '女性创业者的成功之路'),
    (r'\b(clean eating|organic food|meal prep|healthy eating)\b', '健康饮食与营养搭配'),
    (r'\b(glow|dewy|radiant|luminous)\b.*\b(skin|face)\b', '光泽肌养成：打造自然好气色'),
    (r'\b(sell|selling|ecommerce|e.commerce|online.*store)\b', '电商与线上销售趋势'),
    (r'\bbeauty\b', '美容护理与好物推荐'),
    (r'\b(women|woman)\b.*\b(30s|40s|over 30|over 40)\b', '30+女性的生活智慧与选择'),
    (r'\b(working mom|working mother|working parent)\b', '职场妈妈的日常与挑战'),
]

# Subreddit上下文
SUB_CONTEXT = {
    "AskWomenOver30": "30+女性社区",
    "AskWomen": "女性问答社区",
    "TwoXChromosomes": "女性视角社区",
    "TheGirlSurvivalGuide": "女性生活指南",
    "workingmoms": "职场妈妈社区",
    "Mommit": "妈妈社区",
    "beyondthebump": "新手妈妈社区",
    "30PlusSkinCare": "30+护肤社区",
    "SkincareAddiction": "护肤爱好者社区",
    "xxfitness": "女性健身社区",
    "WellnessOver30": "30+养生社区",
    "FrugalFemaleFashion": "女性平价时尚",
    "femalefashionadvice": "女性穿搭建议",
    "SubscriptionBoxes": "订阅盒子社区",
}

# 来源平台上下文
SOURCE_CONTEXT = {
    "pinterest": "Pinterest",
    "quora": "专业问答平台",
    "threads": "Threads社区",
    "reddit": "Reddit社区",
    "news": "新闻媒体",
    "report": "行业报告",
}

CATEGORY_ZH = {
    "pain": "痛点", "topic": "热议", "hobby": "兴趣",
    "pay": "消费", "brand": "品牌", "emotional": "情绪消费", "premium": "优质体验",
}

def match_topic(title, selftext=""):
    """通过正则匹配英文内容, 返回中文话题描述"""
    combined = (title + " " + selftext).lower()
    for pattern, zh_desc in TOPIC_RULES:
        if re.search(pattern, combined, re.IGNORECASE):
            return zh_desc
    return None

def generate_zh_title(title_en, category, matched_topic):
    """生成中文标题"""
    if matched_topic:
        return matched_topic

    # Fallback: 根据分类 + 英文标题前几个词给出通用标题
    cat_zh = CATEGORY_ZH.get(category, "洞察")
    # 尝试提取标题核心信息
    clean = re.sub(r'^(What|How|Why|Do|Does|Is|Are|Has|Have|Can|Should|DAE|AITA|ELI5|TIL|PSA|LPT)\b\s*', '', title_en, flags=re.IGNORECASE)
    clean = clean.strip('?!. ')
    if len(clean) > 50:
        clean = clean[:50] + '...'
    return f"[{cat_zh}] {clean}"


def generate_zh_summary(title_en, summary_en, category, subreddit, matched_topic, source_type="reddit"):
    """生成中文摘要"""
    # 确定来源上下文
    if source_type in ("pinterest", "quora"):
        src_ctx = SOURCE_CONTEXT.get(source_type, source_type)
    else:
        src_ctx = SUB_CONTEXT.get(subreddit, "Reddit社区")

    # 从summary_en提取关键信息 (score/comments if in old format)
    score_match = re.search(r'(\d+)赞.*?(\d+)评论', summary_en or '')

    if matched_topic:
        if source_type == "pinterest":
            base = f"来自{src_ctx}的消费趋势。{matched_topic.rstrip('？?。')}——北美女性群体关注的热门内容。"
        elif source_type == "quora":
            base = f"来自{src_ctx}的专业讨论。{matched_topic.rstrip('？?。')}——一个引发深入探讨的话题。"
        else:
            base = f"来自{src_ctx}的真实讨论。{matched_topic.rstrip('？?。')}——这是北美女性群体中引发广泛共鸣的话题。"
    else:
        if source_type == "pinterest":
            base = f"来自{src_ctx}的消费灵感内容。"
        elif source_type == "quora":
            base = f"来自{src_ctx}的深度讨论。"
        else:
            base = f"来自{src_ctx}的讨论帖子。"

    # 添加互动数据
    if score_match:
        base += f" (获得{score_match.group(1)}赞、{score_match.group(2)}条评论)"

    # 添加分类语境
    cat_context = {
        "pain": "许多女性在评论区分享了相似的经历和困境。",
        "topic": "话题引发了大量讨论和不同角度的见解。",
        "hobby": "用户们热情分享了各自的体验和建议。",
        "pay": "评论区充满了真实的使用体验和消费建议。",
        "brand": "社区成员分享了对新品牌和新产品的看法。",
        "emotional": "用户们讨论了消费带来的情绪价值和幸福感。",
        "premium": "追求品质生活的女性分享了高端体验心得。",
    }
    base += " " + cat_context.get(category, "")

    return base


def main():
    print("加载数据...")
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    translated = 0
    for item in data["insights"]:
        item_id = str(item.get("id", ""))
        if not item_id.startswith("auto_"):
            continue

        title_en = item.get("title_en", "")
        summary_en = item.get("summary_en", "")
        category = item.get("category", "topic")

        # 检查当前title_zh是否已经是纯中文
        old_zh = item.get("title_zh", "")
        # 如果包含大量英文字母, 需要重新翻译
        alpha_ratio = sum(1 for c in old_zh if c.isascii() and c.isalpha()) / max(len(old_zh), 1)
        if alpha_ratio < 0.3:
            continue  # 已经是不错的中文

        # 提取来源信息
        source_type = item.get("source_type", "reddit")
        url = item.get("source_url", "")
        sub_match = re.search(r'reddit\.com/r/(\w+)', url)
        subreddit = sub_match.group(1) if sub_match else ""

        # 匹配话题
        selftext = summary_en if len(summary_en) > len(title_en) else ""
        matched = match_topic(title_en, selftext)

        # 生成中文标题
        item["title_zh"] = generate_zh_title(title_en, category, matched)

        # 生成中文摘要
        item["summary_zh"] = generate_zh_summary(title_en, summary_en, category, subreddit, matched, source_type)

        translated += 1

    # 保存
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"完成! 翻译了 {translated} 条洞察\n")

    # 统计翻译质量
    matched_count = 0
    fallback_count = 0
    for item in data["insights"]:
        if not str(item.get("id","")).startswith("auto_"):
            continue
        if item["title_zh"].startswith("["):
            fallback_count += 1
        else:
            matched_count += 1

    print(f"精准匹配: {matched_count} 条 (有自然中文标题)")
    print(f"通用模板: {fallback_count} 条 (分类前缀+英文)")

    # 示例
    import sys, io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    print("\n=== 翻译示例 ===")
    auto = [i for i in data["insights"] if str(i.get("id","")).startswith("auto_")]
    for item in auto[:8]:
        print(f"  EN: {item['title_en'][:70]}")
        print(f"  ZH: {item['title_zh'][:70]}")
        print(f"  摘要: {item['summary_zh'][:90]}")
        print()


if __name__ == "__main__":
    main()
