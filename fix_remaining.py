"""
修复剩余43条未翻译的洞察 - 直接用标题关键词映射生成中文
"""
import json
import re
from pathlib import Path

DATA_FILE = Path(__file__).parent / "data.json"

# 针对剩余fallback的精准翻译映射 (标题子串 → 中文标题, 中文摘要关键句)
DIRECT_MAP = {
    "broke up with a long term partner who was a good person": (
        "和\"好人但不对的人\"分手：女性的艰难情感决定",
        "当伴侣是好人但不适合你，分手的决定有多难？女性分享了结束长期关系的心路历程。"
    ),
    "Single women, how are you preparing for being single later in life": (
        "单身女性如何规划独自变老的生活？",
        "越来越多单身女性开始认真规划没有伴侣的晚年生活，从财务到社交到居住安排。"
    ),
    "strict South Asian family": (
        "亚裔家庭的束缚：年轻女性在自由与家庭间的挣扎",
        "来自严格亚裔家庭的年轻女性，是选择按自己的方式生活还是服从家庭期望？"
    ),
    "Setting Goals and Sticking with Them": (
        "如何设定目标并坚持下去？女性的自律经验",
        "社区女性分享了设定人生目标、保持动力并坚持执行的实用方法和经验。"
    ),
    "end a relationship at very end of your 30s": (
        "快40岁时结束一段感情：重新开始还来得及吗？",
        "在30岁末期结束一段长期关系，面对年龄焦虑和重新开始的恐惧。"
    ),
    "linguistic habit or 'softening' phrase": (
        "女性说话中的\"软化\"习惯：我们为什么总在道歉？",
        "女性在表达时经常使用'我只是觉得'、'抱歉打扰了'等软化语言，这背后反映了什么？"
    ),
    "girls girl": (
        "做一个\"女生的女生\"有多重要？女性友谊的讨论",
        "什么是'girls girl'？社区女性讨论支持其他女性的意义和重要性。"
    ),
    "comfort activity when life gets stressful": (
        "生活压力大时，你的解压方式是什么？",
        "女性分享了各种缓解压力的舒适活动，从阅读到烘焙到散步到泡澡。"
    ),
    "products keep you fresh all day": (
        "全天保持清爽的好物推荐",
        "女性分享了让自己一整天保持清爽、自信的个人护理产品推荐。"
    ),
    "teen years the most fun": (
        "青少年时期真的是人生最快乐的阶段吗？",
        "社区讨论青少年时期是否真的是最美好的时光，不同年龄段的女性分享了各自看法。"
    ),
    "Single American Women Are Buying Homes": (
        "美国单身女性购房创纪录：超2000万套房产",
        "美国单身女性购房人数创历史新高，反映了女性经济独立和资产意识的显著提升。"
    ),
    "pain of being chronically single": (
        "长期单身的痛苦：孤独感与社会压力",
        "长期单身女性分享了面对孤独、社会期望和节日焦虑的真实感受。"
    ),
    "Medical conscience": (
        "美国\"医疗良心\"法案：女性健康权面临新挑战",
        "新提出的法案可能允许医疗提供者拒绝某些服务，引发女性健康权的担忧。"
    ),
    "19 and have had health issues": (
        "19岁的健康困扰：年轻女性长期未确诊的医疗经历",
        "一位年轻女性分享了长期健康问题迟迟得不到诊断的沮丧经历。"
    ),
    "kicked in the balls": (
        "分娩之痛不可比拟：女性对疼痛认知偏见的反击",
        "社区讨论社会对分娩疼痛的低估和对女性忍痛能力的偏见。"
    ),
    "Cone boobs": (
        "非\"标准\"胸型的自我接纳：女性身体形象讨论",
        "女性讨论不符合主流审美的胸型带来的困扰，以及如何学会接纳自己的身体。"
    ),
    "House of Representatives candidate": (
        "政治人物的性别平等立场：女性选民的关注焦点",
        "社区关注政治候选人在性别平等和女性权益方面的立场和言行。"
    ),
    "Being a plain woman": (
        "做一个\"普通外貌\"的女性是什么体验？",
        "不漂亮也不丑，\"普通长相\"的女性分享了被社会忽视的感受和应对方式。"
    ),
    "apologized for calling me": (
        "被叫\"女士\"需要道歉吗？职场称呼中的年龄焦虑",
        "一位女性被称为'ma'am'后对方道歉，引发了关于年龄、称呼和社会态度的讨论。"
    ),
    "misogynistic father": (
        "如何应对厌女的父亲？年轻女性的家庭困境",
        "面对持有厌女观念的父亲，年轻女性讨论如何在家庭关系中保护自己。"
    ),
    "women's shelter": (
        "给女性庇护所的实用礼物：她们最需要什么？",
        "社区讨论什么是女性庇护所中最受欢迎和最需要的捐赠物品。"
    ),
    "older kids": (
        "孩子长大后生活\"更轻松\"了吗？只是换了一种辛苦",
        "孩子大了之后，妈妈们的生活并没有想象中轻松，只是挑战变了。"
    ),
    "career should end or slow down": (
        "谁有权决定我的职业节奏？女性职场自主权",
        "女性分享了被他人（家人、同事）干涉职业决策时的愤怒和无奈。"
    ),
    "Mom/life/work": (
        "妈妈/生活/工作：三者如何平衡？",
        "职场妈妈们分享了同时兼顾母亲角色、个人生活和工作的真实经验和策略。"
    ),
    "FMLA timing": (
        "美国家庭医疗假法案更新引发不满",
        "FMLA（家庭与医疗休假法案）的时间安排更新让许多职场妈妈感到愤怒。"
    ),
    "husband keeps turning serious parenting decisions into": (
        "丈夫把严肃的育儿决定变成\"惊喜\"：妈妈的崩溃",
        "一位妈妈分享丈夫将重要的教育决策变成玩笑的经历，引发共鸣。"
    ),
    "8 things to do with your parents": (
        "趁父母还在，和孩子一起做这8件事",
        "一位过来人分享了在父母还健在时应该创造的家庭记忆和活动。"
    ),
    "toddler screamed because I gave him the banana": (
        "给了他要的香蕉却被尖叫：幼儿的\"不讲理\"日常",
        "养育幼儿的崩溃瞬间——明明是他要求的东西，拿到后却大哭大闹。"
    ),
    "six year old is BEGGING me to let her read": (
        "6岁女儿想看大人的科幻小说：如何平衡好奇心与年龄适宜？",
        "孩子对成人书籍产生兴趣，家长该如何引导而不扼杀好奇心？"
    ),
    "Anyone else up at 2am": (
        "凌晨2点还醒着的妈妈们：深夜的疲惫与孤独",
        "新手妈妈们分享半夜醒来喂奶、哄睡的孤独时刻和互相鼓励。"
    ),
    "fixate on the horrors of the world": (
        "如何不被世界的糟糕新闻吞噬？女性的焦虑应对",
        "面对层出不穷的负面新闻，女性讨论如何保护心理健康、不被焦虑裹挟。"
    ),
    "letting a stranger touch my toddler": (
        "让陌生人摸了我的孩子：妈妈的自责与边界意识",
        "一位妈妈因为没有阻止陌生人触碰孩子而深感自责，引发关于个人边界的讨论。"
    ),
    "First time mom to a two-month-old": (
        "新手妈妈的焦虑：我是不是什么都做错了？",
        "两个月宝宝的新手妈妈寻求安慰，担心自己的每个决定都不对。"
    ),
    "baby to the ER and ended up being for nothing": (
        "带宝宝去急诊结果没事：虚惊一场的父母焦虑",
        "新手父母带宝宝去急诊，结果虎头蛇尾的经历——是否过度紧张了？"
    ),
    "Floor bed": (
        "地板床：蒙特梭利育儿法的实践讨论",
        "越来越多家长尝试地板床作为婴幼儿的睡眠方案，社区分享实际体验。"
    ),
    "Ingrowns": (
        "毛发内生的困扰与护理方法",
        "女性分享处理毛发内生问题的方法、产品推荐和预防技巧。"
    ),
    "AmLactin Crepe": (
        "AmLactin身体乳对抗皮肤松弛效果评测",
        "社区讨论AmLactin身体乳产品对改善皮肤褶皱和松弛的真实效果。"
    ),
    "Tret": (
        "维A酸使用经验：抗衰老护肤的核心成分",
        "社区讨论维A酸（Tretinoin）的使用方法、效果和注意事项。"
    ),
    "XERF was tested on PIGS": (
        "护肤品动物实验警告：素食主义者需注意",
        "社区分享护肤品牌的动物实验信息，提醒关注成分来源的消费者。"
    ),
    "Routine Help.*reduced face washing": (
        "减少洗脸次数反而皮肤变好了：反直觉的护肤经验",
        "用户分享减少洁面频率后皮肤状况改善的经历，引发护肤理念讨论。"
    ),
    "scar revision": (
        "疤痕修复3年后仍然发红：漫长的恢复之路",
        "用户分享疤痕修复手术后持续泛红的困扰，寻求改善建议。"
    ),
    "Women in tech.*tech bro culture": (
        "科技行业女性如何应对\"技术男\"文化？",
        "40+岁的科技行业女性讨论如何在男性主导的职场文化中保持专业和自信。"
    ),
    "favorite household chore": (
        "你最喜欢的家务活是什么？意想不到的解压方式",
        "女性分享了自己意外喜欢的家务活动，有人享受叠衣服，有人热爱吸尘。"
    ),
}

# Subreddit上下文
SUB_CTX = {
    "AskWomenOver30": "30+女性社区", "AskWomen": "女性社区",
    "TwoXChromosomes": "女性视角社区", "TheGirlSurvivalGuide": "女性生活指南",
    "workingmoms": "职场妈妈社区", "Mommit": "妈妈社区",
    "beyondthebump": "新手妈妈社区", "30PlusSkinCare": "30+护肤社区",
    "SkincareAddiction": "护肤社区", "xxfitness": "女性健身社区",
    "WellnessOver30": "30+养生社区", "FrugalFemaleFashion": "女性平价时尚",
    "femalefashionadvice": "女性穿搭建议", "SubscriptionBoxes": "订阅盒子社区",
}

CATEGORY_CTX = {
    "pain": "许多女性在评论区分享了相似的经历和困境。",
    "topic": "话题引发了大量讨论和不同角度的见解。",
    "hobby": "用户们热情分享了各自的体验和建议。",
    "pay": "评论区充满了真实的使用体验和消费建议。",
    "brand": "社区成员分享了对新品牌和新产品的看法。",
    "emotional": "用户们讨论了消费带来的情绪价值和幸福感。",
    "premium": "追求品质生活的女性分享了高端体验心得。",
}

def main():
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    fixed = 0
    still_fallback = 0

    for item in data["insights"]:
        if not str(item.get("id", "")).startswith("auto_"):
            continue

        title_zh = item.get("title_zh", "")
        alpha_count = sum(1 for c in title_zh if c.isascii() and c.isalpha())
        if alpha_count / max(len(title_zh), 1) < 0.3:
            continue  # Already good Chinese

        title_en = item.get("title_en", "")
        url = item.get("source_url", "")
        sub_match = re.search(r'reddit\.com/r/(\w+)', url)
        subreddit = sub_match.group(1) if sub_match else ""
        sub_ctx = SUB_CTX.get(subreddit, "Reddit社区")
        cat = item.get("category", "topic")

        # Try to match
        matched = False
        for pattern, (zh_title, zh_desc) in DIRECT_MAP.items():
            if re.search(re.escape(pattern) if not pattern.startswith('(') else pattern, title_en, re.IGNORECASE):
                item["title_zh"] = zh_title
                score_info = ""
                score_match = re.search(r'(\d+)赞.*?(\d+)评论', item.get("summary_zh", ""))
                if score_match:
                    score_info = f" (获得{score_match.group(1)}赞、{score_match.group(2)}条评论)"
                item["summary_zh"] = f"来自{sub_ctx}的真实讨论。{zh_desc}{score_info} {CATEGORY_CTX.get(cat, '')}"
                matched = True
                fixed += 1
                break

        if not matched:
            still_fallback += 1

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # Final stats
    auto = [i for i in data["insights"] if str(i.get("id","")).startswith("auto_")]
    good = sum(1 for i in auto if sum(1 for c in i['title_zh'] if c.isascii() and c.isalpha()) / max(len(i['title_zh']),1) < 0.3)

    with open("_check2.txt", "w", encoding="utf-8") as f:
        f.write(f"Fixed this round: {fixed}\n")
        f.write(f"Still fallback: {still_fallback}\n")
        f.write(f"Total good Chinese: {good}/{len(auto)}\n\n")
        for item in auto[:10]:
            f.write(f"EN: {item['title_en'][:70]}\n")
            f.write(f"ZH: {item['title_zh'][:70]}\n\n")

    print(f"Fixed: {fixed}, Still fallback: {still_fallback}, Total good: {good}/{len(auto)}")

if __name__ == "__main__":
    main()
