import asyncio
import hashlib
import html
import json
import os
import random
import re
import time
from http.cookies import SimpleCookie
from urllib.parse import quote, urljoin

import requests

from shared import (
    ALL_BRANDS,
    PROXY_URL,
    USER_AGENTS,
    age_in_range,
    brand_match_terms,
    format_msk_timestamp,
    get_fx_rate,
    keyword_matches_text,
    log,
    market_search_queries,
    notification_chat_ids,
    publish_age_hours,
    state,
    translate_to_ru,
    _has_any_term,
)


GOFISH_HOME_URL = "https://www.goofish.com"
GOFISH_MTOP_HOST = "https://h5api.m.goofish.com"
GOFISH_SEARCH_API = "mtop.taobao.idlemtopsearch.pc.search"
GOFISH_APP_KEY = os.environ.get("GOOFISH_APP_KEY", "34839810")
GOFISH_COOKIE = os.environ.get("GOOFISH_COOKIE", os.environ.get("GOFISH_COOKIE_STRING", ""))

GOFISH_MARKET_PRICE_MAX = 10_000_000
GOFISH_MIN_MARKET_SAMPLES = 1
GOFISH_MAX_MARKET_RATIO = 0.90
GOFISH_TIMEOUT = 15
GOFISH_TIMEOUT_LIMIT = 3

_gofish_timeout_streak = 0
_gofish_disabled_until = 0


GOFISH_BLOCKED_WORDS = [
    "perfume", "fragrance", "향수", "룸스프레이",
    "toy", "figure", "book", "camera", "phone", "watch",
    "피규어", "장난감", "책", "카메라", "핸드폰", "시계",
    "fake", "replica", "copy", "가품", "레플리카",
    "仿", "复刻", "高仿", "假", "山寨",
]

GOFISH_KIND_WORDS = [
    "shirt", "tee", "t-shirt", "hoodie", "sweatshirt", "knit", "cardigan",
    "jacket", "coat", "pants", "jeans", "denim", "trousers", "shorts",
    "skirt", "dress", "sneaker", "shoes", "boots", "loafer", "sandals",
    "bag", "backpack", "wallet", "cap", "hat", "beanie", "belt",
    "衬衫", "短袖", "卫衣", "毛衣", "针织", "开衫", "夹克", "外套",
    "大衣", "裤", "牛仔", "短裤", "裙", "连衣裙", "鞋", "靴",
    "包", "背包", "钱包", "帽", "腰带",
    "셔츠", "티셔츠", "후드", "맨투맨", "니트", "가디건", "자켓", "재킷",
    "코트", "팬츠", "바지", "진", "데님", "스커트", "드레스", "원피스",
    "스니커즈", "신발", "부츠", "로퍼", "샌들", "가방", "백팩", "지갑",
    "캡", "모자", "비니", "벨트",
]


def _make_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,ko;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": GOFISH_HOME_URL,
        "Referer": f"{GOFISH_HOME_URL}/search",
    })
    if PROXY_URL:
        session.proxies = {"http": PROXY_URL, "https": PROXY_URL}
    if GOOFISH_COOKIE:
        session.headers["Cookie"] = GOOFISH_COOKIE
        session.cookies.update(_cookie_dict(GOOFISH_COOKIE))
    return session


def _cookie_dict(cookie_text):
    result = {}
    try:
        c = SimpleCookie()
        c.load(cookie_text)
        for key, morsel in c.items():
            result[key] = morsel.value
    except Exception:
        for part in str(cookie_text or "").split(";"):
            if "=" in part:
                k, v = part.split("=", 1)
                result[k.strip()] = v.strip()
    return result


def _token_from_session(session):
    token = session.cookies.get("_m_h5_tk") or _cookie_dict(GOOFISH_COOKIE).get("_m_h5_tk")
    if not token:
        return ""
    return str(token).split("_", 1)[0]


def _sign(token, timestamp_ms, data_text):
    raw = f"{token}&{timestamp_ms}&{GOFISH_APP_KEY}&{data_text}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _text_blob(item):
    return " ".join(
        str(item.get(key) or "")
        for key in ("title", "brand", "category", "size", "condition", "description", "area", "seller")
    ).lower()


def _price_int(value):
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(float(value))
    text = "".join(str(x.get("text", "")) for x in value) if isinstance(value, list) else str(value)
    text = text.replace("¥", "").replace("￥", "").replace("CNY", "").replace("元", "")
    digits = re.sub(r"[^\d.]", "", text)
    if not digits:
        return 0
    try:
        return int(float(digits))
    except Exception:
        return 0


def _parse_ts(value):
    if value is None:
        return time.time()
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 10_000_000_000:
            ts /= 1000
        return ts
    text = str(value).strip()
    now = time.time()

    patterns = [
        (r"(\d+)\s*(?:秒|sec|second)", 1),
        (r"(\d+)\s*(?:分钟|分|min|minute)", 60),
        (r"(\d+)\s*(?:小时|时|hour|hr|h)", 3600),
        (r"(\d+)\s*(?:天|日|day|d)", 86400),
    ]
    for pattern, mul in patterns:
        m = re.search(pattern, text, re.I)
        if m:
            return now - int(m.group(1)) * mul

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y.%m.%d %H:%M", "%Y.%m.%d", "%Y-%m-%d"):
        try:
            return time.mktime(time.strptime(text[:len(fmt)], fmt))
        except Exception:
            pass
    return now


def _deep_get(obj, *path):
    cur = obj
    for key in path:
        if isinstance(cur, dict):
            cur = cur.get(key)
        elif isinstance(cur, list) and isinstance(key, int) and 0 <= key < len(cur):
            cur = cur[key]
        else:
            return None
    return cur


def _normalize_result_item(raw):
    data = raw.get("data", raw) if isinstance(raw, dict) else {}
    item = data.get("item", data) if isinstance(data, dict) else {}
    main = item.get("main", item) if isinstance(item, dict) else {}
    ex = main.get("exContent", main) if isinstance(main, dict) else {}

    title = (
        ex.get("title")
        or ex.get("itemTitle")
        or ex.get("name")
        or _deep_get(main, "clickParam", "args", "title")
        or ""
    )
    item_id = (
        ex.get("itemId")
        or ex.get("item_id")
        or ex.get("id")
        or _deep_get(main, "clickParam", "args", "item_id")
        or _deep_get(main, "clickParam", "args", "itemId")
    )
    price = _price_int(ex.get("price") or ex.get("soldPrice") or ex.get("priceText") or ex.get("reservePrice"))
    image = ex.get("picUrl") or ex.get("pic_url") or ex.get("imageUrl") or ex.get("cover") or ""
    seller = ex.get("userNickName") or ex.get("sellerNick") or ex.get("nick") or ""
    area = ex.get("area") or ex.get("location") or ""
    created = ex.get("publishTime") or ex.get("publishTimeText") or ex.get("createdAt") or ex.get("gmtCreate")

    target_url = _deep_get(main, "targetUrl") or ex.get("detailUrl") or ex.get("url") or ""
    if not target_url and item_id:
        target_url = f"{GOFISH_HOME_URL}/item?id={item_id}"
    target_url = urljoin(GOFISH_HOME_URL, str(target_url))

    if image and image.startswith("//"):
        image = "https:" + image
    elif image:
        image = urljoin(GOFISH_HOME_URL, image)

    return {
        "id": str(item_id or abs(hash(json.dumps(raw, ensure_ascii=False, sort_keys=True)))) ,
        "title": str(title or "").strip(),
        "brand": "",
        "category": "",
        "price": price,
        "created_at": _parse_ts(created),
        "image": image,
        "url": target_url,
        "seller": seller,
        "area": area,
        "status": "selling",
        "description": "",
    }


def _extract_items(payload):
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    result_list = (
        data.get("resultList")
        or data.get("list")
        or data.get("items")
        or _deep_get(data, "data", "resultList")
        or []
    )
    items = []
    if isinstance(result_list, list):
        for raw in result_list:
            if isinstance(raw, dict):
                item = _normalize_result_item(raw)
                if item["title"] and item["price"]:
                    items.append(item)
    return _dedupe_items(items)


def _dedupe_items(items):
    result = []
    seen = set()
    for item in items:
        iid = str(item.get("id") or item.get("url") or item.get("title"))
        if not iid or iid in seen:
            continue
        seen.add(iid)
        result.append(item)
    return result


def _search_payload(query, page=1, rows=30, price_min=None, price_max=None):
    payload = {
        "pageNumber": page,
        "rowsPerPage": rows,
        "keyword": query,
        "fromFilter": False,
        "searchReqFromPage": "xyHome",
        "searchTabType": "SEARCH_TAB_MAIN",
        "forceUseInputKeyword": True,
        "disableHierarchicalSort": 0,
        "sortField": "create",
        "sortValue": "desc",
        "extraFilterValue": json.dumps({
            "divisionList": [],
            "excludeMultiPlacesSellers": "0",
        }, ensure_ascii=False),
    }
    if price_min is not None or price_max is not None:
        payload["filter"] = {
            "priceRange": {
                "from": int(price_min or 0),
                "to": int(price_max or GOFISH_MARKET_PRICE_MAX),
            }
        }
    return payload


def _mtop_search(query, price_min=None, price_max=None):
    global _gofish_timeout_streak, _gofish_disabled_until

    if _gofish_disabled_until and time.time() < _gofish_disabled_until:
        log.warning("Goofish временно отключен после timeout, осталось %ss", int(_gofish_disabled_until - time.time()))
        return []

    session = _make_session()

    # Первый GET часто нужен, чтобы сервер поставил _m_h5_tk, если cookie нет.
    try:
        session.get(f"{GOFISH_HOME_URL}/search?q={quote(query)}", timeout=(4, 8))
    except Exception:
        pass

    token = _token_from_session(session)
    if not token:
        log.error("Goofish cookie не задан или нет _m_h5_tk. Добавь env GOOFISH_COOKIE из браузера.")
        return []

    data_obj = _search_payload(query, page=1, rows=30, price_min=price_min, price_max=price_max)
    data_text = json.dumps(data_obj, ensure_ascii=False, separators=(",", ":"))
    t = str(int(time.time() * 1000))
    sign = _sign(token, t, data_text)

    params = {
        "jsv": "2.7.2",
        "appKey": GOFISH_APP_KEY,
        "t": t,
        "sign": sign,
        "v": "1.0",
        "type": "originaljson",
        "accountSite": "xianyu",
        "dataType": "json",
        "timeout": "20000",
        "api": GOFISH_SEARCH_API,
        "sessionOption": "AutoLoginOnly",
        "spm_cnt": "a21ybx.search.0.0",
    }

    url = f"{GOFISH_MTOP_HOST}/h5/{GOFISH_SEARCH_API}/1.0/"
    try:
        response = session.post(url, params=params, data={"data": data_text}, timeout=(5, GOFISH_TIMEOUT))
        payload = response.json()
        ret = payload.get("ret", [])
        if not any("SUCCESS" in str(x) for x in ret):
            log.warning("Goofish API ret=%s query=%r", ret, query)
            if any("TOKEN" in str(x).upper() or "FAIL_SYS" in str(x).upper() for x in ret):
                log.error("Goofish cookie устарел. Обнови env GOOFISH_COOKIE.")
            return []

        _gofish_timeout_streak = 0
        return _extract_items(payload)
    except requests.exceptions.Timeout:
        _gofish_timeout_streak += 1
        log.warning("fetch_goofish '%s': timeout %ss streak=%s", query, GOFISH_TIMEOUT, _gofish_timeout_streak)
        if _gofish_timeout_streak >= GOFISH_TIMEOUT_LIMIT:
            _gofish_disabled_until = time.time() + 600
            log.error("Goofish отключен на 10 минут: API не отвечает")
        return []
    except Exception as e:
        log.warning("fetch_goofish '%s': %s", query, e)
        return []


def fetch_gofish(query, price_min=None, price_max=None):
    items = _mtop_search(query, price_min=price_min, price_max=price_max)
    min_price = int(state["gofish_min"] if price_min is None else price_min)
    max_price = int(state["gofish_max"] if price_max is None else price_max)
    items = [item for item in items if min_price <= int(item.get("price") or 0) <= max_price]

    if items:
        log.info("Goofish '%s' -> %s товаров", query, len(items))
    else:
        log.info("Goofish '%s' -> 0 товаров", query)
    return items


def gofish_matches_brand(item, brand):
    return _has_any_term(_text_blob(item), brand_match_terms(brand))


def gofish_matches_keyword(item, keyword):
    return keyword_matches_text(_text_blob(item), keyword)


def _has_blocked_word(item):
    text = _text_blob(item)
    return any(word.lower() in text for word in GOFISH_BLOCKED_WORDS)


def is_relevant_gofish_item(item, brand):
    if not item.get("price"):
        return False
    if _has_blocked_word(item):
        return False
    if not gofish_matches_brand(item, brand):
        return False
    return True


def gofish_fashion_kind(item):
    text = _text_blob(item)
    groups = [
        ("shoes", ["sneaker", "shoe", "boots", "loafer", "sandals", "鞋", "靴", "스니커즈", "신발", "부츠"]),
        ("bag", ["bag", "backpack", "wallet", "包", "背包", "钱包", "가방", "백팩", "지갑"]),
        ("tops", ["shirt", "tee", "hoodie", "sweatshirt", "sweater", "knit", "cardigan", "top", "衬衫", "短袖", "卫衣", "毛衣", "针织"]),
        ("outerwear", ["jacket", "coat", "vest", "parka", "down", "夹克", "外套", "大衣", "羽绒", "자켓", "코트"]),
        ("bottoms", ["pants", "jeans", "denim", "trousers", "shorts", "skirt", "裤", "牛仔", "短裤", "裙", "팬츠", "데님"]),
        ("hat", ["hat", "cap", "beanie", "帽", "캡", "모자"]),
        ("accessory", ["belt", "scarf", "gloves", "腰带", "围巾", "벨트"]),
    ]
    for kind, terms in groups:
        if _has_any_term(text, terms):
            return kind
    return "other"


def gofish_market_price_krw(items, target_item, brand, keyword=None):
    from market_price import calculate_market_price

    return calculate_market_price(
        items,
        target_item,
        price_getter=lambda item: item.get("price", 0),
        id_getter=lambda item: item.get("id"),
        item_filter=lambda item: (
            is_relevant_gofish_item(item, brand)
            and (not keyword or gofish_matches_keyword(item, keyword))
        ),
        kind_getter=gofish_fashion_kind,
        min_samples=GOFISH_MIN_MARKET_SAMPLES,
    )


def format_gofish_message(item, title_ru, price_line):
    link_safe = html.escape(str(item.get("url") or GOFISH_HOME_URL), quote=True)
    title_safe = html.escape(str(title_ru or item.get("title") or "?"))
    meta = []
    if item.get("seller"):
        meta.append(f"<b>Продавец:</b> {html.escape(str(item['seller']))}")
    if item.get("area"):
        meta.append(f"<b>Город:</b> {html.escape(str(item['area']))}")
    meta_text = ("\n".join(meta) + "\n") if meta else ""

    return (
        "<b>Goofish CN</b>\n"
        f"<b>{title_safe}</b>\n\n"
        f"{meta_text}"
        f"<b>Цена:</b> {price_line}\n"
        f"<b>Публикация:</b> {format_msk_timestamp(item.get('created_at'))}\n\n"
        f"<a href='{link_safe}'>Открыть объявление</a>"
    )


async def _send_gofish_item(bot_app, image, msg):
    chat_ids = notification_chat_ids()
    if not chat_ids or not bot_app:
        return
    for chat_id in chat_ids:
        if image:
            try:
                await bot_app.bot.send_photo(chat_id=chat_id, photo=image, caption=msg, parse_mode="HTML")
                continue
            except Exception as e:
                log.warning("Goofish send_photo failed for chat %s: %s", chat_id, e)
        try:
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as e:
            log.warning("Goofish send_message failed for chat %s: %s", chat_id, e)


def gofish_loop(bot_app):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    log.info("Goofish мониторинг запущен")

    while state["gofish_running"]:
        brands = list(state["active_brands"] or ALL_BRANDS)
        random.shuffle(brands)
        state["gofish_stats"]["cycles"] += 1

        for brand in brands:
            if not state["gofish_running"]:
                break
            for query, keyword in market_search_queries(brand, "gofish"):
                if not state["gofish_running"]:
                    break

                items_by_id = {}
                market_items_by_id = {}

                for found in fetch_gofish(query):
                    if found.get("id"):
                        items_by_id[found["id"]] = found

                items = list(items_by_id.values())
                if not items:
                    log.info("SKIP Goofish no items for query: %s", query)
                    continue

                for found in fetch_gofish(query, price_min=1, price_max=GOFISH_MARKET_PRICE_MAX):
                    if found.get("id"):
                        market_items_by_id[found["id"]] = found

                market_items = list(market_items_by_id.values()) or items

                for item in items:
                    iid = item.get("id")
                    if not iid or iid in state["gofish_seen"]:
                        continue

                    if not is_relevant_gofish_item(item, brand):
                        log.info("SKIP Goofish filter: %s", item.get("title", "?")[:60])
                        continue
                    if keyword and not gofish_matches_keyword(item, keyword):
                        log.info("SKIP Goofish keyword '%s': %s", keyword, item.get("title", "?")[:60])
                        continue

                    age_ok = age_in_range(
                        item.get("created_at"),
                        state["gofish_min_age_hours"],
                        state["gofish_max_age_hours"],
                    )
                    if age_ok is False:
                        age_hours = publish_age_hours(item.get("created_at"))
                        age_label = f"{age_hours:.1f}h" if age_hours is not None else "unknown"
                        log.info("SKIP Goofish age %s: %s", age_label, item.get("title", "?")[:60])
                        continue

                    market = gofish_market_price_krw(market_items, item, brand, keyword)
                    if not market:
                        log.info("SKIP Goofish no market sample: %s", item.get("title", "?")[:60])
                        continue

                    market_cny = int(market["price"])
                    market_count = int(market["count"])
                    price = int(item["price"])
                    if price > market_cny * GOFISH_MAX_MARKET_RATIO:
                        log.info("SKIP Goofish not under market %s/%s: %s", price, market_cny, item.get("title", "?")[:60])
                        continue

                    discount = max(0, round((1 - price / market_cny) * 100))
                    rate = get_fx_rate("CNY", "EUR", fallback=0.13)
                    eur = price * rate
                    market_eur = market_cny * rate
                    price_line = (
                        f"¥{price:,} CNY (~{eur:.0f} EUR)\n"
                        f"<b>Рынок:</b> ~¥{market_cny:,} CNY (~{market_eur:.0f} EUR), "
                        f"ниже на {discount}% · {market_count} сравн."
                    )

                    title_ru = translate_to_ru(item.get("title", ""))
                    msg = format_gofish_message(item, title_ru, price_line)
                    state["gofish_seen"].add(iid)
                    state["gofish_stats"]["found"] += 1
                    log.info("FOUND Goofish: %s — ¥%s", item["title"], item["price"])
                    loop.run_until_complete(_send_gofish_item(bot_app, item.get("image"), msg))

                time.sleep(random.uniform(8, 15))

        if state["gofish_running"]:
            time.sleep(state["gofish_interval"])

    loop.close()
