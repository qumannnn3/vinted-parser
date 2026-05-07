import asyncio
import html
import random
import re
import time

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

FRUITS_GRAPHQL_URL = "https://web-server.production.fruitsfamily.com/graphql"
FRUITS_HOME_URL = "https://fruitsfamily.com"

FRUITS_PRODUCT_QUERY = """
query SeeProducts($filter: ProductFilter!, $offset: Int, $limit: Int, $sort: String) {
  searchProducts(filter: $filter, offset: $offset, limit: $limit, sort: $sort) {
    id
    createdAt
    category
    title
    brand
    price
    status
    external_url
    resizedSmallImages
    size
    condition
    discount_rate
    like_count
    is_liked
  }
}
"""

FRUITS_ALLOWED_CATEGORIES = {
    "상의", "아우터", "하의", "신발", "가방", "모자", "액세서리",
}

FRUITS_MIN_MARKET_SAMPLES = 1
FRUITS_MAX_MARKET_RATIO = 0.90
FRUITS_MARKET_PRICE_MAX = 10000000

FRUITS_BLOCKED_WORDS = [
    "perfume", "fragrance", "향수", "룸스프레이",
    "toy", "figure", "book", "camera", "phone", "watch",
    "피규어", "장난감", "책", "카메라", "핸드폰", "시계",
    "fake", "replica", "copy", "가품", "레플리카",
]


def _headers(query):
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": FRUITS_HOME_URL,
        "Referer": f"{FRUITS_HOME_URL}/search/{requests.utils.quote(query)}",
    }


def _slug(text):
    value = str(text or "").strip().lower()
    value = re.sub(r"\s+", "-", value)
    value = re.sub(r"[^a-z0-9가-힣ㄱ-ㅎㅏ-ㅣ_-]+", "", value)
    return value.strip("-") or "item"


def _text_blob(item):
    return " ".join(
        str(item.get(key) or "")
        for key in ("title", "brand", "category", "size", "condition")
    ).lower()


def _has_blocked_word(item):
    text = _text_blob(item)
    return any(word.lower() in text for word in FRUITS_BLOCKED_WORDS)


def fruits_matches_keyword(item, keyword):
    return keyword_matches_text(_text_blob(item), keyword)


def fruits_matches_brand(item, brand):
    return _has_any_term(_text_blob(item), brand_match_terms(brand))


def is_relevant_fruits_item(item, brand):
    if str(item.get("status") or "").lower() != "selling":
        return False
    if item.get("category") not in FRUITS_ALLOWED_CATEGORIES:
        return False
    if _has_blocked_word(item):
        return False
    return fruits_matches_brand(item, brand)


def fruits_fashion_kind(item):
    category = str(item.get("category") or "")
    category_map = {
        "상의": "tops",
        "아우터": "outerwear",
        "하의": "bottoms",
        "신발": "shoes",
        "가방": "bag",
        "모자": "hat",
        "액세서리": "accessory",
    }
    if category in category_map:
        return category_map[category]
    text = _text_blob(item)
    groups = [
        ("shoes", ["sneaker", "shoe", "boots", "loafer", "sandals"]),
        ("bag", ["bag", "backpack", "wallet", "tote", "pouch"]),
        ("tops", ["shirt", "tee", "hoodie", "sweatshirt", "sweater", "knit", "cardigan", "top"]),
        ("outerwear", ["jacket", "coat", "vest", "parka", "down"]),
        ("bottoms", ["pants", "jeans", "denim", "trousers", "shorts", "skirt", "cargo"]),
        ("hat", ["hat", "cap", "beanie"]),
        ("accessory", ["belt", "scarf", "gloves", "accessory"]),
    ]
    for kind, terms in groups:
        if any(term in text for term in terms):
            return kind
    return "other"


def fruits_market_price_krw(items, target_item, brand, keyword=None):
    from market_price import calculate_market_price

    return calculate_market_price(
        items,
        target_item,
        price_getter=lambda item: item.get("price", 0),
        id_getter=lambda item: item.get("id"),
        item_filter=lambda item: (
            is_relevant_fruits_item(item, brand)
            and (not keyword or fruits_matches_keyword(item, keyword))
        ),
        kind_getter=fruits_fashion_kind,
        min_samples=FRUITS_MIN_MARKET_SAMPLES,
    )


def _normalize_fruits_item(item):
    item_id = str(item.get("id") or "")
    title = item.get("title") or "?"
    images = item.get("images") or item.get("resizedImages") or item.get("resizedSmallImages") or []
    return {
        "id": item_id,
        "title": title,
        "brand": item.get("brand") or "",
        "category": item.get("category") or "",
        "price": int(item.get("price") or 0),
        "status": item.get("status") or "",
        "created_at": item.get("createdAt"),
        "images": images,
        "image": images[0] if images else "",
        "size": item.get("size") or "",
        "condition": item.get("condition") or "",
        "like_count": item.get("like_count") or 0,
        "url": f"{FRUITS_HOME_URL}/product/{item_id}/{_slug(title)}" if item_id else FRUITS_HOME_URL,
    }


def fetch_fruits(query, price_min=None, price_max=None):
    proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
    items_by_id = {}
    try:
        for sort in ("RELEVANCE", "POPULAR"):
            for offset in (0, 40):
                payload = {
                    "query": FRUITS_PRODUCT_QUERY,
                    "variables": {
                        "filter": {
                            "query": query,
                            "price_min": int(state["fruits_min"] if price_min is None else price_min),
                            "price_max": int(state["fruits_max"] if price_max is None else price_max),
                            "show_only": "selling",
                        },
                        "offset": offset,
                        "limit": 40,
                        "sort": sort,
                    },
                }
                response = requests.post(
                    FRUITS_GRAPHQL_URL,
                    json=payload,
                    headers=_headers(query),
                    proxies=proxies,
                    timeout=20,
                )
                data = response.json()
                if response.status_code != 200 or data.get("errors"):
                    log.warning(
                        "FruitsFamily GraphQL %s '%s' sort=%s offset=%s: %s",
                        response.status_code, query, sort, offset, data.get("errors"),
                    )
                    continue
                batch = data.get("data", {}).get("searchProducts", []) or []
                for item in batch:
                    normalized = _normalize_fruits_item(item)
                    if normalized["id"]:
                        items_by_id[normalized["id"]] = normalized
                if len(batch) < 40:
                    break
        items = list(items_by_id.values())
        if items:
            log.info("FruitsFamily '%s' -> %s товаров", query, len(items))
        return items
    except Exception as e:
        log.warning("fetch_fruits '%s': %s", query, e)
        return []


def format_fruits_message(item, title_ru, price_line):
    title_safe = html.escape(str(title_ru or item["title"]))
    link_safe = html.escape(str(item["url"]), quote=True)
    details = [str(x) for x in (item.get("brand"), item.get("category"), item.get("size"), item.get("condition")) if x]
    details_line = html.escape(" / ".join(details))
    meta = f"{details_line}\n\n" if details_line else ""
    return (
        "<b>FruitsFamily KR</b>\n"
        f"<b>{title_safe}</b>\n"
        f"{meta}"
        f"<b>Цена:</b> {price_line}\n"
        f"<b>Публикация:</b> {format_msk_timestamp(item.get('created_at'))}\n\n"
        f"<a href='{link_safe}'>Открыть объявление</a>"
    )


async def _send_fruits_item(bot_app, image, msg):
    chat_ids = notification_chat_ids()
    if not chat_ids or not bot_app:
        return
    for chat_id in chat_ids:
        if image:
            try:
                await bot_app.bot.send_document(
                    chat_id=chat_id,
                    document=image,
                    caption=msg,
                    parse_mode="HTML",
                    filename="image.jpg",
                    disable_content_type_detection=True,
                )
                continue
            except Exception as e:
                log.warning("FruitsFamily send_document failed for chat %s: %s", chat_id, e)
        try:
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as e:
            log.warning("FruitsFamily send_message failed for chat %s: %s", chat_id, e)


def fruits_loop(bot_app):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    log.info("FruitsFamily мониторинг запущен")

    while state["fruits_running"]:
        brands = list(state["active_brands"] or ALL_BRANDS)
        random.shuffle(brands)
        state["fruits_stats"]["cycles"] += 1

        for brand in brands:
            if not state["fruits_running"]:
                break
            for query, _keyword in market_search_queries(brand, "fruits"):
                if not state["fruits_running"]:
                    break
                search_queries = [query]
                if _keyword and query.lower().strip() != brand.lower().strip():
                    search_queries.append(brand)
                items_by_id = {}
                market_items_by_id = {}
                for search_query in dict.fromkeys(search_queries):
                    for found in fetch_fruits(search_query):
                        if found.get("id"):
                            items_by_id[found["id"]] = found
                    # Отдельная реальная выборка для рынка: без пользовательского фильтра цены,
                    # чтобы рыночная цена считалась по фактическим объявлениям FruitsFamily.
                    for found in fetch_fruits(search_query, price_min=1, price_max=FRUITS_MARKET_PRICE_MAX):
                        if found.get("id"):
                            market_items_by_id[found["id"]] = found

                market_items = list(market_items_by_id.values()) or list(items_by_id.values())

                for item in items_by_id.values():
                    iid = item.get("id")
                    if not iid or iid in state["fruits_seen"]:
                        continue
                    if not is_relevant_fruits_item(item, brand):
                        log.info("SKIP FruitsFamily filter: %s", item.get("title", "?")[:60])
                        continue
                    if _keyword and not fruits_matches_keyword(item, _keyword):
                        log.info("SKIP FruitsFamily keyword '%s': %s", _keyword, item.get("title", "?")[:60])
                        continue
                    age_ok = age_in_range(
                        item.get("created_at"),
                        state["fruits_min_age_hours"],
                        state["fruits_max_age_hours"],
                    )
                    if age_ok is False:
                        age_hours = publish_age_hours(item.get("created_at"))
                        age_label = f"{age_hours:.1f}h" if age_hours is not None else "unknown"
                        log.info("SKIP FruitsFamily age %s: %s", age_label, item.get("title", "?")[:60])
                        continue

                    market = fruits_market_price_krw(market_items, item, brand, _keyword)
                    if not market:
                        log.info("SKIP FruitsFamily no market sample: %s", item.get("title", "?")[:60])
                        continue
                    market_krw = int(market["price"])
                    market_count = int(market["count"])
                    if item["price"] > market_krw * FRUITS_MAX_MARKET_RATIO:
                        log.info("SKIP FruitsFamily not under market %s/%s: %s", item["price"], market_krw, item.get("title", "?")[:60])
                        continue

                    discount = max(0, round((1 - item["price"] / market_krw) * 100))
                    rate = get_fx_rate("KRW", "EUR")
                    eur = item["price"] * rate if rate else 0
                    if eur:
                        market_eur = market_krw * rate
                        price_line = (
                            f"₩{item['price']:,} (~{eur:.0f} EUR)\n"
                            f"<b>Рынок:</b> ~₩{market_krw:,} (~{market_eur:.0f} EUR), "
                            f"ниже на {discount}% · {market_count} сравн."
                        )
                    else:
                        price_line = (
                            f"₩{item['price']:,}\n"
                            f"<b>Рынок:</b> ~₩{market_krw:,}, ниже на {discount}% · {market_count} сравн."
                        )
                    title_ru = translate_to_ru(item["title"])
                    msg = format_fruits_message(item, title_ru, price_line)
                    state["fruits_seen"].add(iid)
                    state["fruits_stats"]["found"] += 1
                    log.info("FOUND FruitsFamily: %s — ₩%s", item["title"], item["price"])
                    loop.run_until_complete(_send_fruits_item(bot_app, item.get("image"), msg))

                time.sleep(random.uniform(8, 15))

        if state["fruits_running"]:
            time.sleep(state["fruits_interval"])
    loop.close()
