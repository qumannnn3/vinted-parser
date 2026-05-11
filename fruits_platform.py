import asyncio
import html
import random
import re
import time
from io import BytesIO

import requests

from shared import (
    ALL_BRANDS,
    PROXY_URL,
    USER_AGENTS,
    age_in_range,
    brand_match_terms,
    download_image_bytes,
    format_msk_timestamp,
    get_fx_rate,
    has_item_seen,
    is_market_run_current,
    is_unwanted_item_text,
    keyword_matches_text,
    log,
    mark_item_seen,
    market_search_queries,
    notification_chat_ids,
    publish_age_hours,
    run_telegram_coroutine,
    sleep_while_market_running,
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
FRUITS_OLD_ITEM_STOP_STREAK = 8

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


def _base36(value):
    try:
        n = int(value)
    except (TypeError, ValueError):
        return str(value or "").strip()
    if n <= 0:
        return ""
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    result = ""
    while n:
        n, rem = divmod(n, 36)
        result = chars[rem] + result
    return result


def _product_url(item_id, title, external_url=None):
    if external_url:
        url = str(external_url)
        if url.startswith("http://") or url.startswith("https://"):
            return url
        if url.startswith("/"):
            return f"{FRUITS_HOME_URL}{url}"
    short_id = _base36(item_id)
    if not short_id:
        return FRUITS_HOME_URL
    slug = _slug(title)
    return f"{FRUITS_HOME_URL}/product/{short_id}/{slug}"


def _text_blob(item):
    return " ".join(
        str(item.get(key) or "")
        for key in ("title", "brand", "category", "size", "condition")
    ).lower()


def _has_blocked_word(item):
    text = _text_blob(item)
    return is_unwanted_item_text(text) or any(word.lower() in text for word in FRUITS_BLOCKED_WORDS)


def fruits_matches_keyword(item, keyword):
    return keyword_matches_text(_text_blob(item), keyword)


def fruits_matches_brand(item, brand):
    brand_text = str(item.get("brand") or "").lower()
    if brand_text and _has_any_term(brand_text, brand_match_terms(brand)):
        return True
    # Fallback: collab items (e.g. Adidas Jeremy Scott) may have the parent
    # brand in the brand field but the collab name only in the title.
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
    if is_unwanted_item_text(_text_blob(item)):
        return ""
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


def _first_image_url(images):
    if isinstance(images, (dict, str)):
        images = [images]
    for image in images or []:
        if isinstance(image, str) and image.strip():
            return image.strip()
        if isinstance(image, dict):
            for key in ("url", "image_url", "src", "resized_url"):
                value = image.get(key)
                if value:
                    return str(value).strip()
    return ""


def _normalize_fruits_item(item):
    item_id = str(item.get("id") or "")
    title = item.get("title") or "?"
    images = item.get("images") or item.get("resizedImages") or item.get("resizedSmallImages") or []
    image = _first_image_url(images)
    return {
        "id": item_id,
        "title": title,
        "brand": item.get("brand") or "",
        "category": item.get("category") or "",
        "price": int(item.get("price") or 0),
        "status": item.get("status") or "",
        "created_at": item.get("createdAt"),
        "images": images,
        "image": image,
        "size": item.get("size") or "",
        "condition": item.get("condition") or "",
        "like_count": item.get("like_count") or 0,
        "url": _product_url(item_id, title, item.get("external_url")),
    }


def fetch_fruits(query, price_min=None, price_max=None, sort_modes=None):
    proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
    items_by_id = {}
    sort_modes = tuple(sort_modes or ("NEW",))
    try:
        for sort in sort_modes:
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


async def _send_fruits_item(bot_app, photo_data, msg, run_id):
    if not is_market_run_current("fruits", run_id):
        return
    chat_ids = notification_chat_ids()
    if not chat_ids or not bot_app:
        return

    async def send_all():
        for chat_id in chat_ids:
            if not is_market_run_current("fruits", run_id):
                return
            if photo_data:
                try:
                    photo_file = BytesIO(photo_data)
                    photo_file.name = "fruits.jpg"
                    await bot_app.bot.send_photo(
                        chat_id=chat_id,
                        photo=photo_file,
                        caption=msg,
                        parse_mode="HTML",
                    )
                    continue
                except Exception as e:
                    log.warning("FruitsFamily send_photo failed for chat %s: %s", chat_id, e)
            try:
                await bot_app.bot.send_message(
                    chat_id=chat_id,
                    text=msg,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except Exception as e:
                log.warning("FruitsFamily send_message failed for chat %s: %s", chat_id, e)

    run_telegram_coroutine(send_all())


def fruits_loop(bot_app):
    run_id = state.get("fruits_run_id", 0)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    log.info("FruitsFamily мониторинг запущен")

    while is_market_run_current("fruits", run_id):
        brands = list(state["active_brands"] or ALL_BRANDS)
        random.shuffle(brands)
        state["fruits_stats"]["cycles"] += 1

        for brand in brands:
            if not is_market_run_current("fruits", run_id):
                break
            for query, _keyword in market_search_queries(brand, "fruits"):
                if not is_market_run_current("fruits", run_id):
                    break
                search_queries = [query]
                if _keyword and query.lower().strip() != brand.lower().strip():
                    search_queries.append(brand)
                items_by_id = {}
                for search_query in dict.fromkeys(search_queries):
                    old_item_streak = 0
                    for found in fetch_fruits(search_query):
                        if not is_market_run_current("fruits", run_id):
                            break
                        if found.get("id"):
                            items_by_id[found["id"]] = found

                            age_hours = publish_age_hours(found.get("created_at"))
                            if age_hours is not None and age_hours > float(state["fruits_max_age_hours"]):
                                old_item_streak += 1
                                if old_item_streak >= FRUITS_OLD_ITEM_STOP_STREAK:
                                    log.info(
                                        "STOP FruitsFamily newest page '%s': %s старых подряд",
                                        search_query,
                                        old_item_streak,
                                    )
                                    break
                            else:
                                old_item_streak = 0

                fresh_candidates = []

                for item in items_by_id.values():
                    iid = item.get("id")
                    if not iid or has_item_seen("fruits", iid):
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
                    fresh_candidates.append(item)

                if not fresh_candidates:
                    continue

                market_items_by_id = {}
                for search_query in dict.fromkeys(search_queries):
                    # Отдельная реальная выборка для рынка: без пользовательского фильтра цены,
                    # чтобы рыночная цена считалась по фактическим объявлениям FruitsFamily.
                    for found in fetch_fruits(
                        search_query,
                        price_min=1,
                        price_max=FRUITS_MARKET_PRICE_MAX,
                        sort_modes=("RELEVANCE", "POPULAR"),
                    ):
                        if not is_market_run_current("fruits", run_id):
                            break
                        if found.get("id"):
                            market_items_by_id[found["id"]] = found

                market_items = list(market_items_by_id.values()) or list(items_by_id.values())

                for item in fresh_candidates:
                    if not is_market_run_current("fruits", run_id):
                        break
                    iid = item.get("id")
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
                            f"₩{item['price']:,} (~{eur:.0f} евро)\n"
                            f"<b>Рынок:</b> ~₩{market_krw:,} (~{market_eur:.0f} евро), "
                            f"ниже на {discount}% · {market_count} сравн."
                        )
                    else:
                        price_line = (
                            f"₩{item['price']:,}\n"
                            f"<b>Рынок:</b> ~₩{market_krw:,}, ниже на {discount}% · {market_count} сравн."
                        )
                    title_ru = translate_to_ru(item["title"])
                    photo_data = download_image_bytes(item.get("image"), referer=FRUITS_HOME_URL)
                    if not is_market_run_current("fruits", run_id):
                        break
                    msg = format_fruits_message(item, title_ru, price_line)
                    if not mark_item_seen("fruits", iid):
                        continue
                    state["fruits_stats"]["found"] += 1
                    log.info("FOUND FruitsFamily: %s — ₩%s", item["title"], item["price"])
                    loop.run_until_complete(_send_fruits_item(bot_app, photo_data, msg, run_id))

                sleep_while_market_running("fruits", run_id, random.uniform(8, 15))

        if is_market_run_current("fruits", run_id):
            sleep_while_market_running("fruits", run_id, state["fruits_interval"])
    loop.close()
