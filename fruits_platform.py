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
    sort_items_newest,
    state,
    throttle_request,
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
    "\uc0c1\uc758", "\uc544\uc6b0\ud130", "\ud558\uc758", "\uc2e0\ubc1c",
    "\uac00\ubc29", "\ubaa8\uc790", "\uc561\uc138\uc11c\ub9ac",
}

FRUITS_MIN_MARKET_SAMPLES = 1
FRUITS_MAX_MARKET_RATIO = 0.90
FRUITS_MARKET_PRICE_MAX = 10000000
FRUITS_OLD_ITEM_STOP_STREAK = 1_000_000_000

FRUITS_BLOCKED_WORDS = [
    "perfume", "fragrance", "\ud5a5\uc218", "\ub8f8\uc2a4\ud504\ub808\uc774",
    "toy", "figure", "book", "camera", "phone", "watch",
    "\ud53c\uaddc\uc5b4", "\uc7a5\ub09c\uac10", "\ucc45", "\uce74\uba54\ub77c", "\ud578\ub4dc\ud3f0", "\uc2dc\uacc4",
    "fake", "replica", "copy", "\uac00\ud488", "\ub808\ud50c\ub9ac\uce74",
]

FRUITS_ALLOWED_SHOE_TERMS = [
    "sneaker", "sneakers", "trainer", "trainers", "runner", "runners",
    "boot", "boots", "hiking", "track", "ramones", "geobasket", "dunks",
    "jordan", "air force", "air max", "yeezy", "gazelle", "samba",
    "10xl", "3xl", "runner", "tyrex", "tyrex37", "zero", "stansmith", "stan smith",
    "\u30b9\u30cb\u30fc\u30ab\u30fc", "\u30b7\u30e5\u30fc\u30ba", "\u30d6\u30fc\u30c4", "\u30c8\u30e9\u30c3\u30af",
    "\uc6b4\ub3d9\ud654", "\uc2a4\ub2c8\ucee4\uc988", "\ubd80\uce20", "\ud2b8\ub799",
    "\ub7ec\ub108", "\ud0c0\uc774\ub809\uc2a4", "\uc81c\ub85c", "\uc2a4\ud0e0\uc2a4\ubbf8\uc2a4",
]

FRUITS_FORMAL_SHOE_TERMS = [
    "loafer", "loafers", "derby", "derbies", "oxford", "oxfords", "moccasin", "moccasins",
    "dress shoe", "dress shoes", "formal shoe", "formal shoes", "leather shoe", "leather shoes",
    "cima", "pump", "pumps", "heel", "heels", "sandal", "sandals",
    "\u30ed\u30fc\u30d5\u30a1\u30fc", "\u30c0\u30fc\u30d3\u30fc", "\u9769\u9774",
    "\u30aa\u30c3\u30af\u30b9\u30d5\u30a9\u30fc\u30c9", "\u30d1\u30f3\u30d7\u30b9",
    "\u30d2\u30fc\u30eb", "\u30b5\u30f3\u30c0\u30eb",
    "\ub85c\ud37c", "\ub354\ube44", "\uad6c\ub450", "\uc815\uc7a5\ud654",
    "\uc625\uc2a4\ud3ec\ub4dc", "\ubaa8\uce74\uc2e0", "\ud38c\ud504\uc2a4", "\ud790", "\uc0cc\ub4e4",
]


def _is_unwanted_fruits_shoe(item):
    if str(item.get("category") or "") != "\uc2e0\ubc1c":
        return False
    text = _text_blob(item)
    if _has_any_term(text, FRUITS_FORMAL_SHOE_TERMS):
        return True
    return not _has_any_term(text, FRUITS_ALLOWED_SHOE_TERMS)


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
    value = re.sub(r"[^a-z0-9к°Ђ-нћЈг„±-г…Ћг…Џ-г…Ј_-]+", "", value)
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
    if any(word.lower() in text for word in FRUITS_BLOCKED_WORDS):
        return True
    # FruitsFamily often puts normal branded womenswear in broad categories.
    # Keep hard accessory/shoe junk filters, but don't reject normal clothing
    # just because the title says "blouse" or another broad garment word.
    return is_unwanted_item_text(text) and not _has_any_term(
        text,
        [
            "blouse", "blouses", "skirt", "dress", "one piece", "one-piece",
            "tank top", "camisole", "leggings",
            "\uc2a4\ucee4\ud2b8", "\uc6d0\ud53c\uc2a4", "\uce90\ubbf8\uc194", "\ub808\uae45\uc2a4",
            "\ube14\ub77c\uc6b0\uc2a4",
        ],
    )


def fruits_matches_keyword(item, keyword):
    return keyword_matches_text(_text_blob(item), keyword)


def fruits_matches_brand(item, brand):
    brand_text = str(item.get("brand") or "").lower()
    if brand_text and _has_any_term(brand_text, brand_match_terms(brand)):
        return True
    text = _text_blob(item)
    # Keep collabs where the shop brand is a parent brand but the requested
    # brand is in the title/metadata; reject unrelated search-result noise.
    if _has_any_term(text, brand_match_terms(brand)):
        return True
    return False


def is_relevant_fruits_item(item, brand):
    if str(item.get("status") or "").lower() != "selling":
        return False
    if item.get("category") not in FRUITS_ALLOWED_CATEGORIES:
        return False
    if _has_blocked_word(item):
        return False
    if _is_unwanted_fruits_shoe(item):
        return False
    return fruits_matches_brand(item, brand)


def fruits_fashion_kind(item):
    if is_unwanted_item_text(_text_blob(item)):
        return ""
    if _is_unwanted_fruits_shoe(item):
        return ""
    category = str(item.get("category") or "")
    category_map = {
        "\uc0c1\uc758": "tops",
        "\uc544\uc6b0\ud130": "outerwear",
        "\ud558\uc758": "bottoms",
        "\uc2e0\ubc1c": "shoes",
        "\uac00\ubc29": "bag",
        "\ubaa8\uc790": "hat",
        "\uc561\uc138\uc11c\ub9ac": "accessory",
    }
    if category in category_map:
        return category_map[category]
    text = _text_blob(item)
    groups = [
        ("shoes", ["sneaker", "shoe", "boots", "loafer", "sandals", "\uc6b4\ub3d9\ud654", "\uc2a4\ub2c8\ucee4\uc988", "\ubd80\uce20"]),
        ("bag", ["bag", "backpack", "wallet", "tote", "pouch", "\uac00\ubc29", "\ubc31\ud329", "\ud1a0\ud2b8\ubc31"]),
        ("tops", ["shirt", "tee", "hoodie", "sweatshirt", "sweater", "knit", "cardigan", "top", "blouse", "tank top", "camisole", "\uc154\uce20", "\ud2f0\uc154\uce20", "\ud6c4\ub4dc", "\ub2c8\ud2b8", "\uac00\ub514\uac74", "\ube14\ub77c\uc6b0\uc2a4", "\uce90\ubbf8\uc194"]),
        ("outerwear", ["jacket", "coat", "vest", "parka", "down", "\uc790\ucf13", "\uc7ac\ud0b7", "\ucf54\ud2b8", "\uc870\ub07c", "\ud328\ub529"]),
        ("bottoms", ["pants", "jeans", "denim", "trousers", "shorts", "skirt", "leggings", "cargo", "\ubc14\uc9c0", "\ud32c\uce20", "\uccad\ubc14\uc9c0", "\ub370\ub2d8", "\uc1fc\uce20", "\uc2a4\ucee4\ud2b8", "\ub808\uae45\uc2a4", "\uce74\uace0"]),
        ("dress", ["dress", "one piece", "one-piece", "\uc6d0\ud53c\uc2a4", "\ud29c\ub2c9"]),
        ("hat", ["hat", "cap", "beanie", "\ubaa8\uc790", "\ucea1", "\ube44\ub2c8", "\ubcfc\ucea1"]),
        ("accessory", ["belt", "scarf", "gloves", "accessory", "\ubca8\ud2b8", "\uba38\ud50c\ub7ec", "\uc7a5\uac11", "\uc561\uc138\uc11c\ub9ac"]),
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
                throttle_request("fruits", 0.8)
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
        items = sort_items_newest(items)
        if items:
            log.info("FruitsFamily '%s' -> %s С‚РѕРІР°СЂРѕРІ", query, len(items))
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
        f"<b>Р¦РµРЅР°:</b> {price_line}\n"
        f"<b>РџСѓР±Р»РёРєР°С†РёСЏ:</b> {format_msk_timestamp(item.get('created_at'))}\n\n"
        f"<a href='{link_safe}'>РћС‚РєСЂС‹С‚СЊ РѕР±СЉСЏРІР»РµРЅРёРµ</a>"
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
    log.info("FruitsFamily РјРѕРЅРёС‚РѕСЂРёРЅРі Р·Р°РїСѓС‰РµРЅ")

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
                    for found in fetch_fruits(search_query):
                        if not is_market_run_current("fruits", run_id):
                            break
                        if found.get("id"):
                            items_by_id[found["id"]] = found


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
                        continue
                    fresh_candidates.append(item)

                if not fresh_candidates:
                    continue

                market_items_by_id = {}
                for search_query in dict.fromkeys(search_queries):
                    # РћС‚РґРµР»СЊРЅР°СЏ СЂРµР°Р»СЊРЅР°СЏ РІС‹Р±РѕСЂРєР° РґР»СЏ СЂС‹РЅРєР°: Р±РµР· РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРѕРіРѕ С„РёР»СЊС‚СЂР° С†РµРЅС‹,
                    # С‡С‚РѕР±С‹ СЂС‹РЅРѕС‡РЅР°СЏ С†РµРЅР° СЃС‡РёС‚Р°Р»Р°СЃСЊ РїРѕ С„Р°РєС‚РёС‡РµСЃРєРёРј РѕР±СЉСЏРІР»РµРЅРёСЏРј FruitsFamily.
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
                            f"в‚©{item['price']:,} (~{eur:.0f} РµРІСЂРѕ)\n"
                            f"<b>Р С‹РЅРѕРє:</b> ~в‚©{market_krw:,} (~{market_eur:.0f} РµРІСЂРѕ), "
                            f"РЅРёР¶Рµ РЅР° {discount}% В· {market_count} СЃСЂР°РІРЅ."
                        )
                    else:
                        price_line = (
                            f"в‚©{item['price']:,}\n"
                            f"<b>Р С‹РЅРѕРє:</b> ~в‚©{market_krw:,}, РЅРёР¶Рµ РЅР° {discount}% В· {market_count} СЃСЂР°РІРЅ."
                        )
                    title_ru = translate_to_ru(item["title"])
                    photo_data = download_image_bytes(item.get("image"), referer=FRUITS_HOME_URL)
                    if not is_market_run_current("fruits", run_id):
                        break
                    msg = format_fruits_message(item, title_ru, price_line)
                    if not mark_item_seen("fruits", iid):
                        continue
                    state["fruits_stats"]["found"] += 1
                    log.info("FOUND FruitsFamily: %s вЂ” в‚©%s", item["title"], item["price"])
                    loop.run_until_complete(_send_fruits_item(bot_app, photo_data, msg, run_id))

                sleep_while_market_running("fruits", run_id, random.uniform(8, 15))

        if is_market_run_current("fruits", run_id):
            sleep_while_market_running("fruits", run_id, state["fruits_interval"])
    loop.close()
