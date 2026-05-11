import asyncio
import html
import json
import os
import random
import threading
import time
from io import BytesIO
from urllib.parse import quote_plus

import requests

from shared import (
    ALL_BRANDS,
    DEEP_FASHION_BLOCKED_WORDS,
    DEEP_FASHION_SIZE_PATTERN,
    USER_AGENTS,
    age_in_range,
    brand_match_terms,
    download_image_bytes,
    format_msk_timestamp,
    has_brand_disclaimer,
    has_item_seen,
    is_unwanted_item_text,
    keyword_matches_text,
    log,
    mark_item_seen,
    market_search_queries,
    notification_chat_ids,
    publish_age_hours,
    run_telegram_coroutine,
    sort_items_newest,
    state,
    throttle_request,
    translate_to_ru,
    _has_any_term,
)


GRAILED_HOME_URL = "https://www.grailed.com"
GRAILED_ALGOLIA_APP_ID = os.environ.get("GRAILED_ALGOLIA_APP_ID", "MNRWEFSS2Q")
GRAILED_ALGOLIA_API_KEY = os.environ.get("GRAILED_ALGOLIA_API_KEY", "bc9ee1c014521ccf312525a4ef324a16")
GRAILED_INDEX = os.environ.get("GRAILED_ALGOLIA_INDEX", "Listing_production")
GRAILED_SEARCH_URL = (
    f"https://{GRAILED_ALGOLIA_APP_ID.lower()}-1.algolianet.com/1/indexes/*/queries"
    "?x-algolia-agent=Algolia%20for%20JavaScript%20(4.14.3)%3B%20Browser%3B%20JS%20Helper%20(3.11.3)"
)
GRAILED_MARKET_PRICE_MAX = int(os.environ.get("GRAILED_MARKET_PRICE_MAX", "100000"))
GRAILED_MIN_MARKET_SAMPLES = int(os.environ.get("GRAILED_MIN_MARKET_SAMPLES", "1"))
GRAILED_MAX_MARKET_RATIO = float(os.environ.get("GRAILED_MAX_MARKET_RATIO", "0.85"))
GRAILED_OLD_ITEM_STOP_STREAK = 1_000_000_000


GRAILED_KIND_GROUPS = [
    ("shoes", ["sneaker", "sneakers", "shoe", "shoes", "boots", "loafer", "loafers", "sandals"]),
    ("bag", ["bag", "bags", "backpack", "wallet", "shoulder bag", "tote", "pouch"]),
    ("tops", ["shirt", "t-shirt", "tee", "hoodie", "sweatshirt", "sweater", "knit", "cardigan", "polo"]),
    ("outerwear", ["jacket", "coat", "vest", "parka", "down jacket", "windbreaker", "bomber"]),
    ("bottoms", ["pants", "jeans", "denim", "trousers", "shorts", "skirt", "cargo", "slacks"]),
    ("dress", ["dress"]),
    ("accessory", ["belt", "hat", "cap", "beanie", "glasses", "sunglasses", "scarf", "gloves"]),
]
GRAILED_KIND_WORDS = [word for _, words in GRAILED_KIND_GROUPS for word in words]
GRAILED_BLOCKED_WORDS = [
    "style", "inspired", "type", "look", "custom", "reworked", "bootleg",
    "replica", "fake", "copy", "unauthentic", "counterfeit",
    "damaged", "beat", "beater", "flawed", "stain", "stained", "hole", "holes",
    "ripped", "repair", "parts", "sample", "promo",
    "poster", "book", "magazine", "tag", "sticker", "keychain", "box only",
]


def _headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "*/*",
        "Content-Type": "application/x-www-form-urlencoded",
        "x-algolia-api-key": GRAILED_ALGOLIA_API_KEY,
        "x-algolia-application-id": GRAILED_ALGOLIA_APP_ID,
        "Origin": GRAILED_HOME_URL,
        "Referer": f"{GRAILED_HOME_URL}/",
    }


def _text_blob(item):
    parts = [
        item.get("title"),
        item.get("description"),
        item.get("designer_names"),
        item.get("category"),
        item.get("category_path"),
        item.get("category_size"),
        item.get("condition"),
        item.get("size"),
        item.get("location"),
    ]
    for designer in item.get("designers") or []:
        if isinstance(designer, dict):
            parts.append(designer.get("name"))
    return " ".join(str(part) for part in parts if part).lower()


def grailed_matches_brand(item, brand):
    terms = brand_match_terms(brand)
    designer_names = []
    if isinstance(item, dict):
        if item.get("designer_names"):
            designer_names.extend(str(item.get("designer_names")).split(","))
        for designer in item.get("designers") or []:
            if isinstance(designer, dict) and designer.get("name"):
                designer_names.append(designer.get("name"))
    designer_text = " ".join(str(name or "").lower() for name in designer_names)
    if designer_text and _has_any_term(designer_text, terms):
        return True
    # Fallback: collab items (e.g. Adidas Jeremy Scott) may have the parent
    # brand in designers but the collab name only in the title/description.
    return _has_any_term(_text_blob(item), terms)


def grailed_matches_keyword(item, keyword):
    return keyword_matches_text(_text_blob(item), keyword)


def grailed_has_brand_disclaimer(item, brand):
    return has_brand_disclaimer(_text_blob(item), brand)


def is_relevant_grailed_item(item, brand):
    text = _text_blob(item)
    if is_unwanted_item_text(text):
        return False
    if _has_any_term(text, GRAILED_BLOCKED_WORDS):
        return False
    if _has_any_term(text, DEEP_FASHION_BLOCKED_WORDS):
        return False
    if not grailed_matches_brand(item, brand):
        return False
    if grailed_has_brand_disclaimer(item, brand):
        return False
    return _has_any_term(text, GRAILED_KIND_WORDS) or bool(item.get("size")) or bool(DEEP_FASHION_SIZE_PATTERN.search(text))


def grailed_fashion_kind(item):
    raw = item.get("_raw", item) if isinstance(item, dict) else item
    text = _text_blob(raw)
    if is_unwanted_item_text(text):
        return ""
    if _has_any_term(text, GRAILED_BLOCKED_WORDS):
        return ""
    if _has_any_term(text, DEEP_FASHION_BLOCKED_WORDS):
        return ""
    for kind, words in GRAILED_KIND_GROUPS:
        if _has_any_term(text, words):
            return kind
    if DEEP_FASHION_SIZE_PATTERN.search(text):
        return "clothing"
    return ""


def _item_url(item):
    item_id = item.get("id") or item.get("objectID")
    slug = str(item.get("slug") or "").strip("/")
    if slug:
        return f"{GRAILED_HOME_URL}/listings/{item_id}-{slug}" if item_id else f"{GRAILED_HOME_URL}/listings/{slug}"
    return f"{GRAILED_HOME_URL}/listings/{item_id}" if item_id else GRAILED_HOME_URL


def _item_image(item):
    candidates = [
        item.get("cover_photo"),
        item.get("photo"),
        item.get("image"),
        item.get("image_url"),
        item.get("thumbnail"),
    ]
    photos = item.get("photos") or item.get("images") or []
    if isinstance(photos, (dict, str)):
        photos = [photos]
    candidates.extend(photos)

    for image in candidates:
        if isinstance(image, str) and image.strip():
            return image.strip()
        if isinstance(image, dict):
            for key in ("image_url", "url", "src", "large_url", "small_url"):
                value = image.get(key)
                if value:
                    return str(value).strip()
    return ""


def _normalize_item(item):
    return {
        "id": str(item.get("id") or item.get("objectID") or ""),
        "title": item.get("title") or "Grailed item",
        "price": float(item.get("price_i") or item.get("price") or 0),
        "brand": item.get("designer_names") or "",
        "size": item.get("size") or item.get("category_size") or "",
        "condition": item.get("condition") or "",
        "category": item.get("category_path") or item.get("category") or "",
        "created_at": item.get("created_at") or item.get("created_at_i") or item.get("bumped_at"),
        "image": _item_image(item),
        "url": _item_url(item),
        "_raw": item,
    }


def _params(query, price_min, price_max, limit, use_age_filter=True):
    query = quote_plus(str(query or ""))
    numeric_filters = [
        f'"price_i>={float(price_min):g}"',
        f'"price_i<={float(price_max):g}"',
    ]
    if use_age_filter:
        min_created_at = int(time.time() - float(state["grailed_max_age_hours"]) * 3600)
        max_created_at = int(time.time() - float(state["grailed_min_age_hours"]) * 3600)
        numeric_filters.extend([
            f'"created_at_i>={min_created_at}"',
            f'"created_at_i<={max_created_at}"',
        ])
    return (
        "analytics=true"
        "&clickAnalytics=true"
        "&enableABTest=false"
        "&enablePersonalization=false"
        '&facetFilters=[[],[],[],[],[],[],["department:menswear"],[]]'
        "&facets=[]"
        "&filters="
        "&getRankingInfo=true"
        f"&hitsPerPage={int(limit)}"
        f"&numericFilters=[{','.join(numeric_filters)}]"
        "&page=0"
        "&personalizationImpact=0"
        f"&query={query}"
        "&tagFilters="
    )


def fetch_grailed(query, limit=80, price_min=None, price_max=None, use_age_filter=True):
    price_min = state["grailed_min"] if price_min is None else price_min
    price_max = state["grailed_max"] if price_max is None else price_max
    payload = {
        "requests": [
            {
                "indexName": GRAILED_INDEX,
                "params": _params(query, price_min, price_max, limit, use_age_filter=use_age_filter),
            }
        ]
    }
    try:
        throttle_request("grailed", 0.8)
        response = requests.post(
            GRAILED_SEARCH_URL,
            headers=_headers(),
            data=json.dumps(payload),
            timeout=20,
        )
        if response.status_code != 200:
            log.warning("Grailed '%s' HTTP %s: %s", query, response.status_code, response.text[:200])
            return []
        data = response.json()
        hits = []
        for result in data.get("results") or []:
            hits.extend(result.get("hits") or [])
        items = sort_items_newest(_normalize_item(item) for item in hits if item)
        if items:
            log.info("Grailed '%s' -> %s товаров", query, len(items))
        return items
    except Exception as e:
        log.warning("fetch_grailed '%s': %s", query, e)
        return []


def grailed_market_price_usd(items, target_item, brand, keyword=None):
    from market_price import calculate_market_price

    return calculate_market_price(
        items,
        target_item,
        price_getter=lambda item: item.get("price", 0),
        id_getter=lambda item: item.get("id"),
        item_filter=lambda item: (
            grailed_matches_brand(item.get("_raw", item), brand)
            and not grailed_has_brand_disclaimer(item.get("_raw", item), brand)
            and (not keyword or grailed_matches_keyword(item.get("_raw", item), keyword))
        ),
        kind_getter=grailed_fashion_kind,
        min_samples=GRAILED_MIN_MARKET_SAMPLES,
    )


def format_grailed_message(item, title_ru, market_line=""):
    title_safe = html.escape(str(title_ru or item.get("title") or "Grailed item"))
    link_safe = html.escape(str(item.get("url") or GRAILED_HOME_URL), quote=True)
    details = [str(x) for x in (item.get("brand"), item.get("category"), item.get("size"), item.get("condition")) if x]
    details_line = html.escape(" / ".join(details))
    meta = f"{details_line}\n\n" if details_line else ""
    price = float(item.get("price") or 0)
    return (
        "<b>Grailed</b>\n"
        f"<b>{title_safe}</b>\n"
        f"{meta}"
        f"<b>Цена:</b> ${price:g}\n"
        f"{market_line}"
        f"<b>Публикация:</b> {format_msk_timestamp(item.get('created_at'))}\n\n"
        f"<a href='{link_safe}'>Открыть объявление</a>"
    )


async def _send_grailed_item(bot_app, photo_data, msg):
    run_id = getattr(_run_local, "run_id", state.get("grailed_run_id", 0))
    if not _is_run_id_current(run_id):
        return
    chat_ids = notification_chat_ids()
    if not chat_ids or not bot_app:
        return

    async def send_all():
        for chat_id in chat_ids:
            if not _is_run_id_current(run_id):
                return
            if photo_data:
                try:
                    photo_file = BytesIO(photo_data)
                    photo_file.name = "grailed.jpg"
                    await bot_app.bot.send_photo(chat_id=chat_id, photo=photo_file, caption=msg, parse_mode="HTML")
                    continue
                except Exception as e:
                    log.warning("Grailed send_photo failed for chat %s: %s", chat_id, e)
            try:
                await bot_app.bot.send_message(
                    chat_id=chat_id,
                    text=msg,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except Exception as e:
                log.warning("Grailed send_message failed for chat %s: %s", chat_id, e)

    run_telegram_coroutine(send_all())


_run_local = threading.local()


def _is_current_run():
    return _is_run_id_current(getattr(_run_local, "run_id", None))


def _is_run_id_current(run_id):
    return state["grailed_running"] and run_id is not None and state.get("grailed_run_id", 0) == run_id


def _sleep_while_running(seconds):
    end = time.time() + float(seconds)
    while _is_current_run() and time.time() < end:
        time.sleep(min(1.0, end - time.time()))


def grailed_loop(bot_app):
    _run_local.run_id = state.get("grailed_run_id", 0)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    log.info("Grailed мониторинг запущен")

    while _is_current_run():
        brands = list(state["active_brands"] or ALL_BRANDS)
        random.shuffle(brands)
        state["grailed_stats"]["cycles"] += 1

        for brand in brands:
            if not _is_current_run():
                break
            for query, keyword in market_search_queries(brand, "grailed"):
                if not _is_current_run():
                    break
                items = fetch_grailed(query)
                if not _is_current_run():
                    break
                market_items = None
                for item in items:
                    if not _is_current_run():
                        break
                    iid = item.get("id")
                    if not iid or has_item_seen("grailed", iid):
                        continue
                    if not is_relevant_grailed_item(item["_raw"], brand):
                        if grailed_has_brand_disclaimer(item["_raw"], brand):
                            log.info("SKIP Grailed brand/style disclaimer: %s", item.get("title", "?")[:60])
                        continue
                    if keyword and not grailed_matches_keyword(item["_raw"], keyword):
                        continue
                    age_ok = age_in_range(
                        item.get("created_at"),
                        state["grailed_min_age_hours"],
                        state["grailed_max_age_hours"],
                    )
                    if age_ok is False:
                        continue
                    if market_items is None:
                        market_items = fetch_grailed(
                            query,
                            limit=120,
                            price_min=1,
                            price_max=GRAILED_MARKET_PRICE_MAX,
                            use_age_filter=False,
                        ) or items
                        if not _is_current_run():
                            break
                    market = grailed_market_price_usd(market_items, item, brand, keyword)
                    if not market:
                        log.info("SKIP Grailed no market sample: %s", item.get("title", "?")[:60])
                        continue
                    market_usd = float(market["price"])
                    market_count = int(market["count"])
                    price = float(item.get("price") or 0)
                    if price > market_usd * GRAILED_MAX_MARKET_RATIO:
                        log.info(
                            "SKIP Grailed not under market %.0f/%.0f: %s",
                            price,
                            market_usd,
                            item.get("title", "?")[:60],
                        )
                        continue
                    discount = max(0, round((1 - price / market_usd) * 100))
                    market_line = f"<b>Рынок:</b> ~${market_usd:.0f}, ниже на {discount}% · {market_count} сравн.\n"

                    if not _is_current_run():
                        break
                    title_ru = translate_to_ru(item.get("title", ""))
                    photo_data = download_image_bytes(item.get("image"), referer=GRAILED_HOME_URL)
                    if not _is_current_run():
                        break
                    msg = format_grailed_message(item, title_ru, market_line)
                    if not mark_item_seen("grailed", iid):
                        continue
                    state["grailed_stats"]["found"] += 1
                    log.info("FOUND Grailed: %s - $%s", item.get("title", "?"), item.get("price"))
                    loop.run_until_complete(_send_grailed_item(bot_app, photo_data, msg))

                _sleep_while_running(random.uniform(8, 15))

        if _is_current_run():
            _sleep_while_running(state["grailed_interval"])
    loop.close()
