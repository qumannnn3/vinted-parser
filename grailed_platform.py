import asyncio
import html
import json
import os
import random
import time
from urllib.parse import quote_plus

import requests

from shared import (
    ALL_BRANDS,
    DEEP_FASHION_BLOCKED_WORDS,
    DEEP_FASHION_SIZE_PATTERN,
    USER_AGENTS,
    age_in_range,
    brand_match_terms,
    format_msk_timestamp,
    keyword_matches_text,
    log,
    market_search_queries,
    notification_chat_ids,
    publish_age_hours,
    run_telegram_coroutine,
    state,
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


GRAILED_KIND_WORDS = [
    "shirt", "t-shirt", "tee", "hoodie", "sweatshirt", "sweater", "knit",
    "jacket", "coat", "pants", "jeans", "denim", "trousers", "shorts",
    "skirt", "dress", "sneaker", "sneakers", "shoe", "shoes", "boots",
    "bag", "wallet", "belt", "hat", "cap", "beanie", "glasses",
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
    return _has_any_term(_text_blob(item), brand_match_terms(brand))


def grailed_matches_keyword(item, keyword):
    return keyword_matches_text(_text_blob(item), keyword)


def is_relevant_grailed_item(item, brand):
    text = _text_blob(item)
    if _has_any_term(text, DEEP_FASHION_BLOCKED_WORDS):
        return False
    if not grailed_matches_brand(item, brand):
        return False
    return _has_any_term(text, GRAILED_KIND_WORDS) or bool(item.get("size")) or bool(DEEP_FASHION_SIZE_PATTERN.search(text))


def _item_url(item):
    item_id = item.get("id") or item.get("objectID")
    slug = str(item.get("slug") or "").strip("/")
    if slug:
        return f"{GRAILED_HOME_URL}/listings/{item_id}-{slug}" if item_id else f"{GRAILED_HOME_URL}/listings/{slug}"
    return f"{GRAILED_HOME_URL}/listings/{item_id}" if item_id else GRAILED_HOME_URL


def _item_image(item):
    cover = item.get("cover_photo") or {}
    if isinstance(cover, dict):
        return cover.get("image_url") or cover.get("url") or ""
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


def _params(query, price_min, price_max, limit):
    query = quote_plus(str(query or ""))
    min_created_at = int(time.time() - float(state["grailed_max_age_hours"]) * 3600)
    max_created_at = int(time.time() - float(state["grailed_min_age_hours"]) * 3600)
    numeric_filters = [
        f'"price_i>={float(price_min):g}"',
        f'"price_i<={float(price_max):g}"',
        f'"created_at_i>={min_created_at}"',
        f'"created_at_i<={max_created_at}"',
    ]
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


def fetch_grailed(query, limit=30):
    payload = {
        "requests": [
            {
                "indexName": GRAILED_INDEX,
                "params": _params(query, state["grailed_min"], state["grailed_max"], limit),
            }
        ]
    }
    try:
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
        items = [_normalize_item(item) for item in hits if item]
        if items:
            log.info("Grailed '%s' -> %s товаров", query, len(items))
        return items
    except Exception as e:
        log.warning("fetch_grailed '%s': %s", query, e)
        return []


def format_grailed_message(item, title_ru):
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
        f"<b>Price:</b> ${price:g}\n"
        f"<b>Published:</b> {format_msk_timestamp(item.get('created_at'))}\n\n"
        f"<a href='{link_safe}'>Open listing</a>"
    )


async def _send_grailed_item(bot_app, image, msg):
    chat_ids = notification_chat_ids()
    if not chat_ids or not bot_app:
        return

    async def send_all():
        for chat_id in chat_ids:
            if image:
                try:
                    await bot_app.bot.send_photo(chat_id=chat_id, photo=image, caption=msg, parse_mode="HTML")
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


def grailed_loop(bot_app):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    log.info("Grailed мониторинг запущен")

    while state["grailed_running"]:
        brands = list(state["active_brands"] or ALL_BRANDS)
        random.shuffle(brands)
        state["grailed_stats"]["cycles"] += 1

        for brand in brands:
            if not state["grailed_running"]:
                break
            for query, keyword in market_search_queries(brand, "grailed"):
                if not state["grailed_running"]:
                    break
                for item in fetch_grailed(query):
                    iid = item.get("id")
                    if not iid or iid in state["grailed_seen"]:
                        continue
                    if not is_relevant_grailed_item(item["_raw"], brand):
                        continue
                    if keyword and not grailed_matches_keyword(item["_raw"], keyword):
                        continue
                    age_ok = age_in_range(
                        item.get("created_at"),
                        state["grailed_min_age_hours"],
                        state["grailed_max_age_hours"],
                    )
                    if age_ok is False:
                        age_hours = publish_age_hours(item.get("created_at"))
                        age_label = f"{age_hours:.1f}h" if age_hours is not None else "unknown"
                        log.info("SKIP Grailed age %s: %s", age_label, item.get("title", "?")[:60])
                        continue

                    state["grailed_seen"].add(iid)
                    if not state.get("grailed_bootstrap_done"):
                        continue

                    title_ru = translate_to_ru(item.get("title", ""))
                    msg = format_grailed_message(item, title_ru)
                    state["grailed_stats"]["found"] += 1
                    log.info("FOUND Grailed: %s - $%s", item.get("title", "?"), item.get("price"))
                    loop.run_until_complete(_send_grailed_item(bot_app, item.get("image"), msg))

                time.sleep(random.uniform(8, 15))

        state["grailed_bootstrap_done"] = True
        if state["grailed_running"]:
            time.sleep(state["grailed_interval"])
    loop.close()
