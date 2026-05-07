import asyncio
import html
import json
import random
import re
import time
from urllib.parse import quote, urljoin

import requests

from shared import (
    ALL_BRANDS,
    MAX_AGE_HOURS,
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


GOFISH_HOME_URL = "https://www.gofish.co.kr"
GOFISH_MARKET_PRICE_MAX = 10_000_000
GOFISH_MIN_MARKET_SAMPLES = 1
GOFISH_MAX_MARKET_RATIO = 0.90
GOFISH_TIMEOUT = 8
GOFISH_SLEEP_ON_TIMEOUT = 3

GOFISH_BLOCKED_WORDS = [
    "perfume", "fragrance", "향수", "룸스프레이",
    "toy", "figure", "book", "camera", "phone", "watch",
    "피규어", "장난감", "책", "카메라", "핸드폰", "시계",
    "fake", "replica", "copy", "가품", "레플리카",
]

GOFISH_CATEGORY_WORDS = [
    "shirt", "tee", "t-shirt", "hoodie", "sweatshirt", "knit", "cardigan",
    "jacket", "coat", "pants", "jeans", "denim", "trousers", "shorts",
    "skirt", "dress", "sneaker", "shoes", "boots", "loafer", "sandals",
    "bag", "backpack", "wallet", "cap", "hat", "beanie", "belt",
    "셔츠", "티셔츠", "후드", "맨투맨", "니트", "가디건", "자켓", "재킷",
    "코트", "팬츠", "바지", "진", "데님", "스커트", "드레스", "원피스",
    "스니커즈", "신발", "부츠", "로퍼", "샌들", "가방", "백팩", "지갑",
    "캡", "모자", "비니", "벨트",
]


def _headers(query=""):
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "close",
        "Referer": f"{GOFISH_HOME_URL}/search?keyword={quote(str(query or ''))}",
    }


def _text_blob(item):
    return " ".join(
        str(item.get(key) or "")
        for key in ("title", "brand", "category", "size", "condition", "description")
    ).lower()


def _price_int(value):
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else 0


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

    m = re.search(r"(\d+)\s*(?:분|minute|min)", text, re.I)
    if m:
        return now - int(m.group(1)) * 60
    m = re.search(r"(\d+)\s*(?:시간|hour|hr|h)", text, re.I)
    if m:
        return now - int(m.group(1)) * 3600
    m = re.search(r"(\d+)\s*(?:일|day|d)", text, re.I)
    if m:
        return now - int(m.group(1)) * 86400

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y.%m.%d %H:%M", "%Y.%m.%d", "%Y-%m-%d"):
        try:
            return time.mktime(time.strptime(text[:len(fmt)], fmt))
        except Exception:
            pass
    return now


def _extract_json_objects(html_text):
    objects = []
    for m in re.finditer(r"\{[^{}]*(?:title|name|productName|goodsName)[^{}]*\}", html_text, re.I | re.S):
        raw = m.group(0)
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if isinstance(obj, dict):
            objects.append(obj)
    return objects


def _extract_cards_from_html(html_text):
    cards = []
    # Достаём ссылки на товары и берём небольшой HTML-блок вокруг них.
    for m in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html_text, re.I | re.S):
        href, inner = m.group(1), m.group(2)
        if not re.search(r"(product|goods|item|market|products|shop)", href, re.I):
            continue

        block_start = max(0, m.start() - 1500)
        block_end = min(len(html_text), m.end() + 1500)
        block = html_text[block_start:block_end]

        title = re.sub(r"<[^>]+>", " ", inner)
        title = html.unescape(re.sub(r"\s+", " ", title)).strip()
        if len(title) < 2:
            alt = re.search(r'alt=["\']([^"\']+)["\']', block, re.I)
            title = html.unescape(alt.group(1)).strip() if alt else ""

        price_match = re.search(r'([\d,]{3,})\s*(?:원|KRW|₩)', block, re.I)
        price = _price_int(price_match.group(1)) if price_match else 0
        if not title or not price:
            continue

        img = ""
        img_match = re.search(r'<img[^>]+(?:src|data-src|data-original)=["\']([^"\']+)["\']', block, re.I)
        if img_match:
            img = urljoin(GOFISH_HOME_URL, html.unescape(img_match.group(1)))

        created_raw = ""
        age_match = re.search(r'(\d+\s*(?:분|시간|일|minute|min|hour|hr|day|d)\s*(?:전|ago)?)', block, re.I)
        if age_match:
            created_raw = age_match.group(1)

        url = urljoin(GOFISH_HOME_URL, html.unescape(href))
        item_id = re.sub(r"[^a-zA-Z0-9_-]", "", href.split("/")[-1].split("?")[0]) or str(abs(hash(url)))
        cards.append({
            "id": item_id,
            "title": title,
            "brand": "",
            "category": "",
            "price": price,
            "created_at": _parse_ts(created_raw),
            "image": img,
            "url": url,
            "status": "selling",
        })
    return _dedupe_items(cards)


def _normalize_json_item(obj):
    title = (
        obj.get("title")
        or obj.get("name")
        or obj.get("productName")
        or obj.get("goodsName")
        or obj.get("goods_name")
        or ""
    )
    price = _price_int(
        obj.get("price")
        or obj.get("sellPrice")
        or obj.get("salePrice")
        or obj.get("goodsPrice")
        or obj.get("amount")
    )
    item_id = (
        obj.get("id")
        or obj.get("productId")
        or obj.get("goodsNo")
        or obj.get("goodsId")
        or obj.get("itemId")
    )
    url = obj.get("url") or obj.get("link") or obj.get("productUrl") or ""
    if item_id and not url:
        url = f"{GOFISH_HOME_URL}/product/{item_id}"
    image = (
        obj.get("image")
        or obj.get("imageUrl")
        or obj.get("thumbnail")
        or obj.get("thumbnailUrl")
        or obj.get("imgUrl")
        or ""
    )
    return {
        "id": str(item_id or abs(hash(str(obj)))) ,
        "title": str(title or "").strip(),
        "brand": str(obj.get("brand") or obj.get("brandName") or "").strip(),
        "category": str(obj.get("category") or obj.get("categoryName") or "").strip(),
        "price": price,
        "created_at": _parse_ts(obj.get("createdAt") or obj.get("created_at") or obj.get("updatedAt") or obj.get("regDate")),
        "image": urljoin(GOFISH_HOME_URL, image) if image else "",
        "url": urljoin(GOFISH_HOME_URL, url) if url else GOFISH_HOME_URL,
        "status": str(obj.get("status") or "selling"),
    }


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
    text = _text_blob(item)
    # Не режем слишком жёстко: если бренд совпал, одежду/обувь пропускаем,
    # но obvious non-fashion выше уже отфильтрованы.
    if not _has_any_term(text, GOFISH_CATEGORY_WORDS):
        return True
    return True


def gofish_fashion_kind(item):
    text = _text_blob(item)
    groups = [
        ("shoes", ["sneaker", "shoe", "boots", "loafer", "sandals", "스니커즈", "신발", "부츠", "로퍼", "샌들"]),
        ("bag", ["bag", "backpack", "wallet", "tote", "pouch", "가방", "백팩", "지갑"]),
        ("tops", ["shirt", "tee", "hoodie", "sweatshirt", "sweater", "knit", "cardigan", "top", "셔츠", "티셔츠", "후드", "맨투맨", "니트", "가디건"]),
        ("outerwear", ["jacket", "coat", "vest", "parka", "down", "자켓", "재킷", "코트", "패딩"]),
        ("bottoms", ["pants", "jeans", "denim", "trousers", "shorts", "skirt", "cargo", "팬츠", "바지", "데님", "스커트"]),
        ("hat", ["hat", "cap", "beanie", "모자", "캡", "비니"]),
        ("accessory", ["belt", "scarf", "gloves", "벨트", "머플러"]),
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


def fetch_gofish(query, price_min=None, price_max=None):
    items = []
    session = requests.Session()
    if PROXY_URL:
        session.proxies = {"http": PROXY_URL, "https": PROXY_URL}

    search_urls = [
        f"{GOFISH_HOME_URL}/search?keyword={quote(query)}",
        f"{GOFISH_HOME_URL}/search?q={quote(query)}",
    ]

    for url in search_urls:
        try:
            response = session.get(url, headers=_headers(query), timeout=(4, GOFISH_TIMEOUT))
            if response.status_code != 200:
                log.info("Gofish search %s -> HTTP %s", url, response.status_code)
                continue

            html_text = response.text
            for obj in _extract_json_objects(html_text):
                normalized = _normalize_json_item(obj)
                if normalized["title"] and normalized["price"]:
                    items.append(normalized)

            items.extend(_extract_cards_from_html(html_text))

            if items:
                break
        except requests.exceptions.ReadTimeout:
            log.warning("fetch_gofish '%s': timeout %ss url=%s", query, GOFISH_TIMEOUT, url)
            time.sleep(GOFISH_SLEEP_ON_TIMEOUT)
            continue
        except requests.exceptions.ConnectTimeout:
            log.warning("fetch_gofish '%s': connect timeout url=%s", query, url)
            time.sleep(GOFISH_SLEEP_ON_TIMEOUT)
            continue
        except Exception as e:
            log.warning("fetch_gofish '%s': %s", query, e)

    items = _dedupe_items(items)
    min_price = int(state["gofish_min"] if price_min is None else price_min)
    max_price = int(state["gofish_max"] if price_max is None else price_max)
    items = [item for item in items if min_price <= int(item.get("price") or 0) <= max_price]

    if items:
        log.info("Gofish '%s' -> %s товаров", query, len(items))
    else:
        log.info("Gofish '%s' -> 0 товаров", query)
    return items


def format_gofish_message(item, title_ru, price_line):
    link_safe = html.escape(str(item.get("url") or GOFISH_HOME_URL), quote=True)
    title_safe = html.escape(str(title_ru or item.get("title") or "?"))
    meta = []
    if item.get("brand"):
        meta.append(f"<b>Бренд:</b> {html.escape(str(item['brand']))}")
    if item.get("category"):
        meta.append(f"<b>Категория:</b> {html.escape(str(item['category']))}")
    meta_text = ("\n".join(meta) + "\n") if meta else ""

    return (
        "<b>Gofish KR</b>\n"
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
                log.warning("Gofish send_photo failed for chat %s: %s", chat_id, e)
        try:
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as e:
            log.warning("Gofish send_message failed for chat %s: %s", chat_id, e)


def gofish_loop(bot_app):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    log.info("Gofish мониторинг запущен")

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

                # Рыночная выборка без пользовательского ограничения цены.
                for found in fetch_gofish(query, price_min=1, price_max=GOFISH_MARKET_PRICE_MAX):
                    if found.get("id"):
                        market_items_by_id[found["id"]] = found

                items = list(items_by_id.values())
                if not items:
                    log.info("SKIP Gofish no items for query: %s", query)
                    continue
                market_items = list(market_items_by_id.values()) or items

                for item in items:
                    iid = item.get("id")
                    if not iid or iid in state["gofish_seen"]:
                        continue

                    if not is_relevant_gofish_item(item, brand):
                        log.info("SKIP Gofish filter: %s", item.get("title", "?")[:60])
                        continue
                    if keyword and not gofish_matches_keyword(item, keyword):
                        log.info("SKIP Gofish keyword '%s': %s", keyword, item.get("title", "?")[:60])
                        continue

                    age_ok = age_in_range(
                        item.get("created_at"),
                        state["gofish_min_age_hours"],
                        state["gofish_max_age_hours"],
                    )
                    if age_ok is False:
                        age_hours = publish_age_hours(item.get("created_at"))
                        age_label = f"{age_hours:.1f}h" if age_hours is not None else "unknown"
                        log.info("SKIP Gofish age %s: %s", age_label, item.get("title", "?")[:60])
                        continue

                    market = gofish_market_price_krw(market_items, item, brand, keyword)
                    if not market:
                        log.info("SKIP Gofish no market sample: %s", item.get("title", "?")[:60])
                        continue

                    market_krw = int(market["price"])
                    market_count = int(market["count"])
                    price = int(item["price"])
                    if price > market_krw * GOFISH_MAX_MARKET_RATIO:
                        log.info("SKIP Gofish not under market %s/%s: %s", price, market_krw, item.get("title", "?")[:60])
                        continue

                    discount = max(0, round((1 - price / market_krw) * 100))
                    rate = get_fx_rate("KRW", "EUR", fallback=0.00067)
                    eur = price * rate
                    market_eur = market_krw * rate
                    price_line = (
                        f"₩{price:,} (~{eur:.0f} EUR)\n"
                        f"<b>Рынок:</b> ~₩{market_krw:,} (~{market_eur:.0f} EUR), "
                        f"ниже на {discount}% · {market_count} сравн."
                    )

                    title_ru = translate_to_ru(item.get("title", ""))
                    msg = format_gofish_message(item, title_ru, price_line)
                    state["gofish_seen"].add(iid)
                    state["gofish_stats"]["found"] += 1
                    log.info("FOUND Gofish: %s — ₩%s", item["title"], item["price"])
                    loop.run_until_complete(_send_gofish_item(bot_app, item.get("image"), msg))

                time.sleep(random.uniform(8, 15))

        if state["gofish_running"]:
            time.sleep(state["gofish_interval"])

    loop.close()
