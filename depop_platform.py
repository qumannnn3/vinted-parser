import asyncio
import html
import os
import random
import re
import time
from urllib.parse import quote_plus, urljoin

import requests

from shared import (
    ALL_BRANDS,
    DEEP_FASHION_BLOCKED_WORDS,
    PROXY_URL,
    USER_AGENTS,
    brand_match_terms,
    get_fx_rate,
    keyword_matches_text,
    log,
    market_search_queries,
    notification_chat_ids,
    run_telegram_coroutine,
    state,
    translate_to_ru,
    _has_any_term,
)


DEPOP_HOME_URL = "https://www.depop.com"
DEPOP_API_SEARCH_URL = "https://webapi.depop.com/api/v2/search/products/"
DEPOP_COOKIE_ENV_NAMES = ("DEPOP_COOKIE", "DEPOP_COOKIE_STRING", "DEPOP_COOKIES")
DEPOP_PROXY_ENV_NAMES = ("DEPOP_PROXY_URL", "DEPOP_PROXY", "DEPOP_HTTP_PROXY")


def _first_env(names):
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value, name
    return "", ""


DEPOP_COOKIE, DEPOP_COOKIE_SOURCE = _first_env(DEPOP_COOKIE_ENV_NAMES)
DEPOP_PROXY_URL, DEPOP_PROXY_SOURCE = _first_env(DEPOP_PROXY_ENV_NAMES)
DEPOP_EFFECTIVE_PROXY_URL = DEPOP_PROXY_URL or PROXY_URL
DEPOP_EFFECTIVE_PROXY_SOURCE = DEPOP_PROXY_SOURCE or ("PROXY_URL" if PROXY_URL else "")
DEPOP_403_STOP = os.environ.get("DEPOP_403_STOP", "1").lower() not in ("0", "false", "no")
DEPOP_SOURCE = os.environ.get("DEPOP_SOURCE", "auto").lower().strip()
DEPOP_COUNTRY = os.environ.get("DEPOP_COUNTRY", "us").lower().strip() or "us"
DEPOP_CURRENCY = os.environ.get("DEPOP_CURRENCY", "USD").upper().strip() or "USD"

DEPOP_KIND_WORDS = [
    "shirt", "tee", "t-shirt", "hoodie", "sweatshirt", "sweater", "jumper", "knit",
    "cardigan", "jacket", "coat", "blazer", "vest", "parka", "pants", "jeans",
    "denim", "trousers", "shorts", "skirt", "dress", "sneaker", "sneakers",
    "trainer", "trainers", "shoe", "shoes", "boots", "loafer", "sandals",
    "bag", "backpack", "wallet", "purse", "cap", "hat", "beanie", "belt",
    "scarf", "sunglasses", "accessory",
]

DEPOP_BLOCKED_WORDS = [
    *DEEP_FASHION_BLOCKED_WORDS,
    "watch", "watches", "clock", "perfume", "fragrance", "toy", "figure",
    "book", "magazine", "cd", "dvd", "game", "phone", "iphone", "android",
    "camera", "charger", "case", "poster", "sticker", "keychain", "replica",
    "fake", "inspired", "style", "dupe", "jewelry", "necklace", "ring",
    "earring", "bracelet",
]

_depop_blocked = False


def _slug(text):
    value = str(text or "").strip().lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def _strip_tags(raw):
    text = re.sub(r"<script\b.*?</script>", " ", raw, flags=re.I | re.S)
    text = re.sub(r"<style\b.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    return html.unescape(re.sub(r"\s+", " ", text)).strip()


def _make_session(api=False):
    session = requests.Session()
    if api:
        session.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": DEPOP_HOME_URL,
            "Referer": DEPOP_HOME_URL + "/",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        })
    else:
        session.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": DEPOP_HOME_URL + "/",
            "Upgrade-Insecure-Requests": "1",
        })
    if DEPOP_COOKIE:
        session.headers["Cookie"] = DEPOP_COOKIE
    if DEPOP_EFFECTIVE_PROXY_URL:
        session.proxies = {"http": DEPOP_EFFECTIVE_PROXY_URL, "https": DEPOP_EFFECTIVE_PROXY_URL}
    return session


def _response_block_reason(response):
    if response is None:
        return ""
    cf_mitigated = str(response.headers.get("cf-mitigated", "")).lower()
    server = str(response.headers.get("server", "")).lower()
    text_head = (response.text or "")[:2000].lower()
    if cf_mitigated == "challenge":
        return "Cloudflare challenge"
    if response.status_code in (403, 429) and "cloudflare" in server:
        return "Cloudflare block"
    if "just a moment" in text_head or "cf-browser-verification" in text_head:
        return "Cloudflare challenge"
    if response.status_code == 403:
        return "403 Forbidden"
    if response.status_code == 429:
        return "429 Too Many Requests"
    return ""


def _handle_depop_blocked(query, response=None, url=""):
    global _depop_blocked
    if _depop_blocked:
        return
    _depop_blocked = True
    cookie_status = f"задан ({DEPOP_COOKIE_SOURCE})" if DEPOP_COOKIE else "не задан"
    proxy_status = f"задан ({DEPOP_EFFECTIVE_PROXY_SOURCE})" if DEPOP_EFFECTIVE_PROXY_URL else "не задан"
    status = getattr(response, "status_code", "?")
    reason = _response_block_reason(response) or "доступ заблокирован"
    log.error(
        "Depop остановлен на %r: %s, HTTP %s. Cookie: %s. Proxy: %s. "
        "Depop часто блокирует серверные IP через Cloudflare; задай DEPOP_PROXY_URL с регионом, "
        "где depop.com открывается в обычном браузере, или обнови DEPOP_COOKIE. URL: %s",
        query,
        reason,
        status,
        cookie_status,
        proxy_status,
        url,
    )
    if DEPOP_403_STOP:
        state["depop_running"] = False


def _text_blob(item):
    return " ".join(str(item.get(key) or "") for key in ("title", "brand", "size", "description")).lower()


def depop_matches_brand(item, brand):
    return _has_any_term(_text_blob(item), brand_match_terms(brand))


def depop_matches_keyword(item, keyword):
    return keyword_matches_text(_text_blob(item), keyword)


def is_relevant_depop_item(item, brand):
    text = _text_blob(item)
    if _has_any_term(text, DEPOP_BLOCKED_WORDS):
        return False
    if not depop_matches_brand(item, brand):
        return False
    return _has_any_term(text, DEPOP_KIND_WORDS) or bool(item.get("size"))


def _price_to_eur(price, currency):
    currency = (currency or "EUR").upper()
    if currency in ("£", "GBP"):
        currency = "GBP"
    elif currency in ("$", "USD"):
        currency = "USD"
    elif currency in ("€", "EUR"):
        currency = "EUR"
    return float(price or 0) * get_fx_rate(currency, "EUR")


def _price_display(price, currency):
    symbol = {"GBP": "£", "USD": "$", "EUR": "€"}.get(str(currency).upper(), currency or "€")
    return f"{symbol}{float(price):,.2f}".replace(".00", "")


def _block_for_match(html_text, match):
    start = html_text.rfind("<li", 0, match.start())
    end = html_text.find("</li>", match.end())
    if start >= 0 and end >= 0:
        return html_text[start:end + 5]
    start = html_text.rfind("<a", 0, match.start())
    end = html_text.find("</a>", match.end())
    if start >= 0 and end >= 0:
        return html_text[start:end + 4]
    return html_text[max(0, match.start() - 500):min(len(html_text), match.end() + 1200)]


def _title_from_href(href, brand):
    slug = str(href or "").strip("/").split("/")[-1]
    slug = re.sub(r"^\d+-", "", slug)
    words = [word for word in slug.split("-") if word and not word.isdigit()]
    title = " ".join(words[:12]).strip()
    return title.title() if title else str(brand or "Depop item").title()


def _first_text(*values):
    for value in values:
        if isinstance(value, dict):
            value = value.get("name") or value.get("displayName") or value.get("slug")
        if isinstance(value, list):
            value = " ".join(str(v.get("name") if isinstance(v, dict) else v) for v in value[:3])
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if text:
            return text
    return ""


def _nested_get(data, *path):
    cur = data
    for part in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _walk_dicts(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _extract_price(product):
    price_value = (
        _nested_get(product, "price", "priceAmount")
        or _nested_get(product, "price", "amount")
        or _nested_get(product, "price", "value")
        or product.get("priceAmount")
        or product.get("price")
        or product.get("price_amount")
    )
    if isinstance(price_value, dict):
        price_value = price_value.get("amount") or price_value.get("value") or price_value.get("priceAmount")
    try:
        price = float(str(price_value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None, None
    currency = (
        _nested_get(product, "price", "currencyName")
        or _nested_get(product, "price", "currency")
        or product.get("currencyName")
        or product.get("currency")
        or DEPOP_CURRENCY
    )
    return price, str(currency or DEPOP_CURRENCY).upper()


def _extract_image(product):
    for key in ("pictures", "images", "photos"):
        pictures = product.get(key)
        if isinstance(pictures, list) and pictures:
            first = pictures[0]
            if isinstance(first, dict):
                return first.get("url") or first.get("src") or first.get("original") or first.get("thumbnail") or ""
            return str(first or "")
    image = product.get("image") or product.get("imageUrl") or product.get("preview")
    if isinstance(image, dict):
        return image.get("url") or image.get("src") or ""
    return str(image or "")


def _extract_url(product):
    url = product.get("url") or product.get("href") or product.get("webUrl") or product.get("link")
    if url:
        return urljoin(DEPOP_HOME_URL, str(url))
    slug = product.get("slug") or product.get("productSlug")
    if slug:
        return urljoin(DEPOP_HOME_URL, f"/products/{slug}")
    product_id = product.get("id") or product.get("productId") or product.get("product_id")
    if product_id:
        return urljoin(DEPOP_HOME_URL, f"/products/{product_id}")
    return DEPOP_HOME_URL


def _parse_json_items(data, brand, limit):
    items = []
    seen = set()
    for product in _walk_dicts(data):
        product_id = product.get("id") or product.get("productId") or product.get("product_id") or product.get("slug")
        if not product_id or product_id in seen:
            continue
        title = _first_text(
            product.get("title"),
            product.get("name"),
            product.get("description"),
            product.get("slug"),
        )
        price, currency = _extract_price(product)
        if not title or price is None:
            continue
        price_eur = _price_to_eur(price, currency)
        if not (float(state["depop_min"]) <= price_eur <= float(state["depop_max"])):
            continue
        seen.add(product_id)
        item_brand = _first_text(product.get("brand"), product.get("brandName")) or brand
        size = _first_text(product.get("size"), product.get("sizes"))
        items.append({
            "id": str(product_id),
            "title": title,
            "brand": item_brand,
            "description": _first_text(product.get("description")),
            "price": price,
            "price_eur": price_eur,
            "currency": currency,
            "size": size,
            "url": _extract_url(product),
            "image": _extract_image(product),
        })
        if len(items) >= limit:
            break
    return items


def _parse_html_items(html_text, brand, limit):
    items = []
    seen = set()
    link_pattern = re.compile(r'href=["\'](?P<href>/products/[^"\']+)["\']', re.I)
    for match in link_pattern.finditer(html_text):
        href = html.unescape(match.group("href"))
        item_id = href.strip("/").split("/")[-1]
        if not item_id or item_id in seen:
            continue
        seen.add(item_id)
        block = _block_for_match(html_text, match)
        text = _strip_tags(block)
        price_match = re.search(r"([$£€])\s*([\d,.]+)", text)
        if not price_match:
            continue
        currency_symbol = price_match.group(1)
        try:
            price = float(price_match.group(2).replace(",", ""))
        except ValueError:
            continue
        currency = {"£": "GBP", "$": "USD", "€": "EUR"}.get(currency_symbol, "EUR")
        price_eur = _price_to_eur(price, currency)
        if not (float(state["depop_min"]) <= price_eur <= float(state["depop_max"])):
            continue
        img_match = re.search(r'<img[^>]+(?:src|data-src)=["\']([^"\']+)["\']', block, re.I)
        image = html.unescape(img_match.group(1)) if img_match else ""
        title = _title_from_href(href, brand)
        items.append({
            "id": item_id,
            "title": title,
            "brand": brand,
            "description": text,
            "price": price,
            "price_eur": price_eur,
            "currency": currency,
            "size": "",
            "url": urljoin(DEPOP_HOME_URL, href),
            "image": image,
        })
        if len(items) >= limit:
            break
    return items


def _candidate_api_urls(query, limit):
    base = (
        f"{DEPOP_API_SEARCH_URL}?what={quote_plus(query)}"
        f"&itemsPerPage={int(limit)}&country={quote_plus(DEPOP_COUNTRY)}"
        f"&currency={quote_plus(DEPOP_CURRENCY)}&sort=relevance"
    )
    return [
        base,
        f"{DEPOP_API_SEARCH_URL}?q={quote_plus(query)}&itemsPerPage={int(limit)}",
    ]


def _candidate_web_urls(query, brand):
    urls = []
    brand_slug = _slug(brand)
    query_slug = _slug(query)
    if query_slug and query_slug != brand_slug:
        urls.append(f"{DEPOP_HOME_URL}/search/?q={quote_plus(query)}")
        urls.append(f"{DEPOP_HOME_URL}/theme/{query_slug}/")
    if brand_slug:
        urls.append(f"{DEPOP_HOME_URL}/brands/{brand_slug}/")
    return list(dict.fromkeys(urls))


def _fetch_json(session, url, query):
    response = session.get(url, timeout=25)
    block_reason = _response_block_reason(response)
    if block_reason:
        _handle_depop_blocked(query, response, url)
        return None
    response.raise_for_status()
    content_type = str(response.headers.get("content-type", "")).lower()
    if "json" not in content_type and not response.text.lstrip().startswith(("{", "[")):
        return None
    return response.json()


def _fetch_html(session, url, query):
    response = session.get(url, timeout=25)
    block_reason = _response_block_reason(response)
    if block_reason:
        _handle_depop_blocked(query, response, url)
        return ""
    response.raise_for_status()
    return response.text


def fetch_depop(query, brand, limit=30):
    if _depop_blocked:
        return []

    if DEPOP_SOURCE in ("auto", "api"):
        session = _make_session(api=True)
        for url in _candidate_api_urls(query, limit):
            try:
                data = _fetch_json(session, url, query)
            except Exception as e:
                log.warning("fetch_depop api '%s': %s", query, e)
                continue
            if _depop_blocked:
                return []
            if not data:
                continue
            items = _parse_json_items(data, brand, limit)
            if items:
                log.info("Depop API '%s' -> %s товаров", query, len(items))
                return items

    if DEPOP_SOURCE in ("auto", "html", "web"):
        session = _make_session(api=False)
        for url in _candidate_web_urls(query, brand):
            try:
                html_text = _fetch_html(session, url, query)
            except Exception as e:
                log.warning("fetch_depop html '%s': %s", query, e)
                continue
            if _depop_blocked:
                return []
            items = _parse_html_items(html_text, brand, limit)
            if items:
                log.info("Depop HTML '%s' -> %s товаров", query, len(items))
                return items

    log.info("Depop '%s' -> 0 товаров", query)
    return []


def format_depop_message(item, title_ru, price_line):
    title_safe = html.escape(str(title_ru or item.get("title") or "?"))
    link_safe = html.escape(str(item.get("url") or DEPOP_HOME_URL), quote=True)
    return (
        "<b>Depop</b>\n"
        f"<b>{title_safe}</b>\n\n"
        f"<b>Цена:</b> {price_line}\n\n"
        f"<a href='{link_safe}'>Открыть объявление</a>"
    )


async def _send_depop_item(bot_app, image, msg):
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
                    log.warning("Depop send_photo failed for chat %s: %s", chat_id, e)
            try:
                await bot_app.bot.send_message(
                    chat_id=chat_id,
                    text=msg,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except Exception as e:
                log.warning("Depop send_message failed for chat %s: %s", chat_id, e)

    run_telegram_coroutine(send_all())


def depop_loop(bot_app):
    global _depop_blocked
    _depop_blocked = False
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    cookie_status = f"cookie={DEPOP_COOKIE_SOURCE}" if DEPOP_COOKIE else "cookie=нет"
    proxy_status = f"proxy={DEPOP_EFFECTIVE_PROXY_SOURCE}" if DEPOP_EFFECTIVE_PROXY_URL else "proxy=нет"
    log.info("Depop мониторинг запущен (%s, %s, source=%s)", cookie_status, proxy_status, DEPOP_SOURCE)

    while state["depop_running"]:
        brands = list(state["active_brands"] or ALL_BRANDS)
        random.shuffle(brands)
        state["depop_stats"]["cycles"] += 1

        for brand in brands:
            if not state["depop_running"]:
                break
            for query, keyword in market_search_queries(brand, "depop"):
                if not state["depop_running"]:
                    break
                items = fetch_depop(query, brand)
                if not state["depop_running"]:
                    break
                for item in items:
                    iid = item.get("id")
                    title = item.get("title", "?")
                    if not iid or iid in state["depop_seen"]:
                        continue
                    if keyword and not depop_matches_keyword(item, keyword):
                        log.debug("SKIP Depop keyword '%s': %s", keyword, title[:60])
                        continue
                    if not is_relevant_depop_item(item, brand):
                        log.debug("SKIP Depop filter: %s", title[:60])
                        continue

                    state["depop_seen"].add(iid)
                    if not state.get("depop_bootstrap_done"):
                        log.debug("SKIP Depop initial seen: %s", title[:60])
                        continue

                    price = float(item.get("price") or 0)
                    price_eur = float(item.get("price_eur") or 0)
                    price_line = f"{_price_display(price, item.get('currency'))} (~{price_eur:.0f} EUR)"
                    title_ru = translate_to_ru(title)
                    msg = format_depop_message(item, title_ru, price_line)
                    state["depop_stats"]["found"] += 1
                    log.info("FOUND Depop: %s — %s", title, price_line)
                    loop.run_until_complete(_send_depop_item(bot_app, item.get("image"), msg))

                if state["depop_running"]:
                    time.sleep(random.uniform(8, 15))

        state["depop_bootstrap_done"] = True
        if state["depop_running"]:
            time.sleep(state["depop_interval"])
    loop.close()
