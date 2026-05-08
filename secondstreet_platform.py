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
    get_jpy_to_eur,
    keyword_matches_text,
    log,
    market_search_queries,
    notification_chat_ids,
    run_telegram_coroutine,
    state,
    translate_to_ru,
    _has_any_term,
)


SECONDSTREET_HOME_URL = "https://www.2ndstreet.jp"
SECONDSTREET_SEARCH_URL = f"{SECONDSTREET_HOME_URL}/search"
SECONDSTREET_COOKIE = os.environ.get(
    "SECONDSTREET_COOKIE",
    os.environ.get("SECONDSTREET_COOKIE_STRING", ""),
)
SECONDSTREET_403_STOP = os.environ.get("SECONDSTREET_403_STOP", "1").lower() not in ("0", "false", "no")

SECONDSTREET_KIND_WORDS = [
    "shirt", "tee", "t-shirt", "hoodie", "sweatshirt", "sweater", "knit",
    "cardigan", "jacket", "coat", "blouson", "vest", "parka", "pants",
    "jeans", "denim", "trousers", "shorts", "skirt", "dress", "sneaker",
    "sneakers", "shoe", "shoes", "boots", "loafer", "sandals", "bag",
    "backpack", "wallet", "cap", "hat", "beanie", "belt", "scarf",
    "sunglasses", "accessory",
    "\u30b7\u30e3\u30c4", "\u30d1\u30fc\u30ab\u30fc", "\u30b9\u30a6\u30a7\u30c3\u30c8",
    "\u30cb\u30c3\u30c8", "\u30ab\u30fc\u30c7\u30a3\u30ac\u30f3", "\u30b8\u30e3\u30b1\u30c3\u30c8",
    "\u30d6\u30eb\u30be\u30f3", "\u30b3\u30fc\u30c8", "\u30d1\u30f3\u30c4",
    "\u30c7\u30cb\u30e0", "\u30b8\u30fc\u30f3\u30ba", "\u30b9\u30ab\u30fc\u30c8",
    "\u30ef\u30f3\u30d4\u30fc\u30b9", "\u30b9\u30cb\u30fc\u30ab\u30fc", "\u30b7\u30e5\u30fc\u30ba",
    "\u30d6\u30fc\u30c4", "\u30b5\u30f3\u30c0\u30eb", "\u30d0\u30c3\u30b0",
    "\u8ca1\u5e03", "\u5e3d\u5b50", "\u30ad\u30e3\u30c3\u30d7", "\u30d9\u30eb\u30c8",
]

SECONDSTREET_BLOCKED_WORDS = [
    *DEEP_FASHION_BLOCKED_WORDS,
    "watch", "watches", "clock", "quartz", "analog", "digital",
    "keychain", "key ring", "tie", "necktie",
    "phone", "smartphone", "iphone", "android", "camera", "lens",
    "headphone", "earphone", "speaker", "game", "dvd", "cd",
    "figure", "toy", "hobby", "golf", "fishing",
    "sofa", "chair", "desk", "table", "furniture", "interior",
    "kitchen", "tableware", "plate", "cup", "mug", "glass",
    "kids", "baby", "child",
    "\u8155\u6642\u8a08", "\u6642\u8a08", "\u30af\u30a9\u30fc\u30c4", "\u30a2\u30ca\u30ed\u30b0",
    "\u30ad\u30fc\u30db\u30eb\u30c0\u30fc", "\u30ad\u30fc\u30ea\u30f3\u30b0", "\u30cd\u30af\u30bf\u30a4",
    "\u30b9\u30de\u30db", "\u643a\u5e2f", "\u30ab\u30e1\u30e9", "\u30ec\u30f3\u30ba",
    "\u30d5\u30a3\u30ae\u30e5\u30a2", "\u304a\u3082\u3061\u3083", "\u30db\u30d3\u30fc", "\u30b4\u30eb\u30d5",
    "\u5bb6\u5177", "\u5bb6\u96fb", "\u30ad\u30c3\u30c1\u30f3", "\u98df\u5668",
    "\u30ad\u30c3\u30ba", "\u30d9\u30d3\u30fc", "\u5b50\u4f9b",
]

_secondstreet_blocked = False


def _strip_tags(raw):
    text = re.sub(r"<script\b.*?</script>", " ", raw, flags=re.I | re.S)
    text = re.sub(r"<style\b.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    return html.unescape(re.sub(r"\s+", " ", text)).strip()


def _item_block(html_text, match):
    start = html_text.rfind("<li", 0, match.start())
    end = html_text.find("</li>", match.end())
    if start >= 0 and end >= 0:
        return html_text[start:end + 5]
    start = max(0, match.start() - 300)
    end = min(len(html_text), match.end() + 1200)
    return html_text[start:end]


def _clean_item_title(text, price_start):
    before_price = text[:price_start].strip()
    if "NEW " in before_price:
        before_price = before_price.rsplit("NEW ", 1)[-1]
    before_price = re.sub(r"^(Image|画像)\s*", "", before_price)
    before_price = re.sub(r"^(おすすめ順|新着順|価格が安い順|価格が高い順|割引率が高い順)\s*", "", before_price)
    title = before_price[-220:].strip(" -/|")
    title = re.sub(r"\s*商品の状態\s*:.*$", "", title).strip()
    title_l = title.lower()
    if any(bad in title_l for bad in ("href=", "class=", "itemcard_", "sortby=", "<", ">")):
        return ""
    return title


def _secondstreet_text_blob(item):
    return " ".join(str(item.get(key) or "") for key in ("brand", "title", "condition")).lower()


def secondstreet_matches_brand(item, brand):
    return _has_any_term(_secondstreet_text_blob(item), brand_match_terms(brand))


def secondstreet_matches_keyword(item, keyword):
    return keyword_matches_text(_secondstreet_text_blob(item), keyword)


def _has_blocked_word(item):
    return _has_any_term(_secondstreet_text_blob(item), SECONDSTREET_BLOCKED_WORDS)


def is_relevant_secondstreet_item(item):
    text = _secondstreet_text_blob(item)
    if _has_blocked_word(item):
        return False
    return _has_any_term(text, SECONDSTREET_KIND_WORDS)


def _make_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": SECONDSTREET_HOME_URL + "/",
        "Upgrade-Insecure-Requests": "1",
    })
    if SECONDSTREET_COOKIE:
        session.headers["Cookie"] = SECONDSTREET_COOKIE
    if PROXY_URL:
        session.proxies = {"http": PROXY_URL, "https": PROXY_URL}
    return session


def _handle_secondstreet_403(query):
    global _secondstreet_blocked
    if _secondstreet_blocked:
        return
    _secondstreet_blocked = True
    cookie_status = "задан" if SECONDSTREET_COOKIE else "не задан"
    log.error(
        "2nd Street вернул 403 на %r. Останавливаю 2nd Street, чтобы не спамить лог. "
        "SECONDSTREET_COOKIE: %s. Нужен рабочий cookie из браузера или прокси/регион, который 2ndstreet.jp не блокирует.",
        query,
        cookie_status,
    )
    if SECONDSTREET_403_STOP:
        state["secondstreet_running"] = False


def fetch_secondstreet(query, price_min=None, price_max=None, limit=30):
    if _secondstreet_blocked:
        return []
    params = f"keyword={quote_plus(query)}&sortBy=arrival"
    url = f"{SECONDSTREET_SEARCH_URL}?{params}"
    session = _make_session()
    try:
        response = session.get(SECONDSTREET_HOME_URL, timeout=15)
        if response.status_code == 403:
            _handle_secondstreet_403(query)
            return []
        response = session.get(url, timeout=20)
        if response.status_code == 403:
            _handle_secondstreet_403(query)
            return []
        response.raise_for_status()
    except Exception as e:
        log.warning("fetch_2ndstreet '%s': %s", query, e)
        return []

    html_text = response.text
    items = []
    seen = set()
    pattern = re.compile(
        r'href=["\'](?P<href>/goods/detail/goodsId/(?P<id>\d+)/shopsId/\d+)["\']',
        re.I,
    )
    for match in pattern.finditer(html_text):
        iid = match.group("id")
        if iid in seen:
            continue
        seen.add(iid)

        block = _item_block(html_text, match)
        text = _strip_tags(block)
        price_match = re.search(r"[\u00a5\uffe5]\s*([\d,]+)", text)
        if not price_match:
            continue
        try:
            price = int(price_match.group(1).replace(",", ""))
        except ValueError:
            continue
        min_price = int(state["secondstreet_min"] if price_min is None else price_min)
        max_price = int(state["secondstreet_max"] if price_max is None else price_max)
        if not (min_price <= price <= max_price):
            continue

        title = _clean_item_title(text, price_match.start())
        if not title:
            continue
        brand = title.split(" ", 1)[0].split("/", 1)[0].strip()
        img_match = re.search(r'<img[^>]+(?:src|data-src)=["\']([^"\']+)["\']', block, re.I)
        image = urljoin(SECONDSTREET_HOME_URL, img_match.group(1)) if img_match else ""
        items.append({
            "id": iid,
            "brand": brand,
            "title": title,
            "price": price,
            "url": urljoin(SECONDSTREET_HOME_URL, match.group("href")),
            "image": image,
            "condition": "",
        })
        if len(items) >= limit:
            break

    if items:
        log.info("2nd Street '%s' -> %s товаров", query, len(items))
    else:
        log.info("2nd Street '%s' -> 0 товаров", query)
    return items


def format_secondstreet_message(item, title_ru, price_line):
    title_safe = html.escape(str(title_ru or item.get("title") or "?"))
    link_safe = html.escape(str(item.get("url") or SECONDSTREET_HOME_URL), quote=True)
    return (
        "<b>2nd Street JP</b>\n"
        f"<b>{title_safe}</b>\n\n"
        f"<b>Цена:</b> {price_line}\n\n"
        f"<a href='{link_safe}'>Открыть объявление</a>"
    )


async def _send_secondstreet_item(bot_app, image, msg):
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
                    log.warning("2nd Street send_photo failed for chat %s: %s", chat_id, e)
            try:
                await bot_app.bot.send_message(
                    chat_id=chat_id,
                    text=msg,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except Exception as e:
                log.warning("2nd Street send_message failed for chat %s: %s", chat_id, e)

    run_telegram_coroutine(send_all())


def secondstreet_loop(bot_app):
    global _secondstreet_blocked
    _secondstreet_blocked = False
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    log.info("2nd Street мониторинг запущен")

    while state["secondstreet_running"]:
        brands = list(state["active_brands"] or ALL_BRANDS)
        random.shuffle(brands)
        state["secondstreet_stats"]["cycles"] += 1

        for brand in brands:
            if not state["secondstreet_running"]:
                break
            for query, keyword in market_search_queries(brand, "secondstreet"):
                if not state["secondstreet_running"]:
                    break
                items = fetch_secondstreet(query)
                if not state["secondstreet_running"]:
                    break
                for item in items:
                    iid = item.get("id")
                    title = item.get("title", "?")
                    if not iid:
                        continue
                    if iid in state["secondstreet_seen"]:
                        continue
                    if not secondstreet_matches_brand(item, brand):
                        log.debug("SKIP 2nd Street brand mismatch '%s': %s", brand, title[:60])
                        continue
                    if keyword and not secondstreet_matches_keyword(item, keyword):
                        log.debug("SKIP 2nd Street keyword '%s': %s", keyword, title[:60])
                        continue
                    if not is_relevant_secondstreet_item(item):
                        log.debug("SKIP 2nd Street category: %s", title[:60])
                        continue

                    state["secondstreet_seen"].add(iid)
                    if not state.get("secondstreet_bootstrap_done"):
                        log.debug("SKIP 2nd Street initial seen: %s", title[:60])
                        continue

                    price = int(item.get("price") or 0)
                    rate = get_jpy_to_eur()
                    eur = round(price * rate, 0) if rate else None
                    price_line = f"¥{price:,} (~{eur:.0f} EUR)" if eur else f"¥{price:,}"
                    title_ru = translate_to_ru(title)
                    msg = format_secondstreet_message(item, title_ru, price_line)
                    state["secondstreet_stats"]["found"] += 1
                    log.info("FOUND 2nd Street: %s — ¥%s", title, price)
                    loop.run_until_complete(_send_secondstreet_item(bot_app, item.get("image"), msg))

                if state["secondstreet_running"]:
                    time.sleep(random.uniform(8, 15))

        state["secondstreet_bootstrap_done"] = True
        if state["secondstreet_running"]:
            time.sleep(state["secondstreet_interval"])
    loop.close()
