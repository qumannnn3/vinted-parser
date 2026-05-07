import asyncio
import gzip
import html
import json as _json
import random
import time

import requests

from shared import (
    BAD_WORDS,
    CATALOG_IDS,
    DEEP_FASHION_BLOCKED_WORDS,
    PROXY_URL,
    USER_AGENTS,
    VINTED_REGIONS,
    age_in_range,
    format_msk_timestamp,
    keyword_matches_text,
    log,
    market_search_queries,
    notification_chat_ids,
    publish_age_hours,
    state,
    translate_to_ru,
    vinted_price_bounds,
    vinted_price_to_eur,
    _has_any_term,
    _try_parse_ts,
)

vinted_sessions: dict[str, requests.Session] = {}


def make_vinted_session(domain):
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": f"https://{domain}/",
        "Origin": f"https://{domain}",
    })
    if PROXY_URL:
        session.proxies = {"http": PROXY_URL, "https": PROXY_URL}
    return session


def init_vinted(domain):
    session = make_vinted_session(domain)
    try:
        session.get(f"https://{domain}/", timeout=15)
        session.get(f"https://{domain}/catalog", timeout=15)
    except Exception as e:
        log.warning("init_vinted %s: %s", domain, e)
    vinted_sessions[domain] = session
    return session


def get_vinted_session(domain):
    return vinted_sessions.get(domain) or init_vinted(domain)


def decode_response(response):
    enc = response.headers.get("content-encoding", "").lower()
    content = response.content
    try:
        if enc == "br":
            try:
                import brotli
                content = brotli.decompress(content)
            except ImportError:
                pass
        elif enc == "gzip":
            content = gzip.decompress(content)
        return _json.loads(content)
    except Exception:
        try:
            return response.json()
        except Exception:
            return {}


def fetch_vinted(query, domain, retry=True):
    session = get_vinted_session(domain)
    session.headers["User-Agent"] = random.choice(USER_AGENTS)
    try:
        price_from, price_to, currency = vinted_price_bounds(domain)
        params = [
            ("search_text", query),
            ("page", 1),
            ("per_page", 48),
            ("order", "newest_first"),
            ("price_from", f"{price_from:.2f}"),
            ("price_to", f"{price_to:.2f}"),
            ("currency", currency),
        ]
        for cid in CATALOG_IDS:
            params.append(("catalog_ids[]", cid))

        response = session.get(f"https://{domain}/api/v2/catalog/items", params=params, timeout=20)
        if response.status_code == 200:
            items = decode_response(response).get("items", [])
            if items:
                log.info("Vinted %s -> %s товаров", domain, len(items))
                if not state["_vinted_debug_done"]:
                    item0 = items[0]
                    log.info("DEBUG keys: %s", list(item0.keys()))
                    for key in (
                        "created_at_ts", "updated_at_ts", "created_at", "updated_at",
                        "active_at", "last_push_up_at", "activation_ts",
                    ):
                        if key in item0:
                            log.info("DEBUG %s = %r", key, item0[key])
                    state["_vinted_debug_done"] = True
            return items
        if response.status_code == 401 and retry:
            log.warning("Vinted session expired %s, обновляю cookies", domain)
            vinted_sessions.pop(domain, None)
            init_vinted(domain)
            return fetch_vinted(query, domain, retry=False)
        if response.status_code in (403, 429):
            log.error("Vinted BAN %s %s", response.status_code, domain)
            vinted_sessions.pop(domain, None)
            return "BAN"
        return []
    except Exception as e:
        log.warning("fetch_vinted %s: %s", domain, e)
        return []


def _get_nested(data, path):
    cur = data
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list) and part.isdigit():
            idx = int(part)
            if idx >= len(cur):
                return None
            cur = cur[idx]
        else:
            return None
    return cur


def parse_vinted_ts(item) -> float | None:
    cached = state.get("_vinted_ts_field")
    if cached:
        ts = _try_parse_ts(_get_nested(item, cached))
        if ts:
            return ts
    candidates = [
        "created_at_ts", "updated_at_ts", "activation_ts",
        "created_at", "updated_at", "active_at", "last_push_up_at",
        "photo.high_resolution.timestamp", "photo.timestamp",
        "photos.0.high_resolution.timestamp", "photos.0.timestamp",
    ]
    for key in candidates:
        val = _get_nested(item, key)
        if val is None:
            continue
        ts = _try_parse_ts(val)
        if ts:
            if state.get("_vinted_ts_field") != key:
                state["_vinted_ts_field"] = key
                log.info("Поле времени Vinted: '%s' = %r", key, val)
            return ts
    return None


def _vinted_text_blob(item):
    parts = []
    for key in ("title", "brand_title", "size_title", "status", "description"):
        val = item.get(key) if isinstance(item, dict) else ""
        if val:
            parts.append(str(val))
    for path in ("item_box.accessibility_label", "photo.accessibility_label", "catalog_title"):
        val = _get_nested(item, path)
        if val:
            parts.append(str(val))
    return " ".join(parts).lower()


def is_deep_fashion_vinted_item(item):
    text = _vinted_text_blob(item)
    if _has_any_term(text, DEEP_FASHION_BLOCKED_WORDS):
        return False
    if _has_any_term(text, BAD_WORDS):
        return False
    return True


def vinted_matches_keyword(item, keyword):
    return keyword_matches_text(_vinted_text_blob(item), keyword)


def is_relevant(item, brand):
    title = item.get("title", "").lower()
    brand_title = item.get("brand_title", "").lower()
    word = brand.split()[0]
    if not (word in title or word in brand_title):
        return False
    if not is_deep_fashion_vinted_item(item):
        log.info("SKIP Vinted deep fashion filter: %s", item.get("title", "?")[:40])
        return False
    ts = parse_vinted_ts(item)
    if ts is None:
        log.info("SKIP Vinted no publish time id=%s '%s'", item.get("id"), item.get("title", "?")[:40])
        return False
    age_ok = age_in_range(ts, state["vinted_min_age_hours"], state["vinted_max_age_hours"])
    age_hours = publish_age_hours(ts)
    if age_ok is False:
        log.info("SKIP Vinted age %.1fh: %s", age_hours, item.get("title", "?")[:40])
        return False
    return True


def format_vinted_message(item, domain, title, title_ru, price, curr, link, ts_d, brand_title, size, condition):
    country = domain.rsplit(".", 1)[-1].upper()
    seller = item.get("user", {}) or {}
    seller_name = html.escape(str(seller.get("login") or seller.get("username") or "не указан"))
    posted = format_msk_timestamp(ts_d)
    details = [str(x) for x in (brand_title, size, condition) if x]
    details_line = html.escape(" / ".join(details))
    title_safe = html.escape(str(title_ru or title))
    link_safe = html.escape(str(link), quote=True)
    price_line = f"{price:g} {html.escape(str(curr))}"
    try:
        price_eur = vinted_price_to_eur(price, curr)
        if str(curr).upper() != "EUR":
            price_line += f" (~{price_eur:.2f} EUR)"
    except Exception:
        pass
    meta = f"{details_line}\n\n" if details_line else ""
    return (
        f"<b>Vinted {country}</b>\n"
        f"<b>{title_safe}</b>\n"
        f"{meta}"
        f"<b>Цена:</b> {price_line}\n"
        f"<b>Публикация:</b> {posted}\n"
        f"<b>Продавец:</b> {seller_name}\n\n"
        f"<a href='{link_safe}'>Открыть объявление</a>"
    )


async def _send_vinted_item(bot_app, photo_url, msg):
    chat_ids = notification_chat_ids()
    if not chat_ids or not bot_app:
        return
    for chat_id in chat_ids:
        if photo_url:
            try:
                await bot_app.bot.send_photo(chat_id=chat_id, photo=photo_url, caption=msg, parse_mode="HTML")
                continue
            except Exception as e:
                log.warning("Vinted send_photo failed for chat %s: %s", chat_id, e)
        try:
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as e:
            log.warning("Vinted send_message failed for chat %s: %s", chat_id, e)


def vinted_loop(bot_app):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for domain in VINTED_REGIONS.values():
        init_vinted(domain)
        time.sleep(2)
    log.info("Vinted мониторинг запущен")

    while state["vinted_running"]:
        brands = list(state["active_brands"])
        random.shuffle(brands)
        state["vinted_stats"]["cycles"] += 1

        for brand in brands:
            if not state["vinted_running"]:
                break
            for query, keyword in market_search_queries(brand, "vinted"):
                if not state["vinted_running"]:
                    break
                for _, domain in VINTED_REGIONS.items():
                    if not state["vinted_running"]:
                        break
                    items = fetch_vinted(query, domain)
                    if items == "BAN":
                        time.sleep(random.randint(60, 120))
                        continue
                    for item in items or []:
                        iid = item.get("id")
                        if iid in state["vinted_seen"]:
                            continue
                        if not is_relevant(item, brand):
                            continue
                        if keyword and not vinted_matches_keyword(item, keyword):
                            log.info("SKIP Vinted keyword '%s': %s", keyword, item.get("title", "?")[:40])
                            continue
                        try:
                            price = float(item.get("price", {}).get("amount", 0))
                        except (ValueError, TypeError):
                            continue

                        price_data = item.get("price", {})
                        curr = price_data.get("currency_code", "EUR")
                        price_eur = vinted_price_to_eur(price, curr)
                        if not (state["vinted_min"] <= price_eur <= state["vinted_max"]):
                            continue

                        title = item.get("title", "?")
                        size = item.get("size_title", "")
                        brand_title = item.get("brand_title", "")
                        condition = item.get("status", "")
                        url = item.get("url", "")
                        link = f"https://{domain}{url}" if url.startswith("/") else url
                        title_ru = translate_to_ru(title)
                        ts_d = parse_vinted_ts(item)

                        photos = item.get("photos") or item.get("photo") or []
                        if isinstance(photos, dict):
                            photos = [photos]
                        photo_url = ""
                        if photos:
                            photo = photos[0]
                            photo_url = photo.get("full_size_url") or photo.get("url") or photo.get("thumb_url", "")

                        msg = format_vinted_message(
                            item, domain, title, title_ru, price, curr, link, ts_d, brand_title, size, condition
                        )
                        state["vinted_seen"].add(iid)
                        state["vinted_stats"]["found"] += 1
                        log.info("FOUND Vinted: %s — %s", title, price)
                        loop.run_until_complete(_send_vinted_item(bot_app, photo_url, msg))
                    time.sleep(random.uniform(10, 18))
            time.sleep(random.uniform(12, 25))

        if state["vinted_running"]:
            time.sleep(state["vinted_interval"])
    loop.close()
