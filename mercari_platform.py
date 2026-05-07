import asyncio
import html
import random
import re
import time

from shared import (
    DEEP_FASHION_BLOCKED_WORDS,
    DEEP_FASHION_SIZE_PATTERN,
    MERCARI_MAX_MARKET_RATIO,
    MERCARI_MIN_MARKET_SAMPLES,
    PROXY_URL,
    USER_AGENTS,
    age_in_range,
    brand_match_terms,
    format_msk_timestamp,
    get_jpy_to_eur,
    keyword_matches_text,
    log,
    market_search_queries,
    notification_chat_ids,
    publish_age_hours,
    state,
    translate_to_ru,
    _has_any_term,
    _obj_get,
)

mercari_api = None

MERCARI_BLOCKED_WORDS = [
    "watch", "watches", "swatch", "clock", "perfume", "fragrance", "toy",
    "figure", "doll", "book", "magazine", "cd", "dvd", "blu-ray", "game",
    "phone", "iphone", "android", "camera", "charger", "case", "poster",
    "sticker", "card", "keychain", "時計", "腕時計", "置時計", "香水", "おもちゃ",
    "フィギュア", "ぬいぐるみ", "本", "雑誌", "ゲーム", "スマホ", "携帯",
    "カメラ", "充電器", "ケース", "ポスター", "ステッカー", "カード", "キーホルダー",
    "copy", "replica", "fake", "копия", "реплика", "подделка", "偽物", "コピー",
    "模倣", "ノーブランド", "no brand", "brand unknown", "ファックス コピー",
    "style", "inspired", "type", "look", "風", "タイプ", "系", "オマージュ",
    "junk", "damaged", "broken", "stain", "dirty", "hole", "repair", "parts",
    "ジャンク", "汚れ", "シミ", "穴", "破れ", "傷", "訳あり", "難あり",
    "drum", "drums", "snare", "cymbal", "guitar", "bass guitar", "piano",
    "keyboard", "trumpet", "sax", "saxophone", "flute", "clarinet", "violin",
    "instrument", "musical instrument", "amplifier", "amp", "microphone",
    "speaker", "mixer", "audio interface", "record", "vinyl", "lp",
    "ドラム", "スネア", "シンバル", "ギター", "ベース", "ピアノ", "キーボード",
    "トランペット", "サックス", "フルート", "クラリネット", "バイオリン",
    "楽器", "音楽", "アンプ", "マイク", "スピーカー", "レコード",
    "барабан", "гитара", "пианино", "синтезатор", "саксофон", "скрипка",
    "музык", "инструмент",
    "valencia", "pearl valencia",
    "necklace", "ring", "earring", "bracelet", "pendant", "jewelry",
    "ネックレス", "リング", "ピアス", "ブレスレット", "ジュエリー",
]

MERCARI_KIND_GROUPS = [
    ("shoes", [
        "sneaker", "sneakers", "shoe", "shoes", "boots", "loafer", "loafers", "sandals",
        "スニーカー", "シューズ", "靴", "ブーツ", "サンダル",
    ]),
    ("bag", [
        "bag", "bags", "backpack", "wallet", "shoulder bag", "tote", "pouch",
        "バッグ", "リュック", "財布", "ショルダーバッグ", "トート", "ポーチ",
    ]),
    ("tops", [
        "shirt", "t-shirt", "tee", "hoodie", "sweatshirt", "sweat", "sweater",
        "knit", "cardigan", "polo", "top", "blouse",
        "シャツ", "tシャツ", "パーカー", "スウェット", "ニット", "カーディガン", "ブラウス", "トップス",
    ]),
    ("outerwear", [
        "jacket", "coat", "blouson", "vest", "parka", "down jacket", "windbreaker",
        "ジャケット", "コート", "ブルゾン", "ベスト", "ダウン", "アウター",
    ]),
    ("bottoms", [
        "pants", "jeans", "denim", "trousers", "shorts", "skirt", "cargo", "slacks",
        "パンツ", "デニム", "ジーンズ", "ショーツ", "スカート", "スラックス",
    ]),
    ("dress", ["dress", "one piece", "one-piece", "ワンピース", "ドレス"]),
    ("accessory", [
        "cap", "hat", "beanie", "belt", "scarf", "gloves", "sunglasses",
        "帽子", "キャップ", "ハット", "ニット帽", "ベルト", "マフラー", "手袋", "サングラス",
    ]),
]


def _mercari_text_blob(item):
    parts = []
    for key in ("name", "title", "description", "category", "category_name", "brand", "brand_name", "status"):
        val = item.get(key) if isinstance(item, dict) else _obj_get(item, key, default="")
        if isinstance(val, dict):
            parts.extend(str(x) for x in val.values() if x)
        elif isinstance(val, list):
            parts.extend(str(x) for x in val if x)
        elif val:
            parts.append(str(val))
    return " ".join(parts).lower()


def _brand_tokens(brand):
    return [token for token in dict.fromkeys(brand_match_terms(brand)) if token]


def mercari_matches_brand(item, brand):
    text = _mercari_text_blob(item)
    return _has_any_term(text, _brand_tokens(brand))


def mercari_item_kind(item):
    text = _mercari_text_blob(item)
    for kind, words in MERCARI_KIND_GROUPS:
        if _has_any_term(text, words):
            return kind
    return ""


def deep_fashion_kind(item):
    text = _mercari_text_blob(item)
    if _has_any_term(text, MERCARI_BLOCKED_WORDS):
        return ""
    if _has_any_term(text, DEEP_FASHION_BLOCKED_WORDS):
        return ""
    kind = mercari_item_kind(item)
    if kind:
        return kind
    if DEEP_FASHION_SIZE_PATTERN.search(text):
        return "clothing"
    return ""


def is_relevant_mercari_item(item):
    return bool(deep_fashion_kind(item))


def mercari_matches_keyword(item, keyword):
    return keyword_matches_text(_mercari_text_blob(item), keyword)


def _best_mercari_image_url(url):
    if not url:
        return ""
    url = str(url)
    url = re.sub(r"([?&])(w|width|h|height)=\d+&?", r"\1", url)
    url = url.replace("?=", "?").replace("&&", "&").rstrip("?&")
    url = re.sub(r"/resize:[^/]+/", "/", url)
    url = re.sub(r"_(?:thumb|small|medium)(\.[a-zA-Z0-9]+)$", r"\1", url)
    return url


def _median(values):
    values = sorted(values)
    if not values:
        return None
    mid = len(values) // 2
    if len(values) % 2:
        return values[mid]
    return int((values[mid - 1] + values[mid]) / 2)


def mercari_market_price_jpy(items, target_item, brand):
    target_kind = deep_fashion_kind(target_item)
    if not target_kind:
        return None
    target_id = target_item.get("id")
    prices = []
    for item in items or []:
        if target_id and item.get("id") == target_id:
            continue
        try:
            price = int(item.get("price", 0))
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue
        if not mercari_matches_brand(item, brand):
            continue
        if deep_fashion_kind(item) != target_kind:
            continue
        prices.append(price)
    if len(prices) < MERCARI_MIN_MARKET_SAMPLES:
        return None
    prices = sorted(prices)
    if len(prices) >= 7:
        cut = max(1, int(len(prices) * 0.1))
        prices = prices[cut:-cut] or prices
    return {"price": _median(prices), "count": len(prices)}


def _mercari_item_id_from_url(url):
    if not url:
        return ""
    m = re.search(r"/item/([^/?#]+)", str(url))
    return m.group(1) if m else ""


def _mercari_item_url(item_id, url):
    if url:
        url = str(url)
        if url.startswith("http://") or url.startswith("https://"):
            return url
        if url.startswith("/"):
            return f"https://jp.mercari.com{url}"
    return f"https://jp.mercari.com/item/{item_id}" if item_id else ""


def _normalize_mercari_item(item):
    url = _obj_get(item, "productURL", "product_url", "url", "item_url", "webURL", "web_url", default="")
    item_id = _obj_get(
        item,
        "id_", "id", "item_id", "itemId", "item_code", "itemCode", "code",
        "productCode", "product_code", "merItemId", default="",
    ) or _mercari_item_id_from_url(url)
    name = _obj_get(item, "name", "productName", "title", default="?")
    price = _obj_get(item, "price", default=0)
    status = _obj_get(item, "status", "item_status", "itemStatus", default="")
    brand = _obj_get(item, "brand", "brand_name", "brandName", default="")
    category = _obj_get(item, "category", "category_name", "categoryName", "category_id", "categoryId", default="")
    description = _obj_get(item, "description", "item_description", default="")
    created_at = _obj_get(
        item,
        "created", "created_at", "createdAt", "created_time", "createdTime",
        "created_timestamp", "createdTimestamp", "listed_at", "listedAt",
        default=None,
    )
    thumbnails = _obj_get(item, "thumbnails", "item_images", "images", default=[]) or []
    thumb = _obj_get(item, "imageURL", "image_url", "thumbnail", default="")
    if not thumb and thumbnails:
        first = thumbnails[0]
        thumb = first if isinstance(first, str) else _obj_get(first, "url", "image_url", "src", default="")
    thumb = _best_mercari_image_url(thumb)
    url = _mercari_item_url(item_id, url)
    return {
        "id": str(item_id or ""),
        "name": name,
        "price": price,
        "status": status,
        "brand": brand,
        "category": category,
        "category_id": _obj_get(item, "category_id", "categoryId", default=""),
        "description": description,
        "created_at": created_at,
        "url": url,
        "thumbnails": [{"url": thumb}] if thumb else [],
    }


async def fetch_mercari(query):
    global mercari_api
    try:
        from mercapi import Mercapi
        from mercapi.requests import SearchRequestData

        if mercari_api is None:
            proxies = {"http://": PROXY_URL, "https://": PROXY_URL} if PROXY_URL else None
            mercari_api = Mercapi(proxies=proxies, user_agent=random.choice(USER_AGENTS))

        results = await mercari_api.search(
            query,
            sort_by=SearchRequestData.SortBy.SORT_CREATED_TIME,
            sort_order=SearchRequestData.SortOrder.ORDER_DESC,
            status=[SearchRequestData.Status.STATUS_ON_SALE],
            price_min=state["mercari_min"],
            price_max=state["mercari_max"],
        )
        items = [_normalize_mercari_item(item) for item in getattr(results, "items", [])[:30]]
        if items:
            log.info("Mercari '%s' -> %s товаров", query, len(items))
        return items
    except Exception as e:
        log.warning("fetch_mercari '%s': %s", query, e)
        return []


def format_mercari_message(item, name, name_ru, price_str, link):
    seller = item.get("seller") if isinstance(item, dict) else None
    seller_name = html.escape(str((seller or {}).get("name") or (seller or {}).get("id") or "не указан"))
    title_safe = html.escape(str(name_ru or name))
    link_safe = html.escape(str(link), quote=True)
    posted = format_msk_timestamp(item.get("created_at")) if isinstance(item, dict) else "не указано"
    return (
        "<b>Mercari JP</b>\n"
        f"<b>{title_safe}</b>\n\n"
        f"<b>Цена:</b> {price_str}\n"
        f"<b>Публикация:</b> {posted}\n"
        f"<b>Продавец:</b> {seller_name}\n\n"
        f"<a href='{link_safe}'>Открыть объявление</a>"
    )


async def _send_mercari_item(bot_app, thumb, msg):
    chat_ids = notification_chat_ids()
    if not chat_ids or not bot_app:
        return
    for chat_id in chat_ids:
        if thumb:
            try:
                await bot_app.bot.send_photo(chat_id=chat_id, photo=thumb, caption=msg, parse_mode="HTML")
                continue
            except Exception as e:
                log.warning("Mercari send_photo failed for chat %s: %s", chat_id, e)
        try:
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as e:
            log.warning("Mercari send_message failed for chat %s: %s", chat_id, e)


def mercari_loop(bot_app):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    log.info("Mercari мониторинг запущен")

    while state["mercari_running"]:
        brands = list(state["active_brands"])
        random.shuffle(brands)
        state["mercari_stats"]["cycles"] += 1

        for brand in brands:
            if not state["mercari_running"]:
                break
            for query, keyword in market_search_queries(brand, "mercari"):
                if not state["mercari_running"]:
                    break
                items = loop.run_until_complete(fetch_mercari(query))
                for item in items or []:
                    iid = item.get("id")
                    name = item.get("name", "?")
                    if not iid:
                        log.info("SKIP Mercari no item id: %s", name[:60])
                        continue
                    if iid in state["mercari_seen"]:
                        continue

                    try:
                        price = int(item.get("price", 0))
                    except (ValueError, TypeError):
                        log.info("SKIP Mercari bad price: %s price=%r", name[:60], item.get("price"))
                        continue
                    if not (state["mercari_min"] <= price <= state["mercari_max"]):
                        log.info("SKIP Mercari price %s: %s", price, name[:60])
                        continue
                    if not mercari_matches_brand(item, brand):
                        log.info("SKIP Mercari brand mismatch '%s': %s", brand, name[:60])
                        continue
                    if keyword and not mercari_matches_keyword(item, keyword):
                        log.info("SKIP Mercari keyword '%s': %s", keyword, name[:60])
                        continue
                    if not is_relevant_mercari_item(item):
                        log.info("SKIP Mercari category: %s", name[:60])
                        continue
                    if not item.get("created_at"):
                        log.info("SKIP Mercari no publish time: %s", name[:60])
                        continue

                    age_ok = age_in_range(
                        item.get("created_at"),
                        state["mercari_min_age_hours"],
                        state["mercari_max_age_hours"],
                    )
                    if age_ok is False:
                        age_hours = publish_age_hours(item.get("created_at"))
                        age_label = f"{age_hours:.1f}h" if age_hours is not None else "unknown"
                        log.info("SKIP Mercari age %s: %s", age_label, name[:60])
                        continue

                    thumbs = item.get("thumbnails") or item.get("item_images") or []
                    thumb = (thumbs[0].get("url") or thumbs[0].get("image_url", "")) if thumbs else ""
                    thumb = _best_mercari_image_url(thumb)
                    link = item.get("url") or f"https://jp.mercari.com/item/{iid}"
                    if not link or link.rstrip("/").endswith("/item"):
                        log.info("SKIP Mercari bad link id=%r: %s", iid, name[:60])
                        continue

                    name_ru = translate_to_ru(name)
                    rate = get_jpy_to_eur()
                    eur = round(price * rate, 2) if rate else None
                    market = mercari_market_price_jpy(items, item, brand)
                    if not market:
                        log.info("SKIP Mercari no market sample: %s", name[:60])
                        continue
                    market_jpy = int(market["price"])
                    market_count = int(market["count"])
                    if price > market_jpy * MERCARI_MAX_MARKET_RATIO:
                        log.info("SKIP Mercari not under market %s/%s: %s", price, market_jpy, name[:60])
                        continue

                    discount = max(0, round((1 - price / market_jpy) * 100))
                    if eur:
                        market_eur = round(market_jpy * rate, 0)
                        price_str = (
                            f"¥{price:,} (~{eur:.0f} EUR)\n"
                            f"<b>Рынок:</b> ~¥{market_jpy:,} (~{market_eur:.0f} EUR), "
                            f"ниже на {discount}% · {market_count} сравн."
                        )
                    else:
                        price_str = (
                            f"¥{price:,}\n"
                            f"<b>Рынок:</b> ~¥{market_jpy:,}, ниже на {discount}% · {market_count} сравн."
                        )

                    msg = format_mercari_message(item, name, name_ru, price_str, link)
                    state["mercari_seen"].add(iid)
                    state["mercari_stats"]["found"] += 1
                    log.info("FOUND Mercari: %s — ¥%s", name, price)
                    loop.run_until_complete(_send_mercari_item(bot_app, thumb, msg))

                time.sleep(random.uniform(8, 15))

        if state["mercari_running"]:
            time.sleep(state["mercari_interval"])
    loop.close()
