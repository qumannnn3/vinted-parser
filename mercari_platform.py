import asyncio
import html
import inspect
import random
import re
import time
from io import BytesIO

from shared import (
    DEEP_FASHION_BLOCKED_WORDS,
    DEEP_FASHION_SIZE_PATTERN,
    MERCARI_MAX_MARKET_RATIO,
    MERCARI_MIN_MARKET_SAMPLES,
    PROXY_URL,
    USER_AGENTS,
    age_in_range,
    brand_match_terms,
    download_image_bytes,
    format_msk_timestamp,
    get_jpy_to_eur,
    has_brand_disclaimer,
    has_item_seen,
    is_market_run_current,
    is_unwanted_item_text,
    is_wanted_post_text,
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
    _obj_get,
)

mercari_api = None
mercari_details_cache = {}
MERCARI_DETAIL_CACHE_MAX = 1000
MERCARI_OLD_ITEM_STOP_STREAK = 1_000_000_000
MERCARI_EMPTY_BRAND_VALUES = {
    "",
    "-",
    "none",
    "no brand",
    "nobrand",
    "brand unknown",
    "unknown",
    "\u30ce\u30fc\u30d6\u30e9\u30f3\u30c9",
    "\u30d6\u30e9\u30f3\u30c9\u306a\u3057",
    "\u4e0d\u660e",
    "\u6307\u5b9a\u306a\u3057",
    "\u306a\u3057",
    "ノーブランド",
    "ブランドなし",
    "不明",
}


def _normalized_empty_brand_value(value):
    return " ".join(str(value or "").lower().replace("_", " ").split())


def _is_empty_mercari_brand_value(value):
    return _normalized_empty_brand_value(value) in MERCARI_EMPTY_BRAND_VALUES


MERCARI_AMBIGUOUS_TEXT_BRAND_TERMS = {
    "billionaire boys club": {"icecream"},
    "cav empt": {"ce"},
}
MERCARI_EXTRA_TEXT_BRAND_TERMS = {
    "robin jeans": ["robin's jean", "robins jean", "robin jean", "robin"],
}

MERCARI_BLOCKED_WORDS = [
    "watch", "watches", "swatch", "clock", "perfume", "fragrance", "toy",
    "figure", "doll", "book", "magazine", "cd", "dvd", "blu-ray", "game",
    "phone", "iphone", "android", "camera", "charger", "case", "poster",
    "sticker", "card", "keychain", "時計", "腕時計", "置時計", "香水", "おもちゃ",
    "フィギュア", "ぬいぐるみ", "雑誌", "ゲーム", "スマホ", "携帯",
    "カメラ", "充電器", "ケース", "ポスター", "ステッカー", "カード", "キーホルダー",
    "copy", "replica", "fake", "копия", "реплика", "подделка", "偽物", "コピー",
    "模倣", "ノーブランド", "no brand", "brand unknown", "ファックス コピー",
    "style", "inspired", "type", "look", "風", "タイプ", "オマージュ",
    "junk", "damaged", "broken", "hole", "repair", "parts",
    "ジャンク", "穴", "破れ", "訳あり", "難あり",
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

MERCARI_SOFT_CONDITION_WORDS = [
    "stain", "stains", "dirty", "scratch", "scratches",
    "汚れ", "シミ", "傷", "キズ", "スレ",
]

MERCARI_BAD_CONDITION_PATTERNS = [
    r"全体的に状態が悪い",
    r"目立つ(?:傷|キズ|汚れ|シミ)",
    r"(?:大きな|大きい|深い|強い|ひどい|酷い)(?:傷|キズ|汚れ|シミ)",
    r"(?:傷|キズ|汚れ|シミ)(?:多数|多め|多い|あり|有り)",
    r"破損|欠損|剥がれ|ベタつき|ベタ付き",
    r"needs?\s+repair|for\s+parts|parts\s+only|not\s+working",
]

MERCARI_GOOD_CONDITION_PATTERNS = [
    r"目立った(?:傷|キズ)や(?:汚れ|シミ)なし",
    r"目立つ(?:傷|キズ|汚れ|シミ)(?:は)?(?:なし|無し|ありません)",
    r"(?:傷|キズ)や(?:汚れ|シミ)(?:は)?(?:なし|無し|ありません)",
    r"no\s+noticeable\s+(?:stains?|scratches?|damage)",
]

MERCARI_KIND_GROUPS = [
    ("shoes", [
        "sneaker", "sneakers", "shoe", "shoes", "trainer", "trainers", "track trainer",
        "runner", "runners", "detroit runner", "boots", "boot", "loafer", "loafers",
        "sandals", "sandal", "slide", "slides", "derby", "derbies", "adios", "qasa",
        "kaiwa", "terrex", "terek", "zx torsion", "stan smith", "superstar", "wings",
        "\u30b9\u30cb\u30fc\u30ab\u30fc", "\u30b7\u30e5\u30fc\u30ba", "\u9774",
        "\u30b9\u30d4\u30fc\u30c9\u30c8\u30ec\u30fc\u30ca\u30fc", "\u30c8\u30ec\u30fc\u30ca\u30fc",
        "\u30e9\u30f3\u30ca\u30fc", "\u30c7\u30c8\u30ed\u30a4\u30c8\u30e9\u30f3\u30ca\u30fc",
        "\u30cf\u30a4\u30ab\u30c3\u30c8", "\u30ed\u30fc\u30ab\u30c3\u30c8",
        "\u30c8\u30e9\u30c3\u30af\u30c8\u30ec\u30fc\u30ca\u30fc", "\u30c8\u30e9\u30c3\u30af\u30b9\u30cb\u30fc\u30ab\u30fc",
        "\u30c8\u30e9\u30c3\u30af\u30b7\u30e5\u30fc\u30ba",
        "スニーカー", "シューズ", "靴", "ブーツ", "サンダル", "운동화", "스니커즈", "슈즈", "부츠", "샌들",
    ]),
    ("bag", [
        "bag", "bags", "backpack", "wallet", "shoulder bag", "tote", "pouch",
        "バッグ", "リュック", "財布", "ショルダーバッグ", "トート", "ポーチ",
    ]),
    ("tops", [
        "shirt", "t-shirt", "tee", "hoodie", "sweatshirt", "sweat", "sweater",
        "knit", "cardigan", "polo", "top", "blouse", "long sleeve", "ls tee",
        "cutsew", "cut sew", "mock neck", "mockneck", "crew", "crewneck",
        "uniform", "jersey", "soccer", "football shirt", "football jersey",
        "\u30ed\u30f3\u30b0\u30b9\u30ea\u30fc\u30d6", "\u30ed\u30f3t", "\u30ed\u30f3\u30c6\u30a3\u30fc",
        "\u30ab\u30c3\u30c8\u30bd\u30fc", "\u534a\u8896", "\u9577\u8896", "\u30c8\u30ec\u30fc\u30ca\u30fc",
        "\u30d5\u30fc\u30c7\u30a3", "\u30d5\u30fc\u30c9", "\u30dd\u30ed\u30b7\u30e3\u30c4",
        "\u30ad\u30e3\u30df\u30bd\u30fc\u30eb", "\u30bf\u30f3\u30af\u30c8\u30c3\u30d7",
        "\u30d7\u30eb\u30aa\u30fc\u30d0\u30fc", "\u30cf\u30fc\u30d5\u30b8\u30c3\u30d7",
        "\u30dc\u30fc\u30ea\u30f3\u30b0\u30b7\u30e3\u30c4", "\u958b\u895f",
        "シャツ", "tシャツ", "パーカー", "スウェット", "ニット", "カーディガン", "ブラウス", "トップス", "モックネック", "クルー", "ユニフォーム", "ジャージ", "셔츠", "티셔츠", "후드", "후드티", "니트", "가디건", "블라우스", "탑",
    ]),
    ("outerwear", [
        "jacket", "coat", "blouson", "vest", "parka", "down jacket", "windbreaker",
        "track jacket", "tracksuit", "track top",
        "\u30c8\u30e9\u30c3\u30af\u30b8\u30e3\u30b1\u30c3\u30c8", "\u30c8\u30e9\u30c3\u30af\u30c8\u30c3\u30d7",
        "\u30d6\u30eb\u30be\u30f3", "\u30b8\u30e3\u30fc\u30b8",
        "\u30e9\u30a4\u30c0\u30fc\u30b9", "\u30c0\u30a6\u30f3\u30b8\u30e3\u30b1\u30c3\u30c8",
        "\u30c6\u30fc\u30e9\u30fc\u30c9", "\u30b9\u30bf\u30b8\u30e3\u30f3",
        "ジャケット", "コート", "ブルゾン", "ベスト", "ダウン", "アウター", "자켓", "재킷", "코트", "베스트", "패딩", "아우터",
    ]),
    ("bottoms", [
        "pants", "jeans", "denim", "trousers", "shorts", "skirt", "cargo", "slacks",
        "carpenter", "bermuda", "cuffed pants", "track pants", "cargopants",
        "\u30ba\u30dc\u30f3", "\u30dc\u30c8\u30e0", "\u30dc\u30c8\u30e0\u30b9",
        "\u30ab\u30fc\u30b4\u30d1\u30f3\u30c4", "\u30d5\u30ec\u30a2\u30d1\u30f3\u30c4", "\u30ef\u30a4\u30c9\u30d1\u30f3\u30c4",
        "\u30ec\u30ae\u30f3\u30b9", "\u30e1\u30c3\u30b7\u30e5\u30ec\u30ae\u30f3\u30b9",
        "パンツ", "デニム", "ジーンズ", "ショーツ", "スカート", "スラックス", "トラックパンツ", "バミューダ", "カーゴ", "바지", "팬츠", "데님", "청바지", "쇼츠", "스커트", "카고",
    ]),
    ("dress", ["dress", "one piece", "one-piece", "ワンピース", "ドレス"]),
    ("accessory", [
        "cap", "hat", "beanie", "belt", "scarf", "gloves", "sunglasses",
        "帽子", "キャップ", "ハット", "ニット帽", "ベルト", "マフラー", "手袋", "サングラス",
    ]),
]

MERCARI_FASHION_SAFE_AMBIGUOUS_WORDS = [
    "guitar girl", "guitar", "\u30ae\u30bf\u30fc\u30ac\u30fc\u30eb", "\u30ae\u30bf\u30fc",
    "ring hoodie", "ring", "\u30ea\u30f3\u30b0\u30d5\u30fc\u30c7\u30a3", "\u30ea\u30f3\u30b0",
]


def _mercari_text_blob(item):
    parts = []
    for key in ("name", "title", "description", "category", "category_name", "brand", "brand_name", "status"):
        val = item.get(key) if isinstance(item, dict) else _obj_get(item, key, default="")
        if key in ("brand", "brand_name") and _is_empty_mercari_brand_value(val):
            continue
        if isinstance(val, dict):
            parts.extend(str(x) for x in val.values() if x)
        elif isinstance(val, list):
            parts.extend(str(x) for x in val if x)
        elif val:
            parts.append(str(val))
    return " ".join(parts).lower()


def _brand_tokens(brand):
    return [token for token in dict.fromkeys(brand_match_terms(brand)) if token]


def _text_brand_tokens(brand):
    ambiguous = MERCARI_AMBIGUOUS_TEXT_BRAND_TERMS.get(str(brand or "").lower().strip(), set())
    brand_key = str(brand or "").lower().strip()
    tokens = [*_brand_tokens(brand), *MERCARI_EXTRA_TEXT_BRAND_TERMS.get(brand_key, [])]
    return [token for token in dict.fromkeys(tokens) if token.lower().replace(" ", "") not in ambiguous]


def _mercari_brand_text(item):
    raw_brand = ""
    if isinstance(item, dict):
        raw_brand = item.get("brand") or item.get("brand_name") or item.get("brandName") or ""
    else:
        raw_brand = _obj_get(item, "brand", "brand_name", "brandName", default="") or ""
    if isinstance(raw_brand, dict):
        return " ".join(str(value or "") for value in raw_brand.values()).lower()
    if isinstance(raw_brand, list):
        return " ".join(str(value or "") for value in raw_brand).lower()
    if raw_brand and not isinstance(raw_brand, str):
        nested = _obj_get(raw_brand, "name", "brand_name", "brandName", "title", default="")
        return str(nested or "").lower()
    return str(raw_brand or "").lower()


def mercari_matches_brand(item, brand):
    brand_text = _mercari_brand_text(item)
    normalized_brand = " ".join(brand_text.replace("_", " ").split())
    if normalized_brand and not _is_empty_mercari_brand_value(brand_text):
        from shared import text_matches_brand
        if text_matches_brand(brand_text, brand):
            return True

    text = _mercari_text_blob(item)
    from shared import text_matches_brand
    return bool(text and text_matches_brand(text, brand, extra_terms=MERCARI_EXTRA_TEXT_BRAND_TERMS.get(str(brand or "").lower().strip(), []), exclude_compact_terms=MERCARI_AMBIGUOUS_TEXT_BRAND_TERMS.get(str(brand or "").lower().strip(), set())))


def mercari_item_kind(item):
    text = _mercari_text_blob(item)
    for kind, words in MERCARI_KIND_GROUPS:
        if _has_any_term(text, words):
            return kind
    return ""


def _mercari_has_soft_bad_condition(text):
    if not _has_any_term(text, MERCARI_SOFT_CONDITION_WORDS):
        return False
    if any(re.search(pattern, text, re.IGNORECASE) for pattern in MERCARI_GOOD_CONDITION_PATTERNS):
        return False
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in MERCARI_BAD_CONDITION_PATTERNS)


def deep_fashion_kind(item):
    text = _mercari_text_blob(item)
    if is_wanted_post_text(text):
        return ""
    # Do not reject whole branded listings just because the item is a wallet, derby, sandal, etc.
    # Those are real resale items for many brands. Keep only hard non-fashion/bad-condition filters below.
    if _mercari_has_soft_bad_condition(text):
        return ""
    if _has_any_term(text, DEEP_FASHION_BLOCKED_WORDS):
        return ""
    kind = mercari_item_kind(item)
    if kind:
        if (
            _has_any_term(text, MERCARI_BLOCKED_WORDS)
            and not _has_any_term(text, MERCARI_FASHION_SAFE_AMBIGUOUS_WORDS)
        ):
            return ""
        return kind
    if _has_any_term(text, MERCARI_BLOCKED_WORDS):
        return ""
    if DEEP_FASHION_SIZE_PATTERN.search(text):
        return "clothing"
    return ""


def is_relevant_mercari_item(item):
    return bool(deep_fashion_kind(item))


def mercari_matches_keyword(item, keyword):
    return keyword_matches_text(_mercari_text_blob(item), keyword)


def mercari_has_brand_disclaimer(item, brand):
    return has_brand_disclaimer(_mercari_text_blob(item), brand)


def _best_mercari_image_url(url):
    if not url:
        return ""
    url = str(url)
    url = re.sub(r"([?&])(w|width|h|height)=\d+&?", r"\1", url)
    url = url.replace("?=", "?").replace("&&", "&").rstrip("?&")
    url = re.sub(r"/resize:[^/]+/", "/", url)
    url = re.sub(r"_(?:thumb|small|medium)(\.[a-zA-Z0-9]+)$", r"\1", url)
    return url


def _mercari_image_url(item):
    thumbs = item.get("thumbnails") or item.get("item_images") or item.get("images") or []
    if isinstance(thumbs, (dict, str)):
        thumbs = [thumbs]

    candidates = [
        item.get("image"),
        item.get("imageURL"),
        item.get("image_url"),
        item.get("thumbnail"),
    ]
    candidates.extend(thumbs)

    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return _best_mercari_image_url(candidate)
        if isinstance(candidate, dict):
            for key in ("url", "image_url", "imageURL", "src", "thumbnail"):
                value = candidate.get(key)
                if value:
                    return _best_mercari_image_url(value)
    return ""


def mercari_market_price_jpy(items, target_item, brand, keyword=None):
    from market_price import calculate_market_price

    return calculate_market_price(
        items,
        target_item,
        price_getter=lambda item: item.get("price", 0),
        id_getter=lambda item: item.get("id"),
        item_filter=lambda item: (
            mercari_matches_brand(item, brand)
            and not mercari_has_brand_disclaimer(item, brand)
            and (not keyword or mercari_matches_keyword(item, keyword))
            and is_relevant_mercari_item(item)
        ),
        kind_getter=deep_fashion_kind,
        min_samples=MERCARI_MIN_MARKET_SAMPLES,
    )


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
    if not item_id:
        return ""
    item_id = str(item_id)
    # Mercari item IDs in URLs require the "m" prefix (e.g. m12345678901).
    # The API sometimes returns bare numeric IDs — add the prefix if missing.
    if item_id.isdigit():
        item_id = f"m{item_id}"
    return f"https://jp.mercari.com/item/{item_id}"


def _mercari_category_text(category):
    if not category:
        return ""
    parts = []
    for name in ("name", "parent_category_name", "root_category_name"):
        value = _obj_get(category, name, default="")
        if value:
            parts.append(str(value))
    return " ".join(dict.fromkeys(parts))


def _mercari_seller_data(item):
    seller = _obj_get(item, "seller", default=None)
    if not seller:
        return {}
    name = _obj_get(seller, "name", "login", "username", default="")
    seller_id = _obj_get(seller, "id_", "id", "seller_id", "sellerId", default="")
    return {k: v for k, v in {"name": name, "id": seller_id}.items() if v}


def _normalize_mercari_item(item):
    url = _obj_get(item, "productURL", "product_url", "url", "item_url", "webURL", "web_url", default="")
    item_id = _obj_get(
        item,
        "id_", "id", "item_id", "itemId", "item_code", "itemCode", "code",
        "productCode", "product_code", "merItemId", default="",
    ) or _mercari_item_id_from_url(url)
    name = _obj_get(item, "name", "productName", "title", default="?")
    is_no_price = bool(_obj_get(item, "is_no_price", "isNoPrice", default=False))
    real_price = _obj_get(item, "real_price", "realPrice", default=None)
    price = real_price if real_price is not None else _obj_get(item, "price", default=0)
    if is_no_price and real_price is None:
        price = 0
    status = _obj_get(item, "status", "item_status", "itemStatus", default="")
    brand = _obj_get(item, "brand", "brand_name", "brandName", default="")
    item_category = _obj_get(item, "item_category", "itemCategory", default=None)
    category = (
        _mercari_category_text(item_category)
        or _obj_get(item, "category", "category_name", "categoryName", "category_id", "categoryId", default="")
    )
    category_id = (
        _obj_get(item, "category_id", "categoryId", default="")
        or _obj_get(item_category, "id_", "id", default="")
    )
    description = _obj_get(item, "description", "item_description", default="")
    created_at = _obj_get(
        item,
        "created", "created_at", "createdAt", "created_time", "createdTime",
        "created_timestamp", "createdTimestamp", "listed_at", "listedAt",
        default=None,
    )
    thumbnails = _obj_get(item, "thumbnails", "item_images", "images", "photos", "photo_paths", default=[]) or []
    thumb = _mercari_image_url({
        "thumbnails": thumbnails,
        "imageURL": _obj_get(item, "imageURL", "image_url", "thumbnail", default=""),
    })
    url = _mercari_item_url(item_id, url)
    return {
        "id": str(item_id or ""),
        "name": name,
        "price": price,
        "status": status,
        "brand": brand,
        "category": category,
        "category_id": category_id,
        "description": description,
        "created_at": created_at,
        "url": url,
        "thumbnails": [{"url": thumb}] if thumb else [],
        "seller": _mercari_seller_data(item),
        "is_no_price": is_no_price,
        "_raw": item.get("_raw") if isinstance(item, dict) else item,
    }


def _cache_mercari_details(item_id, details):
    if not item_id:
        return
    mercari_details_cache[str(item_id)] = details
    if len(mercari_details_cache) > MERCARI_DETAIL_CACHE_MAX:
        for key in list(mercari_details_cache)[: len(mercari_details_cache) - MERCARI_DETAIL_CACHE_MAX]:
            mercari_details_cache.pop(key, None)


async def enrich_mercari_item(item):
    item_id = str(item.get("id") or "")
    if not item_id or item.get("_details_loaded"):
        return item

    cached = mercari_details_cache.get(item_id)
    if cached:
        item.update(cached)
        item["_details_loaded"] = True
        return item

    raw = item.get("_raw")
    full_item = None
    try:
        if raw is not None and hasattr(raw, "full_item"):
            full_item = await raw.full_item()
        elif mercari_api is not None and hasattr(mercari_api, "item"):
            full_item = await mercari_api.item(item_id)
    except Exception as e:
        log.info("Mercari details skipped id=%s: %s", item_id, e)

    if not full_item:
        item["_details_loaded"] = True
        return item

    details = _normalize_mercari_item(full_item)
    details.pop("_raw", None)
    clean = {}
    for key, value in details.items():
        if value in (None, "", [], {}):
            continue
        clean[key] = value
    item.update(clean)
    item["_details_loaded"] = True
    _cache_mercari_details(item_id, clean)
    return item


async def fetch_mercari(query, price_min=None, price_max=None, limit=30):
    global mercari_api
    try:
        from mercapi import Mercapi
        from mercapi.requests import SearchRequestData

        if mercari_api is None:
            proxies = {"http://": PROXY_URL, "https://": PROXY_URL} if PROXY_URL else None
            mercari_api = Mercapi(proxies=proxies, user_agent=random.choice(USER_AGENTS))

        throttle_request("mercari", 0.8)
        results = await mercari_api.search(
            query,
            sort_by=SearchRequestData.SortBy.SORT_CREATED_TIME,
            sort_order=SearchRequestData.SortOrder.ORDER_DESC,
            status=[SearchRequestData.Status.STATUS_ON_SALE],
            price_min=state["mercari_min"] if price_min is None else price_min,
            price_max=state["mercari_max"] if price_max is None else price_max,
        )
        items = sort_items_newest(
            _normalize_mercari_item(item) for item in getattr(results, "items", [])[:limit]
        )
        if items:
            log.info("Mercari '%s' -> %s товаров", query, len(items))
        return items
    except Exception as e:
        if "Event loop is closed" in str(e) or "different event loop" in str(e):
            mercari_api = None
        log.warning("fetch_mercari '%s': %s", query, e)
        return []


def format_mercari_message(item, name, name_ru, price_str, link):
    seller = item.get("seller") if isinstance(item, dict) else None
    raw_seller = (seller or {}).get("name") or (seller or {}).get("id")
    seller_line = f"<b>Продавец:</b> {html.escape(str(raw_seller))}\n" if raw_seller else ""
    title_safe = html.escape(str(name_ru or name))
    link_safe = html.escape(str(link), quote=True)
    posted = format_msk_timestamp(item.get("created_at")) if isinstance(item, dict) else "не указано"
    return (
        "<b>Mercari JP</b>\n"
        f"<b>{title_safe}</b>\n\n"
        f"<b>Цена:</b> {price_str}\n"
        f"<b>Публикация:</b> {posted}\n"
        f"{seller_line}\n"
        f"<a href='{link_safe}'>Открыть объявление</a>"
    )


async def _send_mercari_item(bot_app, photo_data, msg, run_id):
    if not is_market_run_current("mercari", run_id):
        return
    chat_ids = notification_chat_ids()
    if not chat_ids or not bot_app:
        return

    async def send_all():
        for chat_id in chat_ids:
            if not is_market_run_current("mercari", run_id):
                return
            if photo_data:
                try:
                    photo_file = BytesIO(photo_data)
                    photo_file.name = "mercari.jpg"
                    await bot_app.bot.send_photo(
                        chat_id=chat_id,
                        photo=photo_file,
                        caption=msg,
                        parse_mode="HTML",
                    )
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

    run_telegram_coroutine(send_all())


async def _close_mercari_api():
    global mercari_api
    api = mercari_api
    mercari_api = None
    if api is None:
        return
    closer = getattr(api, "aclose", None) or getattr(api, "close", None)
    if closer is None:
        return
    result = closer()
    if inspect.isawaitable(result):
        await result


def mercari_loop(bot_app):
    global mercari_api
    run_id = state.get("mercari_run_id", 0)
    mercari_api = None
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    log.info("Mercari мониторинг запущен")

    while is_market_run_current("mercari", run_id):
        brands = list(state["active_brands"])
        random.shuffle(brands)
        state["mercari_stats"]["cycles"] += 1

        for brand in brands:
            if not is_market_run_current("mercari", run_id):
                break
            for query, keyword in market_search_queries(brand, "mercari"):
                if not is_market_run_current("mercari", run_id):
                    break
                items = loop.run_until_complete(fetch_mercari(query))
                market_items = None
                for item in items or []:
                    if not is_market_run_current("mercari", run_id):
                        break
                    iid = item.get("id")
                    name = item.get("name", "?")
                    if not iid:
                        log.info("SKIP Mercari no item id: %s", name[:60])
                        continue
                    if has_item_seen("mercari", iid):
                        continue

                    if item.get("is_no_price"):
                        log.info("SKIP Mercari no-price/wanted placeholder: %s", name[:60])
                        continue

                    try:
                        price = int(item.get("price", 0))
                    except (ValueError, TypeError):
                        log.info("SKIP Mercari bad price: %s price=%r", name[:60], item.get("price"))
                        continue
                    if not (state["mercari_min"] <= price <= state["mercari_max"]):
                        log.info("SKIP Mercari price %s: %s", price, name[:60])
                        continue

                    item = loop.run_until_complete(enrich_mercari_item(item))
                    name = item.get("name", name)
                    if item.get("is_no_price"):
                        log.info("SKIP Mercari no-price/wanted placeholder: %s", name[:60])
                        continue
                    try:
                        price = int(item.get("price", price))
                    except (ValueError, TypeError):
                        log.info("SKIP Mercari bad detail price: %s price=%r", name[:60], item.get("price"))
                        continue
                    if not (state["mercari_min"] <= price <= state["mercari_max"]):
                        log.info("SKIP Mercari price %s: %s", price, name[:60])
                        continue
                    if is_wanted_post_text(_mercari_text_blob(item)):
                        log.info("SKIP Mercari wanted/buying post: %s", name[:60])
                        continue

                    if not mercari_matches_brand(item, brand):
                        log.info("SKIP Mercari brand mismatch '%s': %s", brand, name[:60])
                        continue
                    if mercari_has_brand_disclaimer(item, brand):
                        log.info("SKIP Mercari brand/style disclaimer: %s", name[:60])
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
                        continue

                    thumb = _mercari_image_url(item)
                    photo_data = download_image_bytes(thumb, referer="https://jp.mercari.com/") if thumb else None
                    link = item.get("url") or f"https://jp.mercari.com/item/{iid}"
                    if not link or link.rstrip("/").endswith("/item"):
                        log.info("SKIP Mercari bad link id=%r: %s", iid, name[:60])
                        continue

                    name_ru = translate_to_ru(name)
                    rate = get_jpy_to_eur()
                    eur = round(price * rate, 2) if rate else None
                    if market_items is None:
                        market_items = loop.run_until_complete(
                            fetch_mercari(query, price_min=1, price_max=10_000_000, limit=80)
                        ) or items
                    market = mercari_market_price_jpy(market_items, item, brand, keyword)
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
                            f"¥{price:,} (~{eur:.0f} евро)\n"
                            f"<b>Рынок:</b> ~¥{market_jpy:,} (~{market_eur:.0f} евро), "
                            f"ниже на {discount}% · {market_count} сравн."
                        )
                    else:
                        price_str = (
                            f"¥{price:,}\n"
                            f"<b>Рынок:</b> ~¥{market_jpy:,}, ниже на {discount}% · {market_count} сравн."
                        )

                    if not is_market_run_current("mercari", run_id):
                        break
                    msg = format_mercari_message(item, name, name_ru, price_str, link)
                    if not mark_item_seen("mercari", iid):
                        continue
                    state["mercari_stats"]["found"] += 1
                    log.info("FOUND Mercari: %s — ¥%s", name, price)
                    loop.run_until_complete(_send_mercari_item(bot_app, photo_data, msg, run_id))

                sleep_while_market_running("mercari", run_id, random.uniform(8, 15))

        if is_market_run_current("mercari", run_id):
            sleep_while_market_running("mercari", run_id, state["mercari_interval"])
    try:
        loop.run_until_complete(_close_mercari_api())
    except Exception as e:
        log.warning("Mercari client close failed: %s", e)
    loop.close()

                        
