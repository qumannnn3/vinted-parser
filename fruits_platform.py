import asyncio
import html
import random
import re
import time

import requests

from shared import (
    ALL_BRANDS,
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
    run_telegram_coroutine,
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


def _text_blob(item):
    return " ".join(
        str(item.get(key) or "")
        for key in ("title", "brand", "category", "size", "condition")
    ).lower()


def _has_blocked_word(item):
    text = _text_blob(item)
    return any(word.lower() in text for word in FRUITS_BLOCKED_WORDS)


def fruits_matches_keyword(item, keyword):
    return keyword_matches_text(_text_blob(item), keyword)

