import asyncio
import hashlib
import html
import json
import os
import random
import re
import time
from http.cookies import SimpleCookie
from urllib.parse import quote, urljoin

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


GOFISH_HOME_URL = "https://www.goofish.com"
GOFISH_MTOP_HOST = "https://h5api.m.goofish.com"
GOFISH_SEARCH_API = "mtop.taobao.idlemtopsearch.pc.search"
GOFISH_APP_KEY = os.environ.get("GOOFISH_APP_KEY", "34839810")
GOOFISH_COOKIE = os.environ.get(
    "GOOFISH_COOKIE",
    os.environ.get(
        "GOFISH_COOKIE",
        os.environ.get("GOOFISH_COOKIE_STRING", os.environ.get("GOFISH_COOKIE_STRING", "")),
    ),
)

GOFISH_MARKET_PRICE_MAX = 10_000_000
GOFISH_MIN_MARKET_SAMPLES = 1
GOFISH_MAX_MARKET_RATIO = 0.90
GOFISH_TIMEOUT = 15
GOFISH_TIMEOUT_LIMIT = 3

_gofish_timeout_streak = 0
_gofish_disabled_until = 0
_gofish_cookie_error_logged = False


GOFISH_BLOCKED_WORDS = [
    "perfume", "fragrance", "향수", "룸스프레이",
    "toy", "figure", "book", "camera", "phone", "watch",
    "피규어", "장난감", "책", "카메라", "핸드폰", "시계",
    "fake", "replica", "copy", "가품", "레플리카",
    "仿", "复刻", "高仿", "假", "山寨",
]

GOFISH_KIND_WORDS = [
    "shirt", "tee", "t-shirt", "hoodie", "sweatshirt", "knit", "cardigan",
    "jacket", "coat", "pants", "jeans", "denim", "trousers", "shorts",
    "skirt", "dress", "sneaker", "shoes", "boots", "loafer", "sandals",
    "bag", "backpack", "wallet", "cap", "hat", "beanie", "belt",
    "衬衫", "短袖", "卫衣", "毛衣", "针织", "开衫", "夹克", "外套",
    "大衣", "裤", "牛仔", "短裤", "裙", "连衣裙", "鞋", "靴",
    "包", "背包", "钱包", "帽", "腰带",
    "셔츠", "티셔츠", "후드", "맨투맨", "니트", "가디건", "자켓", "재킷",
    "코트", "팬츠", "바지", "진", "데님", "스커트", "드레스", "원피스",
    "스니커즈", "신발", "부츠", "로퍼", "샌들", "가방", "백팩", "지갑",
    "캡", "모자", "비니", "벨트",
]


def _make_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,ko;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": GOFISH_HOME_URL,
        "Referer": f"{GOFISH_HOME_URL}/search",
    })
    if PROXY_URL:
        session.proxies = {"http": PROXY_URL, "https": PROXY_URL}
    if GOOFISH_COOKIE:
        session.headers["Cookie"] = GOOFISH_COOKIE
        session.cookies.update(_cookie_dict(GOOFISH_COOKIE))
    return session


def _cookie_dict(cookie_text):
    result = {}
    try:
