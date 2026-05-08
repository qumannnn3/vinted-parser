import asyncio
import gzip
import html
import json as _json
import random
import time

import requests

from shared import (
    ALL_BRANDS,
    BAD_WORDS,
    CATALOG_IDS,
    DEEP_FASHION_BLOCKED_WORDS,
    PROXY_URL,
    USER_AGENTS,
    VINTED_REGIONS,
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
    vinted_price_bounds,
    vinted_price_to_eur,
    _has_any_term,
    _try_parse_ts,
)

vinted_sessions: dict[str, requests.Session] = {}

VINTED_MIN_MARKET_SAMPLES = 1
VINTED_MAX_MARKET_RATIO = 0.90
VINTED_MARKET_PRICE_MAX_EUR = 5000


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


def fetch_vinted(query, domain, retry=True, price_min=None, price_max=None):
    session = get_vinted_session(domain)
    session.headers["User-Agent"] = random.choice(USER_AGENTS)
    try:
        price_from, price_to, currency = vinted_price_bounds(domain)
        if price_min is not None:
            price_from = float(price_min)
        if price_max is not None:
            price_to = float(price_max)
