#!/usr/bin/env python3
from __future__ import annotations

import html
import logging
import os
import queue
import random
import re
import threading
import time
import unicodedata
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
PROXY_URL = os.environ.get("PROXY_URL", "").strip()

VINTED_REGIONS = {
    "pl": "www.vinted.pl",
    "lt": "www.vinted.lt",
    "lv": "www.vinted.lv",
}
DOMAIN_CURRENCY = {
    "www.vinted.pl": "PLN",
    "www.vinted.lt": "EUR",
    "www.vinted.lv": "EUR",
}

DEFAULT_CATALOG_IDS = "1,3,5,7,9,12"
MAX_SEEN_ITEMS = 7000
MSK_TZ = ZoneInfo("Europe/Moscow")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

ALL_BRANDS = [
    "stone island",
    "balenciaga",
    "raf simons",
    "bape",
    "aape",
    "gucci",
    "chanel",
    "jeremy scott",
    "undercover",
    "comme des garcons",
    "yohji yamamoto",
    "vetements",
    "palm angels",
    "maison margiela",
    "givenchy",
    "burberry",
    "supreme",
    "amiri",
    "acne studios",
    "alyx",
]

BRAND_ALIASES = {
    "stone island": ["stone island", "stoneisland", "stoney", "stone isl"],
    "comme des garcons": [
        "comme des garcons",
        "comme des garçons",
        "comme des garcon",
        "cdg",
        "play cdg",
    ],
    "raf simons": ["raf simons", "rafsimons"],
    "balenciaga": ["balenciaga", "balen"],
    "maison margiela": ["maison margiela", "margiela", "mm6"],
    "yohji yamamoto": ["yohji yamamoto", "yohji"],
    "acne studios": ["acne studios", "acne"],
}

BAD_WORDS = [
    "pieluchy",
    "pampers",
    "dziecko",
    "dla dzieci",
    "podгузники",
    "детское",
    "nosidelko",
    "nosidełko",
    "fotelik",
    "wozek",
    "wózek",
    "kocyk",
    "smoczek",
    "lozeczko",
    "łóżeczko",
    "underwear",
    "socks",
    "bielizna",
    "majtki",
    "skarpety",
    "rajstopy",
    "biustonosz",
    "bokserki",
    "stringi",
    "figi",
    "kask",
    "rower",
    "hulajnoga",
    "rolki",
    "narty",
    "deska",
    "telefon",
    "laptop",
    "tablet",
    "konsola",
    "perfumy",
    "perfume",
    "fragrance",
    "krem",
    "szampon",
    "ksiazka",
    "książka",
    "zabawka",
    "puzzle",
    "klocki",
    "posciel",
    "pościel",
    "poduszka",
    "koldra",
    "kołdra",
    "recznik",
    "ręcznik",
    "zaslona",
    "zasłona",
]

FAKE_WORDS = [
    "replica",
    "replika",
    "fake",
    "podrobka",
    "podróbka",
    "kopie",
    "kopia",
    "копия",
    "реплика",
    "1:1",
    "copy",
    "inspired",
    "bootleg",
]

BLACKLIST_SELLERS = {
    "luxury_outlet_fake",
    "replica_store",
}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)).replace(",", "."))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.environ.get(name, str(default)).replace(",", ".")))
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int_list(name: str, default: str) -> list[int]:
    raw = os.environ.get(name, default).strip()
    if not raw:
        return []
    values: list[int] = []
    for part in raw.split(","):
        try:
            values.append(int(part.strip()))
        except ValueError:
            continue
    return values


CATALOG_IDS = _env_int_list("VINTED_CATALOG_IDS", DEFAULT_CATALOG_IDS)
REQUEST_DELAY_MIN = max(1.0, _env_float("VINTED_REQUEST_DELAY_MIN", 5.0))
REQUEST_DELAY_MAX = max(REQUEST_DELAY_MIN, _env_float("VINTED_REQUEST_DELAY_MAX", 10.0))
SEND_FIRST_CYCLE = _env_bool("VINTED_SEND_FIRST_CYCLE", True)
TRANSLATE_TITLES = _env_bool("TRANSLATE_TITLES", False)

state: dict[str, Any] = {
    "chat_id": None,
    "awaiting": None,
    "brands_page": 0,
    "active_brands": set(ALL_BRANDS),
    "running": False,
    "min_eur": _env_float("VINTED_MIN_EUR", 10.0),
    "max_eur": _env_float("VINTED_MAX_EUR", 500.0),
    "max_age_hours": _env_float("VINTED_MAX_AGE_HOURS", 24.0),
    "interval": _env_int("VINTED_INTERVAL", 240),
    "seen": OrderedDict(),
    "stats": {
        "found": 0,
        "cycles": 0,
        "requests": 0,
        "errors": 0,
        "throttled": 0,
        "last_error": "",
        "last_started": None,
        "last_finished": None,
    },
}

state_lock = threading.RLock()
outbox: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=500)
vinted_sessions: dict[str, requests.Session] = {}
fx_cache: dict[tuple[str, str], tuple[float, float]] = {}
translate_cache: OrderedDict[str, str] = OrderedDict()
monitor_thread: threading.Thread | None = None
bot_app: Application | None = None

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger("vinted_bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def normalize_text(text: Any) -> str:
    raw = unicodedata.normalize("NFKD", str(text).lower())
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    raw = raw.replace("-", " ").replace("/", " ").replace("_", " ")
    raw = re.sub(r"[^a-z0-9а-яёąćęłńóśźż ]+", " ", raw, flags=re.IGNORECASE)
    return " ".join(raw.split())


def contains_term(text: str, terms: list[str] | set[str]) -> bool:
    norm = normalize_text(text)
    padded = f" {norm} "
    for term in terms:
        nterm = normalize_text(term)
        if nterm and f" {nterm} " in padded:
            return True
    return False


def brand_match(text: str, brand: str) -> bool:
    aliases = BRAND_ALIASES.get(brand.lower(), [brand])
    return contains_term(text, set(aliases))


def html_safe(value: Any, *, quote: bool = False) -> str:
    return html.escape(str(value or ""), quote=quote)


def format_eur(value: float) -> str:
    return f"{value:g} EUR"


def age_label(hours: float) -> str:
    if hours < 1:
        return f"{int(hours * 60)} мин"
    if hours == int(hours):
        return f"{int(hours)} ч"
    return f"{hours:g} ч"


def now_msk() -> datetime:
    return datetime.now(tz=MSK_TZ)


def format_msk_time(ts: float | None) -> str:
    if not ts:
        return "не определено"
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc).astimezone(MSK_TZ)
        return dt.strftime("%d-%m-%Y %H:%M МСК")
    except (OSError, ValueError, TypeError):
        return "не определено"


def relative_age(ts: float | None) -> str:
    if not ts:
        return ""
    minutes = max(0, int((time.time() - ts) / 60))
    if minutes < 60:
        return f"{minutes} мин назад"
    hours = minutes / 60
    if hours < 48:
        return f"{hours:.1f} ч назад"
    return f"{hours / 24:.1f} дн назад"


def make_vinted_session(domain: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Referer": f"https://{domain}/",
            "Origin": f"https://{domain}",
        }
    )
    if PROXY_URL:
        session.proxies = {"http": PROXY_URL, "https": PROXY_URL}
    return session


def init_vinted(domain: str) -> requests.Session:
    session = make_vinted_session(domain)
    for path in ("/", "/catalog"):
        try:
            response = session.get(f"https://{domain}{path}", timeout=(8, 20))
            if response.status_code in {403, 429}:
                log.warning("Vinted init throttled: %s -> %s", domain, response.status_code)
                break
        except requests.RequestException as exc:
            log.warning("Vinted init failed: %s -> %s", domain, exc)
            break
    vinted_sessions[domain] = session
    return session


def get_vinted_session(domain: str) -> requests.Session:
    return vinted_sessions.get(domain) or init_vinted(domain)


def reset_vinted_session(domain: str) -> requests.Session:
    vinted_sessions.pop(domain, None)
    return init_vinted(domain)


def get_fx_rate(from_currency: str, to_currency: str) -> float:
    source = from_currency.upper()
    target = to_currency.upper()
    if source == target:
        return 1.0

    key = (source, target)
    cached = fx_cache.get(key)
    if cached and time.time() - cached[1] < 6 * 3600:
        return cached[0]

    fallbacks = {
        ("EUR", "PLN"): 4.23,
        ("PLN", "EUR"): 1 / 4.23,
    }

    try:
        response = requests.get(
            "https://api.frankfurter.app/latest",
            params={"from": source, "to": target},
            timeout=10,
        )
        response.raise_for_status()
        rate = float(response.json()["rates"][target])
    except Exception as exc:
        rate = fallbacks.get(key, 1.0)
        log.warning("FX fallback %s->%s=%s (%s)", source, target, rate, exc)

    fx_cache[key] = (rate, time.time())
    return rate


def convert_price(amount: float, from_currency: str, to_currency: str) -> float:
    return float(amount) * get_fx_rate(from_currency, to_currency)


def price_bounds_for_domain(domain: str) -> tuple[str, str, str]:
    currency = DOMAIN_CURRENCY.get(domain, "EUR")
    with state_lock:
        min_eur = float(state["min_eur"])
        max_eur = float(state["max_eur"])

    min_local = convert_price(min_eur, "EUR", currency)
    max_local = convert_price(max_eur, "EUR", currency)
    return f"{min_local:.2f}", f"{max_local:.2f}", currency


def fetch_vinted(brand: str, domain: str, retry: bool = True) -> list[dict[str, Any]] | str:
    session = get_vinted_session(domain)
    price_from, price_to, currency = price_bounds_for_domain(domain)
    params: list[tuple[str, Any]] = [
        ("search_text", brand),
        ("page", 1),
        ("per_page", _env_int("VINTED_PER_PAGE", 48)),
        ("order", "newest_first"),
        ("price_from", price_from),
        ("price_to", price_to),
        ("currency", currency),
    ]
    for catalog_id in CATALOG_IDS:
        params.append(("catalog_ids[]", catalog_id))

    try:
        response = session.get(
            f"https://{domain}/api/v2/catalog/items",
            params=params,
            timeout=(8, 25),
        )
        with state_lock:
            state["stats"]["requests"] += 1

        if response.status_code == 401 and retry:
            log.info("Vinted session expired: %s. Refreshing cookies.", domain)
            reset_vinted_session(domain)
            return fetch_vinted(brand, domain, retry=False)

        if response.status_code in {403, 429}:
            with state_lock:
                state["stats"]["throttled"] += 1
                state["stats"]["last_error"] = f"{domain}: HTTP {response.status_code}"
            vinted_sessions.pop(domain, None)
            return "THROTTLED"

        response.raise_for_status()
        data = response.json()
        items = data.get("items") or []
        log.info("Vinted %s / %s -> %s items", domain, brand, len(items))
        return items
    except requests.RequestException as exc:
        with state_lock:
            state["stats"]["errors"] += 1
            state["stats"]["last_error"] = f"{domain}: {exc}"
        log.warning("fetch_vinted failed: %s / %s -> %s", domain, brand, exc)
        return []
    except ValueError as exc:
        with state_lock:
            state["stats"]["errors"] += 1
            state["stats"]["last_error"] = f"{domain}: bad JSON"
        log.warning("Bad JSON from Vinted: %s / %s -> %s", domain, brand, exc)
        return []


def try_parse_ts(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if 1577836800000 < ts < 1893456000000:
            ts /= 1000
        if 1577836800 < ts < 1893456000:
            return ts
        return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return try_parse_ts(float(raw))
        except ValueError:
            pass
        for candidate in (
            raw.replace(" UTC", "+00:00").replace("Z", "+00:00"),
            raw.replace(" ", "T"),
        ):
            try:
                dt = datetime.fromisoformat(candidate)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.timestamp()
            except ValueError:
                continue
    return None


def get_nested(data: Any, path: str) -> Any:
    current = data
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            index = int(part)
            if index >= len(current):
                return None
            current = current[index]
        else:
            return None
    return current


def parse_vinted_ts(item: dict[str, Any]) -> float | None:
    for path in (
        "created_at_ts",
        "updated_at_ts",
        "activation_ts",
        "created_at",
        "updated_at",
        "active_at",
        "last_push_up_at",
        "photo.high_resolution.timestamp",
        "photo.timestamp",
        "photos.0.high_resolution.timestamp",
        "photos.0.timestamp",
    ):
        ts = try_parse_ts(get_nested(item, path))
        if ts:
            return ts
    return None


def item_text_blob(item: dict[str, Any]) -> str:
    item_box = item.get("item_box") or {}
    return " ".join(
        str(part or "")
        for part in (
            item.get("title"),
            item.get("brand_title"),
            item.get("description"),
            item_box.get("first_line"),
            item_box.get("second_line"),
            item_box.get("accessibility_label"),
        )
    )


def item_price(item: dict[str, Any]) -> tuple[float, str] | None:
    price = item.get("price") or {}
    try:
        amount = float(str(price.get("amount", "")).replace(",", "."))
    except (TypeError, ValueError):
        return None
    currency = str(price.get("currency_code") or "EUR").upper()
    return amount, currency


def item_price_eur(item: dict[str, Any]) -> float | None:
    parsed = item_price(item)
    if not parsed:
        return None
    amount, currency = parsed
    return convert_price(amount, currency, "EUR")


def is_relevant(item: dict[str, Any], brand: str) -> bool:
    blob = item_text_blob(item)
    if not brand_match(blob, brand):
        return False
    if contains_term(blob, BAD_WORDS) or contains_term(blob, FAKE_WORDS):
        return False

    seller = item.get("user") or {}
    seller_name = normalize_text(seller.get("login") or seller.get("username") or "")
    if seller_name in BLACKLIST_SELLERS:
        return False

    price_eur = item_price_eur(item)
    if price_eur is None:
        return False
    with state_lock:
        min_eur = float(state["min_eur"])
        max_eur = float(state["max_eur"])
        max_age_hours = float(state["max_age_hours"])
    if not (min_eur <= price_eur <= max_eur):
        return False

    ts = parse_vinted_ts(item)
    if ts:
        age_hours = (time.time() - ts) / 3600
        if age_hours < -2:
            return False
        if age_hours > max_age_hours:
            return False

    return True


def best_photo_url(item: dict[str, Any]) -> str:
    photos = item.get("photos") or item.get("photo") or []
    if isinstance(photos, dict):
        photos = [photos]
    if not isinstance(photos, list):
        return ""

    for photo in photos:
        if not isinstance(photo, dict):
            continue
        url = (
            photo.get("full_size_url")
            or photo.get("url")
            or photo.get("image", {}).get("url")
            or photo.get("no_watermark_url")
        )
        if url:
            return str(url)
    return ""


def translate_to_ru(text: str) -> str:
    if not TRANSLATE_TITLES or not text.strip():
        return text
    cyrillic = sum(1 for char in text if "\u0400" <= char <= "\u04FF")
    if cyrillic / max(len(text), 1) > 0.4:
        return text

    cached = translate_cache.get(text)
    if cached:
        translate_cache.move_to_end(text)
        return cached

    try:
        response = requests.get(
            "https://translate.googleapis.com/translate_a/single",
            params={"client": "gtx", "sl": "auto", "tl": "ru", "dt": "t", "q": text},
            timeout=8,
        )
        response.raise_for_status()
        data = response.json()
        translated = "".join(part[0] for part in data[0] if part and part[0]).strip() or text
    except Exception:
        translated = text

    translate_cache[text] = translated
    while len(translate_cache) > 300:
        translate_cache.popitem(last=False)
    return translated


def make_item_link(item: dict[str, Any], domain: str) -> str:
    url = str(item.get("url") or item.get("path") or "")
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/"):
        return f"https://{domain}{url}"
    item_id = item.get("id")
    return f"https://{domain}/items/{item_id}" if item_id else f"https://{domain}/catalog"


def format_vinted_message(item: dict[str, Any], domain: str, brand: str) -> str:
    title = str(item.get("title") or "Без названия")
    title_ru = translate_to_ru(title)
    brand_title = str(item.get("brand_title") or brand).strip()
    size = str(item.get("size_title") or "").strip()
    condition = str(item.get("status") or "").strip()
    seller = item.get("user") or {}
    seller_name = seller.get("login") or seller.get("username") or "не указан"
    views = item.get("view_count")
    favourites = item.get("favourite_count")
    price = item_price(item)
    price_eur = item_price_eur(item)
    amount, currency = price if price else (0.0, "EUR")
    ts = parse_vinted_ts(item)
    link = make_item_link(item, domain)
    country = domain.rsplit(".", 1)[-1].upper()

    price_line = f"{amount:g} {html_safe(currency)}"
    if currency != "EUR" and price_eur is not None:
        price_line += f" (~{price_eur:.2f} EUR)"

    details = [part for part in (brand_title, size, condition) if part]
    meta_line = " / ".join(html_safe(part) for part in details) or "не указано"
    age = relative_age(ts)
    posted = format_msk_time(ts)
    if age:
        posted = f"{posted} ({age})"

    counters: list[str] = []
    if views is not None:
        counters.append(f"просмотры: {views}")
    if favourites is not None:
        counters.append(f"избранное: {favourites}")

    return (
        f"<b>Vinted {country}</b>\n\n"
        f"<b>{html_safe(title_ru or title)}</b>\n"
        f"{meta_line}\n\n"
        f"<b>Цена:</b> {price_line}\n"
        f"<b>Публикация:</b> {html_safe(posted)}\n"
        f"<b>Продавец:</b> {html_safe(seller_name)}\n"
        f"{html_safe(' | '.join(counters)) + chr(10) if counters else ''}\n"
        f"<a href='{html_safe(link, quote=True)}'>Открыть объявление</a>"
    )


def remember_seen(key: str) -> bool:
    with state_lock:
        seen: OrderedDict[str, float] = state["seen"]
        if key in seen:
            seen.move_to_end(key)
            return False
        seen[key] = time.time()
        while len(seen) > MAX_SEEN_ITEMS:
            seen.popitem(last=False)
        return True


def enqueue_alert(item: dict[str, Any], domain: str, brand: str) -> None:
    with state_lock:
        chat_id = state["chat_id"]
    if not chat_id:
        return

    alert = {
        "chat_id": chat_id,
        "text": format_vinted_message(item, domain, brand),
        "photo_url": best_photo_url(item),
    }
    try:
        outbox.put_nowait(alert)
    except queue.Full:
        with state_lock:
            state["stats"]["errors"] += 1
            state["stats"]["last_error"] = "Очередь Telegram заполнена"
        log.warning("Telegram outbox is full; dropping alert")


async def flush_outbox(ctx: ContextTypes.DEFAULT_TYPE) -> None:
    for _ in range(10):
        try:
            alert = outbox.get_nowait()
        except queue.Empty:
            return

        chat_id = alert["chat_id"]
        text = alert["text"]
        photo_url = alert.get("photo_url")

        if photo_url:
            try:
                await ctx.bot.send_photo(chat_id=chat_id, photo=photo_url, caption=text, parse_mode="HTML")
                continue
            except Exception as exc:
                log.warning("send_photo failed, falling back to text: %s", exc)

        try:
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=False,
            )
        except Exception as exc:
            with state_lock:
                state["stats"]["errors"] += 1
                state["stats"]["last_error"] = f"Telegram: {exc}"
            log.warning("send_message failed: %s", exc)


def is_running() -> bool:
    with state_lock:
        return bool(state["running"])


def interruptible_sleep(seconds: float) -> None:
    deadline = time.time() + seconds
    while is_running() and time.time() < deadline:
        time.sleep(min(1.0, deadline - time.time()))


def process_vinted_item(item: dict[str, Any], domain: str, brand: str, first_cycle: bool) -> None:
    item_id = item.get("id")
    if not item_id:
        return

    key = f"{domain}:{item_id}"
    if not remember_seen(key):
        return
    if first_cycle and not SEND_FIRST_CYCLE:
        return
    if not is_relevant(item, brand):
        return

    with state_lock:
        state["stats"]["found"] += 1
    log.info("FOUND Vinted: %s / %s / %s", domain, brand, item.get("title"))
    enqueue_alert(item, domain, brand)


def vinted_loop() -> None:
    log.info("Vinted monitoring started")
    for domain in VINTED_REGIONS.values():
        if not is_running():
            return
        init_vinted(domain)
        interruptible_sleep(random.uniform(1.0, 2.5))

    while is_running():
        with state_lock:
            state["stats"]["cycles"] += 1
            cycle = state["stats"]["cycles"]
            state["stats"]["last_started"] = time.time()
            brands = list(state["active_brands"])

        if not brands:
            interruptible_sleep(5)
            continue

        random.shuffle(brands)
        first_cycle = cycle == 1

        for brand in brands:
            if not is_running():
                break
            for domain in VINTED_REGIONS.values():
                if not is_running():
                    break

                items = fetch_vinted(brand, domain)
                if items == "THROTTLED":
                    delay = random.uniform(60, 120)
                    log.warning("Vinted throttled on %s; sleeping %.0fs", domain, delay)
                    interruptible_sleep(delay)
                    continue

                for item in items or []:
                    if isinstance(item, dict):
                        process_vinted_item(item, domain, brand, first_cycle)

                interruptible_sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

        with state_lock:
            state["stats"]["last_finished"] = time.time()
            interval = int(state["interval"])
        interruptible_sleep(max(10, interval))

    log.info("Vinted monitoring stopped")


def start_monitor() -> bool:
    global monitor_thread
    with state_lock:
        if state["running"] and monitor_thread and monitor_thread.is_alive():
            return False
        state["running"] = True
        state["stats"]["last_error"] = ""
        state["stats"]["cycles"] = 0
    monitor_thread = threading.Thread(target=vinted_loop, name="vinted-monitor", daemon=True)
    monitor_thread.start()
    return True


def stop_monitor() -> None:
    with state_lock:
        state["running"] = False


def main_text() -> str:
    with state_lock:
        running = state["running"]
        active = len(state["active_brands"])
        min_eur = state["min_eur"]
        max_eur = state["max_eur"]
        max_age = state["max_age_hours"]
        interval = state["interval"]
        stats = dict(state["stats"])

    status = "работает" if running else "остановлен"
    return (
        "<b>Vinted Parser</b>\n"
        "Мониторинг свежих объявлений по выбранным брендам.\n\n"
        f"<b>Статус:</b> {status}\n"
        f"<b>Бренды:</b> {active}/{len(ALL_BRANDS)}\n"
        f"<b>Цена:</b> {min_eur:g}-{max_eur:g} EUR\n"
        f"<b>Возраст:</b> до {age_label(float(max_age))}\n"
        f"<b>Пауза после цикла:</b> {interval} сек\n\n"
        f"<b>Найдено:</b> {stats['found']} | <b>Циклов:</b> {stats['cycles']} | "
        f"<b>Запросов:</b> {stats['requests']}"
    )


def main_kb() -> InlineKeyboardMarkup:
    with state_lock:
        running = state["running"]
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Остановить" if running else "Запустить", callback_data="toggle")],
            [
                InlineKeyboardButton("Фильтры", callback_data="settings"),
                InlineKeyboardButton("Бренды", callback_data="brands_0"),
            ],
            [InlineKeyboardButton("Статус", callback_data="status")],
        ]
    )


def settings_text() -> str:
    with state_lock:
        min_eur = state["min_eur"]
        max_eur = state["max_eur"]
        max_age = state["max_age_hours"]
        interval = state["interval"]
    catalogs = ", ".join(str(cid) for cid in CATALOG_IDS) if CATALOG_IDS else "без ограничения"
    return (
        "<b>Фильтры Vinted</b>\n\n"
        f"<b>Страны:</b> {', '.join(VINTED_REGIONS.keys())}\n"
        f"<b>Категории:</b> {html_safe(catalogs)}\n"
        f"<b>Цена:</b> {min_eur:g}-{max_eur:g} EUR\n"
        f"<b>Публикация:</b> до {age_label(float(max_age))}\n"
        f"<b>Интервал:</b> {interval} сек\n\n"
        "Для Польши диапазон автоматически конвертируется в PLN перед запросом."
    )


def settings_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Мин. цена", callback_data="set_min"),
                InlineKeyboardButton("Макс. цена", callback_data="set_max"),
            ],
            [
                InlineKeyboardButton("Возраст", callback_data="set_age"),
                InlineKeyboardButton("Интервал", callback_data="set_interval"),
            ],
            [InlineKeyboardButton("Назад", callback_data="main")],
        ]
    )


def brands_kb(page: int = 0) -> InlineKeyboardMarkup:
    per_page = 6
    max_page = max(0, (len(ALL_BRANDS) - 1) // per_page)
    page = max(0, min(page, max_page))
    start = page * per_page

    rows: list[list[InlineKeyboardButton]] = []
    with state_lock:
        active = set(state["active_brands"])

    for brand in ALL_BRANDS[start : start + per_page]:
        mark = "✓" if brand in active else "□"
        rows.append([InlineKeyboardButton(f"{mark} {brand.title()}", callback_data=f"brand_{brand}")])

    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("Назад", callback_data=f"brands_{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page + 1}/{max_page + 1}", callback_data="noop"))
    if page < max_page:
        nav.append(InlineKeyboardButton("Вперед", callback_data=f"brands_{page + 1}"))
    rows.append(nav)
    rows.append(
        [
            InlineKeyboardButton("Все", callback_data="brands_all"),
            InlineKeyboardButton("Снять все", callback_data="brands_none"),
        ]
    )
    rows.append([InlineKeyboardButton("Назад", callback_data="main")])
    return InlineKeyboardMarkup(rows)


def brands_text(page: int = 0) -> str:
    with state_lock:
        active = len(state["active_brands"])
    return f"<b>Бренды</b>\n\nАктивно: {active}/{len(ALL_BRANDS)}\nСтраница: {page + 1}"


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    with state_lock:
        state["chat_id"] = update.effective_chat.id
        state["awaiting"] = None
    await update.message.reply_text(main_text(), reply_markup=main_kb(), parse_mode="HTML")


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    with state_lock:
        state["chat_id"] = update.effective_chat.id
        stats = dict(state["stats"])
        last_error = stats.get("last_error") or "нет"
        queued = outbox.qsize()
    text = (
        f"{main_text()}\n\n"
        f"<b>Ошибки:</b> {stats['errors']} | <b>429/403:</b> {stats['throttled']}\n"
        f"<b>Очередь Telegram:</b> {queued}\n"
        f"<b>Последняя ошибка:</b> {html_safe(last_error)}"
    )
    await update.message.reply_text(text, reply_markup=main_kb(), parse_mode="HTML")


async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    stop_monitor()
    await update.message.reply_text("Мониторинг остановлен.", reply_markup=main_kb(), parse_mode="HTML")


async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    with state_lock:
        state["chat_id"] = query.message.chat_id
    data = query.data or ""

    async def edit(text: str, kb: InlineKeyboardMarkup | None = None) -> None:
        try:
            await query.edit_message_text(text, reply_markup=kb or main_kb(), parse_mode="HTML")
        except Exception:
            await query.message.reply_text(text, reply_markup=kb or main_kb(), parse_mode="HTML")

    if data in {"main", "back"}:
        with state_lock:
            state["awaiting"] = None
        await edit(main_text(), main_kb())
        return

    if data == "toggle":
        with state_lock:
            active_empty = not state["active_brands"]
            running = state["running"]
        if running:
            stop_monitor()
        else:
            if active_empty:
                await query.answer("Выбери хотя бы один бренд", show_alert=True)
                return
            start_monitor()
        await edit(main_text(), main_kb())
        return

    if data == "settings":
        await edit(settings_text(), settings_kb())
        return

    if data == "status":
        await edit(await _status_text(), main_kb())
        return

    if data in {"set_min", "set_max", "set_age", "set_interval"}:
        prompts = {
            "set_min": ("min_eur", "Введи минимальную цену в EUR, например: <code>10</code>"),
            "set_max": ("max_eur", "Введи максимальную цену в EUR, например: <code>500</code>"),
            "set_age": ("max_age_hours", "Введи максимальный возраст объявления в часах, например: <code>24</code>"),
            "set_interval": ("interval", "Введи паузу между циклами в секундах, например: <code>240</code>"),
        }
        awaiting, prompt = prompts[data]
        with state_lock:
            state["awaiting"] = awaiting
        await edit(prompt, settings_kb())
        return

    if data.startswith("brands_") and data not in {"brands_all", "brands_none"}:
        try:
            page = int(data.split("_", 1)[1])
        except ValueError:
            page = 0
        with state_lock:
            state["brands_page"] = page
        await edit(brands_text(page), brands_kb(page))
        return

    if data == "brands_all":
        with state_lock:
            state["active_brands"] = set(ALL_BRANDS)
            page = int(state["brands_page"])
        await edit(brands_text(page), brands_kb(page))
        return

    if data == "brands_none":
        with state_lock:
            state["active_brands"] = set()
            page = int(state["brands_page"])
        await edit(brands_text(page), brands_kb(page))
        return

    if data.startswith("brand_"):
        brand = data[6:]
        with state_lock:
            active: set[str] = state["active_brands"]
            if brand in active:
                active.discard(brand)
            elif brand in ALL_BRANDS:
                active.add(brand)
            page = next((idx // 6 for idx, value in enumerate(ALL_BRANDS) if value == brand), 0)
            state["brands_page"] = page
        await edit(brands_text(page), brands_kb(page))
        return

    await query.answer("Неизвестная команда", show_alert=False)


async def _status_text() -> str:
    with state_lock:
        stats = dict(state["stats"])
        last_started = stats.get("last_started")
        last_finished = stats.get("last_finished")
        last_error = stats.get("last_error") or "нет"
        queued = outbox.qsize()
    return (
        f"{main_text()}\n\n"
        f"<b>Последний старт цикла:</b> {format_msk_time(last_started)}\n"
        f"<b>Последнее завершение:</b> {format_msk_time(last_finished)}\n"
        f"<b>Ошибки:</b> {stats['errors']} | <b>429/403:</b> {stats['throttled']}\n"
        f"<b>Очередь Telegram:</b> {queued}\n"
        f"<b>Последняя ошибка:</b> {html_safe(last_error)}"
    )


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip().replace(",", ".")
    with state_lock:
        state["chat_id"] = update.effective_chat.id
        awaiting = state.get("awaiting")

    if not awaiting:
        await update.message.reply_text(main_text(), reply_markup=main_kb(), parse_mode="HTML")
        return

    try:
        value = float(text)
        if value <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Нужно число больше 0.", reply_markup=settings_kb(), parse_mode="HTML")
        return

    with state_lock:
        if awaiting == "interval":
            state[awaiting] = max(30, int(value))
        else:
            state[awaiting] = float(value)
        if float(state["min_eur"]) > float(state["max_eur"]):
            state["min_eur"], state["max_eur"] = state["max_eur"], state["min_eur"]
        state["awaiting"] = None

    await update.message.reply_text(settings_text(), reply_markup=settings_kb(), parse_mode="HTML")


async def setup_bot_commands(app: Application) -> None:
    await app.bot.set_my_commands(
        [
            BotCommand("start", "Главное меню"),
            BotCommand("status", "Статус мониторинга"),
            BotCommand("stop", "Остановить мониторинг"),
        ]
    )


def build_application() -> Application:
    builder = Application.builder().token(BOT_TOKEN)
    if PROXY_URL:
        if hasattr(builder, "proxy_url"):
            builder = builder.proxy_url(PROXY_URL)
        elif hasattr(builder, "proxy"):
            builder = builder.proxy(PROXY_URL)
        if hasattr(builder, "get_updates_proxy_url"):
            builder = builder.get_updates_proxy_url(PROXY_URL)
        elif hasattr(builder, "get_updates_proxy"):
            builder = builder.get_updates_proxy(PROXY_URL)

    return (
        builder.connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(30)
        .post_init(setup_bot_commands)
        .build()
    )


def main() -> None:
    global bot_app
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN не задан. Добавь переменную окружения BOT_TOKEN.")

    log.info(
        "Start Vinted parser | brands=%s | price=%s-%s EUR | age=%s",
        len(ALL_BRANDS),
        state["min_eur"],
        state["max_eur"],
        state["max_age_hours"],
    )

    bot_app = build_application()
    bot_app.add_handler(CommandHandler("start", cmd_start))
    bot_app.add_handler(CommandHandler("status", cmd_status))
    bot_app.add_handler(CommandHandler("stop", cmd_stop))
    bot_app.add_handler(CallbackQueryHandler(on_button))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    bot_app.job_queue.run_repeating(flush_outbox, interval=2, first=2)
    bot_app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True, timeout=30)


if __name__ == "__main__":
    while True:
        try:
            main()
        except SystemExit:
            raise
        except Exception as exc:
            log.exception("Bot crashed: %s. Restart in 15s.", exc)
            stop_monitor()
            time.sleep(15)
