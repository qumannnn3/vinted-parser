import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
PROXY_URL = os.environ.get("PROXY_URL", "")

VINTED_REGIONS = {
    "pl": "www.vinted.pl",
    "lt": "www.vinted.lt",
    "lv": "www.vinted.lv",
}
CATALOG_IDS = [1, 3, 5, 9, 7, 12]
MAX_AGE_HOURS = 24

try:
    MERCARI_MIN_MARKET_SAMPLES = max(3, int(os.environ.get("MERCARI_MIN_MARKET_SAMPLES", "5")))
except ValueError:
    MERCARI_MIN_MARKET_SAMPLES = 5

try:
    MERCARI_MAX_MARKET_RATIO = float(os.environ.get("MERCARI_MAX_MARKET_RATIO", "0.95"))
except ValueError:
    MERCARI_MAX_MARKET_RATIO = 0.95

BAD_WORDS = [
    "pieluchy", "pampers", "baby", "dziecko", "dla dzieci", "подгузники", "детское",
    "nosidelko", "fotelik", "wozek", "kocyk", "smoczek", "lozeczko",
    "underwear", "socks", "bielizna", "majtki", "skarpety", "rajstopy",
    "biustonosz", "bokserki", "stringi", "figi",
    "kask", "rower", "hulajnoga", "rolki", "narty", "deska",
    "telefon", "laptop", "tablet", "konsola",
    "perfumy", "krem", "szampon",
    "ksiazka", "zabawka", "puzzle", "klocki",
    "posciel", "poduszka", "koldra", "recznik", "zaslona",
]

DEEP_FASHION_BLOCKED_WORDS = [
    "novelty", "ノベルティ", "sample", "gift", "promo", "limited gift", "付録",
    "mirror", "ミラー", "鏡", "basket", "バスケット", "籠", "かご",
    "cosmetic", "makeup", "化粧", "メイク", "ポーチのみ", "case only",
    "tableware", "plate", "cup", "mug", "bottle", "glass", "皿", "カップ", "マグ",
    "interior", "home", "room", "blanket", "pillow", "towel", "rug",
    "キッチン", "インテリア", "タオル", "ブランケット", "クッション",
    "baby", "kids", "child", "children", "ベビー", "キッズ", "子供",
]

DEEP_FASHION_SIZE_PATTERN = re.compile(
    r"(?<![a-z0-9])("
    r"xxs|xs|s|m|l|xl|xxl|xxxl|"
    r"it\s?\d{2}|eu\s?\d{2}|jp\s?\d{1,2}|us\s?\d{1,2}|"
    r"\d{2}(?:\.\d)?\s?cm"
    r")(?![a-z0-9])",
    re.IGNORECASE,
)

ALL_BRANDS = [
    "stone island", "balenciaga", "raf simons", "bape", "aape",
    "gucci", "chanel", "jeremy scott", "undercover", "comme des garcons",
    "yohji yamamoto", "vetements", "palm angels", "maison margiela",
    "givenchy", "burberry", "supreme", "amiri", "acne studios", "alyx",
]

USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

state = {
    "chat_id": None,
    "chat_ids": set(),
    "awaiting": None,
    "current_market": None,
    "brands_page": 0,
    "active_brands": set(ALL_BRANDS),
    "vinted_running": False,
    "vinted_min": 10,
    "vinted_max": 500,
    "vinted_min_age_hours": 0,
    "vinted_max_age_hours": MAX_AGE_HOURS,
    "vinted_keywords": [],
    "vinted_interval": 300,
    "vinted_seen": set(),
    "vinted_stats": {"found": 0, "cycles": 0},
    "_vinted_ts_field": None,
    "_vinted_debug_done": False,
    "mercari_running": False,
    "mercari_min": 1000,
    "mercari_max": 50000,
    "mercari_min_age_hours": 0,
    "mercari_max_age_hours": MAX_AGE_HOURS,
    "mercari_keywords": [],
    "mercari_interval": 300,
    "mercari_seen": set(),
    "mercari_stats": {"found": 0, "cycles": 0},
    "fruits_running": False,
    "fruits_min": 10000,
    "fruits_max": 1000000,
    "fruits_min_age_hours": 0,
    "fruits_max_age_hours": MAX_AGE_HOURS,
    "fruits_keywords": [],
    "fruits_interval": 300,
    "fruits_seen": set(),
    "fruits_stats": {"found": 0, "cycles": 0},
}

log = logging.getLogger("parser")
MSK_TZ = timezone(timedelta(hours=3), "MSK")
_eur_rate_cache = {"rate": None, "ts": 0}
_fx_rate_cache = {}


def register_chat_id(chat_id):
    if chat_id is None:
        return
    try:
        chat_id = int(chat_id)
    except (TypeError, ValueError):
        return
    state["chat_id"] = chat_id
    state.setdefault("chat_ids", set()).add(chat_id)


def notification_chat_ids():
    ids = set(state.get("chat_ids") or [])
    if state.get("chat_id") is not None:
        ids.add(state["chat_id"])
    return sorted(ids)


def get_jpy_to_eur() -> float:
    now = time.time()
    if _eur_rate_cache["rate"] and now - _eur_rate_cache["ts"] < 3600:
        return _eur_rate_cache["rate"]
    try:
        r = requests.get("https://api.frankfurter.app/latest?from=JPY&to=EUR", timeout=10)
        rate = float(r.json()["rates"]["EUR"])
        _eur_rate_cache["rate"] = rate
        _eur_rate_cache["ts"] = now
        log.info("Курс JPY->EUR обновлен: %.5f", rate)
        return rate
    except Exception as e:
        log.warning("Не удалось получить курс JPY->EUR: %s", e)
        return 0.0062


def get_fx_rate(from_currency: str, to_currency: str) -> float:
    src = from_currency.upper()
    dst = to_currency.upper()
    if src == dst:
        return 1.0
    key = (src, dst)
    now = time.time()
    cached = _fx_rate_cache.get(key)
    if cached and now - cached["ts"] < 6 * 3600:
        return cached["rate"]
    fallback = {("EUR", "PLN"): 4.23, ("PLN", "EUR"): 1 / 4.23}.get(key, 1.0)
    try:
        r = requests.get(
            "https://api.frankfurter.app/latest",
            params={"from": src, "to": dst},
            timeout=10,
        )
        rate = float(r.json()["rates"][dst])
        _fx_rate_cache[key] = {"rate": rate, "ts": now}
        return rate
    except Exception as e:
        log.warning("Не удалось получить курс %s->%s: %s", src, dst, e)
        return fallback


def vinted_domain_currency(domain: str) -> str:
    return "PLN" if domain.endswith(".pl") else "EUR"


def vinted_price_bounds(domain: str) -> tuple[float, float, str]:
    currency = vinted_domain_currency(domain)
    rate = get_fx_rate("EUR", currency)
    return state["vinted_min"] * rate, state["vinted_max"] * rate, currency


def vinted_price_to_eur(price: float, currency: str) -> float:
    return float(price) * get_fx_rate(currency or "EUR", "EUR")


def _try_parse_ts(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        dt = val if val.tzinfo else val.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    if hasattr(val, "seconds"):
        try:
            return _try_parse_ts(float(val.seconds))
        except (TypeError, ValueError):
            pass
    if hasattr(val, "timestamp") and callable(val.timestamp):
        try:
            return _try_parse_ts(float(val.timestamp()))
        except (TypeError, ValueError, OSError):
            pass
    if isinstance(val, (int, float)):
        ts = float(val)
        if 1577836800000 < ts < 1893456000000:
            ts /= 1000
        return ts if 1577836800 < ts < 1893456000 else None
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        try:
            return _try_parse_ts(float(val))
        except ValueError:
            pass
        try:
            v = val.replace(" UTC", "+00:00").replace("Z", "+00:00").replace(" ", "T")
            dt = datetime.fromisoformat(v)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(val[:19], fmt).replace(tzinfo=timezone.utc).timestamp()
            except Exception:
                pass
    return None


def format_msk_timestamp(ts) -> str:
    parsed = _try_parse_ts(ts)
    if not parsed:
        return "не указано"
    return datetime.fromtimestamp(parsed, tz=timezone.utc).astimezone(MSK_TZ).strftime("%d-%m-%Y в %H:%M МСК")


def _age_label(hours):
    hours = float(hours)
    return f"{int(hours)}ч" if hours == int(hours) else f"{hours:g}ч"


def age_range_label(min_hours, max_hours):
    min_hours = float(min_hours or 0)
    max_hours = float(max_hours or 0)
    if min_hours <= 0:
        return f"до {_age_label(max_hours)}"
    return f"{_age_label(min_hours)}–{_age_label(max_hours)}"


def _eur_label(value):
    return f"{float(value):g}"


def vinted_price_range_label():
    return f"{_eur_label(state['vinted_min'])}–{_eur_label(state['vinted_max'])}€"


def mercari_price_range_label():
    return f"{int(state['mercari_min']):,}–{int(state['mercari_max']):,}¥"


def fruits_price_range_label():
    return f"{int(state['fruits_min']):,}–{int(state['fruits_max']):,}₩"


def parse_keywords(text):
    raw = str(text or "").strip()
    if raw.lower() in ("", "-", "нет", "none", "clear", "off", "выкл"):
        return []
    parts = re.split(r"[,;\n]+", raw)
    result = []
    seen = set()
    for part in parts:
        keyword = re.sub(r"\s+", " ", part).strip()
        if not keyword:
            continue
        key = keyword.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(keyword)
    return result[:20]


def keywords_label(market):
    keywords = state.get(f"{market}_keywords", [])
    if not keywords:
        return "только бренд"
    text = ", ".join(keywords)
    return text if len(text) <= 90 else text[:87] + "..."


def _keyword_contains_brand(keyword, brand):
    keyword_l = re.sub(r"\s+", " ", str(keyword or "").lower()).strip()
    brand_l = re.sub(r"\s+", " ", str(brand or "").lower()).strip()
    if not keyword_l or not brand_l:
        return False
    return brand_l in keyword_l or brand_l.replace(" ", "") in keyword_l.replace(" ", "")


def _keyword_without_brand(keyword, brand):
    result = str(keyword or "").strip()
    brand_text = str(brand or "").strip()
    if not result or not brand_text:
        return result
    pattern = re.escape(brand_text).replace(r"\ ", r"\s+")
    result = re.sub(pattern, " ", result, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", result).strip(" ,;:-")


def market_search_queries(brand, market):
    keywords = state.get(f"{market}_keywords", [])
    if not keywords:
        return [(brand, "")]
    queries = []
    for keyword in keywords:
        query = keyword if _keyword_contains_brand(keyword, brand) else f"{brand} {keyword}"
        match_keyword = _keyword_without_brand(keyword, brand) if _keyword_contains_brand(keyword, brand) else keyword
        queries.append((query, match_keyword))
    return queries


def parse_age_range(text):
    nums = [n.replace(",", ".") for n in re.findall(r"\d+(?:[.,]\d+)?", text or "")]
    if not nums:
        raise ValueError
    if len(nums) == 1:
        min_hours = 0.0
        max_hours = float(nums[0])
    else:
        min_hours = float(nums[0])
        max_hours = float(nums[1])
    if min_hours < 0 or max_hours <= 0 or min_hours >= max_hours:
        raise ValueError
    return min_hours, max_hours


def _parse_price_number(raw):
    s = re.sub(r"[^\d,.\s]", "", str(raw or "")).replace(" ", "")
    if not s:
        raise ValueError
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        chunks = s.split(",")
        if len(chunks) > 1 and len(chunks[0]) <= 3 and all(len(c) == 3 for c in chunks[1:]):
            s = "".join(chunks)
        else:
            s = s.replace(",", ".")
    elif "." in s:
        chunks = s.split(".")
        if len(chunks) > 1 and len(chunks[0]) <= 3 and all(len(c) == 3 for c in chunks[1:]):
            s = "".join(chunks)
    return float(s)


def parse_price_range(text, *, is_int=False):
    raw = str(text or "").strip()
    parts = [p for p in re.split(r"\s*(?:-|–|—|до|to)\s*", raw, maxsplit=1, flags=re.IGNORECASE) if p.strip()]
    if len(parts) < 2:
        parts = re.findall(r"\d+(?:[.,]\d+)?", raw)
    if len(parts) < 2:
        raise ValueError
    min_price = _parse_price_number(parts[0])
    max_price = _parse_price_number(parts[1])
    if min_price <= 0 or max_price <= 0 or min_price >= max_price:
        raise ValueError
    if is_int:
        return int(min_price), int(max_price)
    return min_price, max_price


def publish_age_hours(ts):
    parsed = _try_parse_ts(ts)
    if not parsed:
        return None
    return (time.time() - parsed) / 3600


def age_in_range(ts, min_hours, max_hours):
    age = publish_age_hours(ts)
    if age is None:
        return None
    if age < -1:
        return False
    return float(min_hours) <= age <= float(max_hours)


def translate_to_ru(text: str) -> str:
    if not text or not text.strip():
        return text
    cyr = sum(1 for c in text if "\u0400" <= c <= "\u04FF")
    if cyr / max(len(text), 1) > 0.4:
        return text
    try:
        r = requests.get(
            "https://translate.googleapis.com/translate_a/single",
            params={"client": "gtx", "sl": "auto", "tl": "ru", "dt": "t", "q": text},
            timeout=8,
        )
        data = r.json()
        return "".join(part[0] for part in data[0] if part[0]).strip() or text
    except Exception:
        return text


def _obj_get(obj, *names, default=None):
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj[name]
        if hasattr(obj, name):
            return getattr(obj, name)
    return default


def _contains_term(text, term):
    term = str(term).lower().strip()
    if not term:
        return False
    if re.fullmatch(r"[a-z0-9][a-z0-9 .&'/-]*[a-z0-9]", term):
        pattern = r"(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])"
        return re.search(pattern, text) is not None
    return term in text


def _has_any_term(text, terms):
    return any(_contains_term(text, term) for term in terms)


def keyword_matches_text(text, keyword):
    if not keyword:
        return True
    return _contains_term(str(text or "").lower(), keyword)
