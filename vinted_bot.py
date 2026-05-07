#!/usr/bin/env python3
import logging, time, threading, os, random, requests, json as _json, gzip, re, html
from datetime import datetime, timezone, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
PROXY_URL  = os.environ.get("PROXY_URL", "")

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
    "pieluchy","pampers","baby","dziecko","dla dzieci","подгузники","детское",
    "nosidełko","fotelik","wózek","kocyk","smoczek","łóżeczko",
    "underwear","socks","bielizna","majtki","skarpety","rajstopy",
    "biustonosz","bokserki","stringi","figi",
    "kask","rower","hulajnoga","rolki","narty","deska",
    "telefon","laptop","tablet","konsola",
    "perfumy","krem","szampon",
    "książka","zabawka","puzzle","klocki",
    "pościel","poduszka","kołdra","ręcznik","zasłona",
]

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_eur_rate_cache = {"rate": None, "ts": 0}
_fx_rate_cache = {}
MSK_TZ = timezone(timedelta(hours=3), "MSK")

def get_jpy_to_eur() -> float:
    now = time.time()
    if _eur_rate_cache["rate"] and now - _eur_rate_cache["ts"] < 3600:
        return _eur_rate_cache["rate"]
    try:
        r = requests.get("https://api.frankfurter.app/latest?from=JPY&to=EUR", timeout=10)
        rate = r.json()["rates"]["EUR"]
        _eur_rate_cache["rate"] = rate
        _eur_rate_cache["ts"]   = now
        log.info(f"Курс JPY->EUR обновлён: {rate:.5f}")
        return rate
    except Exception as e:
        log.warning(f"Не удалось получить курс JPY->EUR: {e}")
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
    fallback = {
        ("EUR", "PLN"): 4.23,
        ("PLN", "EUR"): 1 / 4.23,
    }.get(key, 1.0)
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
        log.warning(f"Не удалось получить курс {src}->{dst}: {e}")
        return fallback

def vinted_domain_currency(domain: str) -> str:
    return "PLN" if domain.endswith(".pl") else "EUR"

def vinted_price_bounds(domain: str) -> tuple[float, float, str]:
    currency = vinted_domain_currency(domain)
    rate = get_fx_rate("EUR", currency)
    return state["vinted_min"] * rate, state["vinted_max"] * rate, currency

def vinted_price_to_eur(price: float, currency: str) -> float:
    return float(price) * get_fx_rate(currency or "EUR", "EUR")

def format_msk_timestamp(ts) -> str:
    if not ts:
        return "не указано"
    parsed = _try_parse_ts(ts)
    if not parsed:
        return "не указано"
    return datetime.fromtimestamp(parsed, tz=timezone.utc).astimezone(MSK_TZ).strftime("%d-%m-%Y в %H:%M МСК")

def format_msk_now() -> str:
    return datetime.now(MSK_TZ).strftime("%d-%m-%Y в %H:%M МСК")

def _age_label(hours):
    return f"{int(hours)}ч" if hours == int(hours) else f"{hours:g}ч"

def age_range_label(min_hours, max_hours):
    min_hours = float(min_hours or 0)
    max_hours = float(max_hours or 0)
    if min_hours <= 0:
        return f"до {_age_label(max_hours)}"
    return f"{_age_label(min_hours)}–{_age_label(max_hours)}"

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
    cyr = sum(1 for c in text if '\u0400' <= c <= '\u04FF')
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

ALL_BRANDS = [
    "stone island","balenciaga","raf simons","bape","aape",
    "gucci","chanel","jeremy scott","undercover","comme des garcons",
    "yohji yamamoto","vetements","palm angels","maison margiela",
    "givenchy","burberry","supreme","amiri","acne studios","alyx",
]

state = {
    "chat_id": None,
    "awaiting": None,
    "current_market": None,
    "brands_page": 0,
    "active_brands": set(ALL_BRANDS),
    "vinted_running": False,
    "vinted_min": 10,
    "vinted_max": 500,
    "vinted_min_age_hours": 0,
    "vinted_max_age_hours": MAX_AGE_HOURS,
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
    "mercari_interval": 300,
    "mercari_seen": set(),
    "mercari_stats": {"found": 0, "cycles": 0},
}

bot_app = None
USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]
vinted_sessions: dict = {}
mercari_api = None

def make_vinted_session(domain):
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": f"https://{domain}/",
        "Origin": f"https://{domain}",
    })
    if PROXY_URL:
        s.proxies = {"http": PROXY_URL, "https": PROXY_URL}
    return s

def init_vinted(domain):
    s = make_vinted_session(domain)
    try:
        s.get(f"https://{domain}/", timeout=15)
        s.get(f"https://{domain}/catalog", timeout=15)
    except Exception as e:
        log.warning(f"init_vinted {domain}: {e}")
    vinted_sessions[domain] = s
    return s

def get_vinted_session(domain):
    return vinted_sessions.get(domain) or init_vinted(domain)

def decode_response(r):
    enc     = r.headers.get("content-encoding", "").lower()
    content = r.content
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
        try: return r.json()
        except Exception: return {}

def fetch_vinted(query, domain, retry=True):
    s = get_vinted_session(domain)
    s.headers["User-Agent"] = random.choice(USER_AGENTS)
    try:
        price_from, price_to, currency = vinted_price_bounds(domain)
        params = [
            ("search_text", query), ("page", 1), ("per_page", 48),
            ("order", "newest_first"),
            ("price_from", f"{price_from:.2f}"),
            ("price_to",   f"{price_to:.2f}"),
            ("currency", currency),
        ]
        for cid in CATALOG_IDS:
            params.append(("catalog_ids[]", cid))
        r = s.get(f"https://{domain}/api/v2/catalog/items", params=params, timeout=20)
        if r.status_code == 200:
            items = decode_response(r).get("items", [])
            if items:
                log.info(f"Vinted {domain} -> {len(items)} товаров")
                if not state["_vinted_debug_done"]:
                    item0 = items[0]
                    log.info(f"DEBUG keys: {list(item0.keys())}")
                    for k in ("created_at_ts","updated_at_ts","created_at","updated_at",
                              "active_at","last_push_up_at","activation_ts"):
                        if k in item0:
                            log.info(f"DEBUG {k} = {item0[k]!r}")
                    state["_vinted_debug_done"] = True
            return items
        elif r.status_code == 401 and retry:
            log.warning(f"Vinted session expired {domain}, обновляю cookies")
            vinted_sessions.pop(domain, None)
            init_vinted(domain)
            return fetch_vinted(query, domain, retry=False)
        elif r.status_code in (403, 429):
            log.error(f"Vinted BAN {r.status_code} {domain}")
            vinted_sessions.pop(domain, None)
            return "BAN"
        return []
    except Exception as e:
        log.warning(f"fetch_vinted {domain}: {e}")
        return []

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
            ts = ts / 1000
        if 1577836800 < ts < 1893456000:
            return ts
        return None
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        try:
            ts = float(val)
            if 1577836800000 < ts < 1893456000000:
                ts = ts / 1000
            if 1577836800 < ts < 1893456000:
                return ts
        except ValueError:
            pass
        try:
            v  = val.replace(" UTC", "+00:00").replace("Z", "+00:00").replace(" ", "T")
            dt = datetime.fromisoformat(v)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(val[:19], fmt).replace(tzinfo=timezone.utc)
                return dt.timestamp()
            except Exception:
                pass
    return None

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
        "created_at_ts","updated_at_ts","activation_ts",
        "created_at","updated_at","active_at","last_push_up_at",
        "photo.high_resolution.timestamp","photo.timestamp",
        "photos.0.high_resolution.timestamp","photos.0.timestamp",
    ]
    for key in candidates:
        val = _get_nested(item, key)
        if val is None:
            continue
        ts = _try_parse_ts(val)
        if ts:
            if state.get("_vinted_ts_field") != key:
                state["_vinted_ts_field"] = key
                log.info(f"Поле времени Vinted: '{key}' = {val!r}")
            return ts
    return None

def is_relevant(item, brand):
    title  = item.get("title", "").lower()
    brand2 = item.get("brand_title", "").lower()
    word   = brand.split()[0]
    if not (word in title or word in brand2):
        return False
    if not is_deep_fashion_vinted_item(item):
        log.info(f"SKIP Vinted deep fashion filter: {item.get('title','?')[:40]}")
        return False
    if any(w in title for w in BAD_WORDS):
        return False
    ts = parse_vinted_ts(item)
    if ts is None:
        log.info(f"SKIP no publish time id={item.get('id')} '{item.get('title','?')[:40]}'")
        return False
    age_ok = age_in_range(ts, state["vinted_min_age_hours"], state["vinted_max_age_hours"])
    age_hours = publish_age_hours(ts)
    if age_ok is False:
        log.info(f"SKIP Vinted age {age_hours:.1f}h: {item.get('title','?')[:40]}")
        return False
    return True

def vinted_loop():
    import asyncio
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
            if not state["vinted_running"]: break
            for _, domain in VINTED_REGIONS.items():
                if not state["vinted_running"]: break
                items = fetch_vinted(brand, domain)
                if items == "BAN":
                    time.sleep(random.randint(60, 120))
                    continue
                for item in (items or []):
                    iid = item.get("id")
                    if iid in state["vinted_seen"]: continue
                    state["vinted_seen"].add(iid)
                    if not is_relevant(item, brand): continue
                    try:
                        price = float(item.get("price", {}).get("amount", 0))
                    except (ValueError, TypeError):
                        continue

                    pd      = item.get("price", {})
                    curr    = pd.get("currency_code", "EUR")
                    price_eur = vinted_price_to_eur(price, curr)
                    if not (state["vinted_min"] <= price_eur <= state["vinted_max"]): continue
                    title   = item.get("title", "?")
                    size    = item.get("size_title", "")
                    brand_t = item.get("brand_title", "")
                    cond    = item.get("status", "")
                    url     = item.get("url", "")
                    link    = f"https://{domain}{url}" if url.startswith("/") else url
                    title_ru = translate_to_ru(title)

                    age_str = ""
                    ts_d = parse_vinted_ts(item)
                    if ts_d:
                        age_min = (time.time() - ts_d) / 60
                        age_str = f"{int(age_min)} мин. назад" if age_min < 60 else f"{age_min/60:.1f} ч. назад"

                    photos = item.get("photos") or item.get("photo") or []
                    if isinstance(photos, dict): photos = [photos]
                    photo_url = ""
                    if photos:
                        p = photos[0]
                        photo_url = p.get("full_size_url") or p.get("url") or p.get("thumb_url", "")

                    extra = []
                    if size: extra.append(f"Размер: {size}")
                    if cond: extra.append(f"Состояние: {cond}")
                    if age_str: extra.append(f"🕐 {age_str}")

                    lines = [
                        "🔔 <b>Новый товар!</b>",
                        f"🧥 Vinted • {(brand_t or brand).lower()} винтед",
                        "",
                        title_ru,
                    ]
                    if extra: lines.append("  •  ".join(extra))
                    lines += [f"💰 {price} {curr}", f"<a href='{link}'>Открыть</a>"]
                    msg = format_vinted_message(item, domain, title, title_ru, price, curr, link, photo_url, ts_d, brand_t, size, cond)

                    state["vinted_stats"]["found"] += 1
                    log.info(f"FOUND Vinted: {title} — {price}")
                    if state["chat_id"] and bot_app:
                        if photo_url:
                            try:
                                loop.run_until_complete(
                                    bot_app.bot.send_photo(
                                        chat_id=state["chat_id"], photo=photo_url,
                                        caption=msg, parse_mode="HTML",
                                    )
                                )
                            except Exception:
                                loop.run_until_complete(
                                    bot_app.bot.send_message(
                                        chat_id=state["chat_id"], text=msg,
                                        parse_mode="HTML", disable_web_page_preview=True,
                                    )
                                )
                        else:
                            loop.run_until_complete(
                                bot_app.bot.send_message(
                                    chat_id=state["chat_id"], text=msg,
                                    parse_mode="HTML", disable_web_page_preview=True,
                                )
                            )
                time.sleep(random.uniform(10, 18))
            time.sleep(random.uniform(12, 25))

        if state["vinted_running"]:
            time.sleep(state["vinted_interval"])
    loop.close()

def _obj_get(obj, *names, default=None):
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj[name]
        if hasattr(obj, name):
            return getattr(obj, name)
    return default

MERCARI_ALLOWED_WORDS = [
    "shirt", "t-shirt", "tee", "hoodie", "sweat", "sweater", "jacket", "coat",
    "pants", "jeans", "denim", "trousers", "shorts", "skirt", "dress", "knit",
    "cardigan", "vest", "blouson", "cargo", "sneaker", "sneakers", "shoes",
    "boots", "loafer", "sandals", "cap", "hat", "beanie", "bag", "backpack",
    "wallet", "belt", "scarf", "gloves", "sunglasses", "wear", "clothes",
    "シャツ", "tシャツ", "パーカー", "スウェット", "ジャケット", "コート",
    "パンツ", "デニム", "ジーンズ", "スカート", "ワンピース", "ニット",
    "カーディガン", "セーター", "ベスト", "ブルゾン", "スニーカー",
    "シューズ", "靴", "ブーツ", "サンダル", "ローファー", "帽子", "キャップ",
    "ハット", "ニット帽", "バッグ", "リュック", "財布", "ベルト", "マフラー",
    "手袋", "サングラス", "服", "衣",
]

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
]

MERCARI_BLOCKED_WORDS += [
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
    ("shoes", ["sneaker", "sneakers", "shoe", "shoes", "boots", "loafer", "loafers", "sandals", "スニーカー", "シューズ", "靴", "ブーツ", "サンダル"]),
    ("bag", ["bag", "bags", "backpack", "wallet", "shoulder bag", "tote", "pouch", "バッグ", "リュック", "財布", "ショルダーバッグ", "トート", "ポーチ"]),
    ("tops", ["shirt", "t-shirt", "tee", "hoodie", "sweatshirt", "sweat", "sweater", "knit", "cardigan", "polo", "top", "blouse", "シャツ", "tシャツ", "パーカー", "スウェット", "ニット", "カーディガン", "ブラウス", "トップス"]),
    ("outerwear", ["jacket", "coat", "blouson", "vest", "parka", "down jacket", "windbreaker", "ジャケット", "コート", "ブルゾン", "ベスト", "ダウン", "アウター"]),
    ("bottoms", ["pants", "jeans", "denim", "trousers", "shorts", "skirt", "cargo", "slacks", "パンツ", "デニム", "ジーンズ", "ショーツ", "スカート", "スラックス"]),
    ("dress", ["dress", "one piece", "one-piece", "ワンピース", "ドレス"]),
    ("accessory", ["cap", "hat", "beanie", "belt", "scarf", "gloves", "sunglasses", "帽子", "キャップ", "ハット", "ニット帽", "ベルト", "マフラー", "手袋", "サングラス"]),
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

def _brand_tokens(brand):
    tokens = [brand.lower()]
    aliases = {
        "stone island": ["stone island", "stoneisland", "ストーンアイランド"],
        "balenciaga": ["balenciaga", "バレンシアガ"],
        "bape": ["bape", "a bathing ape", "ベイプ", "エイプ", "アベイシングエイプ"],
        "aape": ["aape", "aape by a bathing ape", "エーエイプ"],
        "gucci": ["gucci", "グッチ"],
        "chanel": ["chanel", "シャネル"],
        "jeremy scott": ["jeremy scott", "ジェレミースコット"],
        "undercover": ["undercover", "under cover", "アンダーカバー"],
        "comme des garcons": ["comme des garcons", "comme des garçons", "garcons", "garçons", "cdg", "コムデギャルソン"],
        "yohji yamamoto": ["yohji yamamoto", "yohji", "ヨウジヤマモト"],
        "vetements": ["vetements", "ヴェトモン"],
        "palm angels": ["palm angels", "パームエンジェルス"],
        "givenchy": ["givenchy", "ジバンシィ", "ジバンシー"],
        "burberry": ["burberry", "バーバリー"],
        "supreme": ["supreme", "シュプリーム"],
        "amiri": ["amiri", "アミリ"],
        "raf simons": ["raf simons", "ラフシモンズ"],
        "acne studios": ["acne studios", "acne", "アクネ"],
        "alyx": ["alyx", "1017 alyx", "1017 alyx 9sm", "アリクス"],
        "maison margiela": ["maison margiela", "margiela", "マルジェラ"],
    }
    tokens.extend(aliases.get(brand.lower(), []))
    return [token for token in dict.fromkeys(tokens) if token]

def is_relevant_mercari_item(item):
    return bool(deep_fashion_kind(item))

def mercari_matches_brand(item, brand):
    text = _mercari_text_blob(item)
    return _has_any_term(text, _brand_tokens(brand))

def _best_mercari_image_url(url):
    if not url:
        return ""
    url = str(url)
    url = re.sub(r"([?&])(w|width|h|height)=\d+&?", r"\1", url)
    url = url.replace("?=", "?").replace("&&", "&").rstrip("?&")
    url = re.sub(r"/resize:[^/]+/", "/", url)
    url = re.sub(r"_(?:thumb|small|medium)(\.[a-zA-Z0-9]+)$", r"\1", url)
    return url

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
            log.info(f"Mercari '{query}' -> {len(items)} товаров")
        return items
    except Exception as e:
        log.warning(f"fetch_mercari '{query}': {e}")
        return []

def fetch_mercari_old(query):
    try:
        proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": f"https://jp.mercari.com/search?keyword={requests.utils.quote(query)}&status=on_sale",
            "Origin": "https://jp.mercari.com",
            "X-Platform": "web",
        }
        r = requests.get(
            "https://api.mercari.jp/items/get",
            params={
                "search_keyword": query,
                "status": "on_sale",
                "order": "desc",
                "sort": "created_time",
                "item_types": "1",
                "page_size": 30,
                "price_min": state["mercari_min"],
                "price_max": state["mercari_max"],
            },
            headers=headers, proxies=proxies, timeout=20,
        )
        if r.status_code == 200:
            data  = r.json()
            items = data.get("data", data.get("items", []))
            if items: log.info(f"Mercari '{query}' -> {len(items)} товаров")
            return items
        else:
            r2 = requests.get(
                "https://jp.mercari.com/api/items/search",
                params={
                    "keyword": query, "status": "on_sale",
                    "page": 1, "limit": 30,
                    "price_min": state["mercari_min"],
                    "price_max": state["mercari_max"],
                },
                headers=headers, proxies=proxies, timeout=20,
            )
            if r2.status_code == 200:
                data  = r2.json()
                items = data.get("items", data.get("data", []))
                if items: log.info(f"Mercari '{query}' -> {len(items)} товаров")
                return items
            log.warning(f"Mercari {r.status_code}/{r2.status_code} '{query}'")
            return []
    except Exception as e:
        log.warning(f"fetch_mercari '{query}': {e}")
        return []

def mercari_loop():
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    log.info("Mercari мониторинг запущен")

    while state["mercari_running"]:
        brands = list(state["active_brands"])
        random.shuffle(brands)
        state["mercari_stats"]["cycles"] += 1

        for brand in brands:
            if not state["mercari_running"]: break
            items = loop.run_until_complete(fetch_mercari(brand))
            for item in (items or []):
                iid = item.get("id")
                if not iid:
                    log.info(f"SKIP Mercari no item id: {item.get('name', '?')[:60]}")
                    continue
                if iid in state["mercari_seen"]: continue

                name  = item.get("name", "?")
                price = item.get("price", 0)
                try: price = int(price)
                except (ValueError, TypeError):
                    log.info(f"SKIP Mercari bad price: {name[:60]} price={price!r}")
                    continue
                if not (state["mercari_min"] <= price <= state["mercari_max"]):
                    log.info(f"SKIP Mercari price {price}: {name[:60]}")
                    continue
                if not mercari_matches_brand(item, brand):
                    log.info(f"SKIP Mercari brand mismatch '{brand}': {name[:60]}")
                    continue
                if not is_relevant_mercari_item(item):
                    log.info(f"SKIP Mercari category: {name[:60]}")
                    continue
                if not item.get("created_at"):
                    log.info(f"SKIP Mercari no publish time: {name[:60]}")
                    continue
                age_ok = age_in_range(
                    item.get("created_at"),
                    state["mercari_min_age_hours"],
                    state["mercari_max_age_hours"],
                )
                if age_ok is False:
                    age_hours = publish_age_hours(item.get("created_at"))
                    age_label = f"{age_hours:.1f}h" if age_hours is not None else "unknown"
                    log.info(f"SKIP Mercari age {age_label}: {name[:60]}")
                    continue

                thumbs    = item.get("thumbnails") or item.get("item_images") or []
                thumb     = (thumbs[0].get("url") or thumbs[0].get("image_url", "")) if thumbs else ""
                thumb     = _best_mercari_image_url(thumb)
                iid2      = item.get("id", "")
                link      = item.get("url") or f"https://jp.mercari.com/item/{iid2}"
                if not link or link.rstrip("/").endswith("/item"):
                    log.info(f"SKIP Mercari bad link id={iid2!r}: {name[:60]}")
                    continue
                name_ru   = translate_to_ru(name)
                rate      = get_jpy_to_eur()
                eur       = round(price * rate, 2) if rate else None
                market = mercari_market_price_jpy(items, item, brand)
                if not market:
                    log.info(f"SKIP Mercari no market sample: {name[:60]}")
                    continue
                market_jpy = int(market["price"])
                market_count = int(market["count"])
                if price > market_jpy * MERCARI_MAX_MARKET_RATIO:
                    log.info(f"SKIP Mercari not under market {price}/{market_jpy}: {name[:60]}")
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

                lines = [
                    "🔔 <b>Новый товар!</b>",
                    f"🧥 Mercari 🇯🇵 • {brand.lower()} меркари",
                    "",
                    name_ru,
                    f"💰 {price_str}",
                    f"<a href='{link}'>Открыть</a>",
                ]
                msg = format_mercari_message(item, name, name_ru, price, price_str, link, thumb)
                state["mercari_seen"].add(iid)
                state["mercari_stats"]["found"] += 1
                log.info(f"FOUND Mercari: {name} — ¥{price}")
                if state["chat_id"] and bot_app:
                    if thumb:
                        try:
                            loop.run_until_complete(
                                bot_app.bot.send_photo(
                                    chat_id=state["chat_id"], photo=thumb,
                                    caption=msg, parse_mode="HTML",
                                )
                            )
                        except Exception as e:
                            log.warning(f"Mercari send_photo failed: {e}")
                            try:
                                loop.run_until_complete(
                                    bot_app.bot.send_message(
                                        chat_id=state["chat_id"], text=msg,
                                        parse_mode="HTML", disable_web_page_preview=True,
                                    )
                                )
                            except Exception as e2:
                                log.warning(f"Mercari send_message failed: {e2}")
                    else:
                        try:
                            loop.run_until_complete(
                                bot_app.bot.send_message(
                                    chat_id=state["chat_id"], text=msg,
                                    parse_mode="HTML", disable_web_page_preview=True,
                                )
                            )
                        except Exception as e:
                            log.warning(f"Mercari send_message failed: {e}")

            time.sleep(random.uniform(8, 15))

        if state["mercari_running"]:
            time.sleep(state["mercari_interval"])
    loop.close()

def main_kb():
    v  = "⏹ Стоп Vinted"  if state["vinted_running"]  else "▶️ Старт Vinted"
    m  = "⏹ Стоп Mercari" if state["mercari_running"] else "▶️ Старт Mercari"
    vs = "🟢" if state["vinted_running"]  else "🔴"
    ms = "🟢" if state["mercari_running"] else "🔴"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{vs} {v}", callback_data="toggle_vinted")],
        [InlineKeyboardButton(f"{ms} {m}", callback_data="toggle_mercari")],
        [InlineKeyboardButton("⚙️ Настройки Vinted",  callback_data="vinted_settings")],
        [InlineKeyboardButton("⚙️ Настройки Mercari", callback_data="mercari_settings")],
        [InlineKeyboardButton("👕 Бренды", callback_data="brands_0"),
         InlineKeyboardButton("📊 Статус", callback_data="status")],
    ])

def vinted_settings_kb():
    age = state["vinted_max_age_hours"]
    age_label = f"{int(age)}ч" if age == int(age) else f"{age}ч"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"💶 Мин: {state['vinted_min']}€", callback_data="vset_min"),
         InlineKeyboardButton(f"💶 Макс: {state['vinted_max']}€", callback_data="vset_max")],
        [InlineKeyboardButton(f"🕐 Возраст: до {age_label}", callback_data="vset_age")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back")],
    ])

def mercari_settings_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"¥ Мин: {state['mercari_min']:,}¥", callback_data="mset_min"),
         InlineKeyboardButton(f"¥ Макс: {state['mercari_max']:,}¥", callback_data="mset_max")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back")],
    ])

def brands_kb(page=0):
    per_page = 5
    start    = page * per_page
    chunk    = ALL_BRANDS[start:start + per_page]
    rows = []
    for brand in chunk:
        active = brand in state["active_brands"]
        icon   = "✅" if active else "☐"
        rows.append([InlineKeyboardButton(f"{icon} {brand.title()}", callback_data=f"brand_{brand}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Назад", callback_data=f"brands_{page-1}"))
    if start + per_page < len(ALL_BRANDS):
        nav.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"brands_{page+1}"))
    if nav: rows.append(nav)
    rows.append([
        InlineKeyboardButton("✅ Все",       callback_data="brands_all"),
        InlineKeyboardButton("☐ Снять все", callback_data="brands_none"),
    ])
    rows.append([InlineKeyboardButton("🔙 Главное меню", callback_data="back")])
    return InlineKeyboardMarkup(rows)

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    vs = "🟢" if state["vinted_running"] else "🔴"
    ms = "🟢" if state["mercari_running"] else "🔴"
    text = (
        f"<b>Vinted + Mercari Monitor</b>\n\n"
        f"{vs} Vinted: .pl .lt .lv | {state['vinted_min']}–{state['vinted_max']}€ | до {state['vinted_max_age_hours']}ч\n"
        f"{ms} Mercari: jp.mercari.com | {state['mercari_min']:,}–{state['mercari_max']:,}¥\n\n"
        f"Активных брендов: {len(state['active_brands'])} из {len(ALL_BRANDS)}"
    )
    await update.message.reply_text(text, reply_markup=main_kb(), parse_mode="HTML")

async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    await q.answer()
    state["chat_id"] = q.message.chat_id
    data = q.data

    async def edit(text, kb=None):
        try:
            await q.edit_message_text(text, reply_markup=kb or main_kb(), parse_mode="HTML")
        except Exception:
            pass

    if data == "toggle_vinted":
        if state["vinted_running"]:
            state["vinted_running"] = False
            await edit("Vinted остановлен.")
        else:
            if not state["active_brands"]:
                await q.answer("Выбери хотя бы один бренд!", show_alert=True); return
            state["vinted_running"] = True
            threading.Thread(target=vinted_loop, daemon=True).start()
            await edit(
                f"▶️ <b>Vinted запущен!</b>\n"
                f"Брендов: {len(state['active_brands'])}\n"
                f"Цена: {state['vinted_min']}–{state['vinted_max']}€\n"
                f"Фильтр возраста: до {state['vinted_max_age_hours']}ч"
            )
    elif data == "toggle_mercari":
        if state["mercari_running"]:
            state["mercari_running"] = False
            await edit("Mercari остановлен.")
        else:
            if not state["active_brands"]:
                await q.answer("Выбери хотя бы один бренд!", show_alert=True); return
            state["mercari_running"] = True
            threading.Thread(target=mercari_loop, daemon=True).start()
            await edit(
                f"▶️ <b>Mercari запущен!</b>\n"
                f"Брендов: {len(state['active_brands'])}\n"
                f"Цена: {state['mercari_min']:,}–{state['mercari_max']:,}¥"
            )
    elif data == "vinted_settings":
        await edit(
            f"<b>Настройки Vinted</b>\n\n"
            f"Цена: {state['vinted_min']}€ – {state['vinted_max']}€\n"
            f"Фильтр возраста: не старше <b>{state['vinted_max_age_hours']}ч</b>",
            vinted_settings_kb()
        )
    elif data == "vset_min":
        state["awaiting"] = "vinted_min"
        await edit(f"Введи минимальную цену Vinted (€)\nСейчас: <b>{state['vinted_min']}€</b>\n\nНапример: <code>10</code>")
    elif data == "vset_max":
        state["awaiting"] = "vinted_max"
        await edit(f"Введи максимальную цену Vinted (€)\nСейчас: <b>{state['vinted_max']}€</b>\n\nНапример: <code>500</code>")
    elif data == "vset_age":
        state["awaiting"] = "vinted_age"
        await edit(
            f"Введи максимальный возраст объявления в часах\n"
            f"Сейчас: <b>{state['vinted_max_age_hours']}ч</b>\n\n"
            f"<code>6</code> — 6 часов\n<code>24</code> — сутки\n<code>168</code> — неделя"
        )
    elif data == "mercari_settings":
        await edit(
            f"<b>Настройки Mercari</b>\n\nЦена: {state['mercari_min']:,}¥ – {state['mercari_max']:,}¥",
            mercari_settings_kb()
        )
    elif data == "mset_min":
        state["awaiting"] = "mercari_min"
        await edit(f"Введи минимальную цену Mercari (¥)\nСейчас: <b>{state['mercari_min']:,}¥</b>\n\nНапример: <code>1000</code>")
    elif data == "mset_max":
        state["awaiting"] = "mercari_max"
        await edit(f"Введи максимальную цену Mercari (¥)\nСейчас: <b>{state['mercari_max']:,}¥</b>\n\nНапример: <code>50000</code>")
    elif data == "status":
        vs = state["vinted_stats"]
        ms = state["mercari_stats"]
        tf = state.get("_vinted_ts_field") or "не определено"
        await edit(
            f"<b>Статус</b>\n\n"
            f"<b>Vinted</b> {'🟢' if state['vinted_running'] else '🔴'}\n"
            f"Циклов: {vs['cycles']} | Находок: {vs['found']}\n"
            f"Цена: {state['vinted_min']}–{state['vinted_max']}€\n"
            f"Фильтр: до {state['vinted_max_age_hours']}ч\n"
            f"Поле времени: <code>{tf}</code>\n\n"
            f"<b>Mercari</b> {'🟢' if state['mercari_running'] else '🔴'}\n"
            f"Циклов: {ms['cycles']} | Находок: {ms['found']}\n"
            f"Цена: {state['mercari_min']:,}–{state['mercari_max']:,}¥\n\n"
            f"Брендов: {len(state['active_brands'])}"
        )
    elif data.startswith("brands_") and data not in ("brands_all", "brands_none"):
        try: page = int(data.split("_")[1])
        except (IndexError, ValueError): page = 0
        active = len(state["active_brands"])
        total  = len(ALL_BRANDS)
        await edit(
            f"<b>Бренды</b>\n\nАктивных: {active} из {total}\n"
            f"Страница {page+1}/{(total-1)//5+1}\n\nНажми чтобы включить/выключить:",
            brands_kb(page)
        )
    elif data == "brands_all":
        state["active_brands"] = set(ALL_BRANDS)
        await edit(f"<b>Бренды</b>\n\nВсе {len(ALL_BRANDS)} активны:", brands_kb(0))
    elif data == "brands_none":
        state["active_brands"] = set()
        await edit(f"<b>Бренды</b>\n\nВсе отключены:", brands_kb(0))
    elif data.startswith("brand_"):
        brand = data[6:]
        if brand in state["active_brands"]: state["active_brands"].discard(brand)
        else: state["active_brands"].add(brand)
        page = next((i // 5 for i, b in enumerate(ALL_BRANDS) if b == brand), 0)
        active = len(state["active_brands"])
        await edit(
            f"<b>Бренды</b>\n\nАктивных: {active} из {len(ALL_BRANDS)}\n"
            f"Страница {page+1}/{(len(ALL_BRANDS)-1)//5+1}:",
            brands_kb(page)
        )
    elif data == "back":
        vs2 = "🟢" if state["vinted_running"] else "🔴"
        ms2 = "🟢" if state["mercari_running"] else "🔴"
        await edit(
            f"<b>Vinted + Mercari Monitor</b>\n\n"
            f"{vs2} Vinted: {state['vinted_min']}–{state['vinted_max']}€ | до {state['vinted_max_age_hours']}ч\n"
            f"{ms2} Mercari: {state['mercari_min']:,}–{state['mercari_max']:,}¥\n"
            f"Брендов: {len(state['active_brands'])}"
        )

async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    aw   = state.get("awaiting")
    text = update.message.text.strip().replace(",", ".")
    mapping = {
        "vinted_min":  ("vinted_min",  "€", False),
        "vinted_max":  ("vinted_max",  "€", False),
        "mercari_min": ("mercari_min", "¥", True),
        "mercari_max": ("mercari_max", "¥", True),
    }
    if aw in mapping:
        key, symbol, is_int = mapping[aw]
        try:
            val = int(float(text)) if is_int else float(text)
            if val > 0:
                state[key]        = val
                state["awaiting"] = None
                fmt = f"{val:,}" if is_int else str(val)
                await update.message.reply_text(
                    f"✅ Установлено: <b>{fmt}{symbol}</b>",
                    parse_mode="HTML", reply_markup=main_kb()
                )
            else:
                await update.message.reply_text("Введи число больше 0", reply_markup=main_kb())
        except ValueError:
            await update.message.reply_text("Нужно число", reply_markup=main_kb())
    elif aw == "vinted_age":
        try:
            val = float(text)
            if val > 0:
                state["vinted_max_age_hours"] = val
                state["awaiting"]             = None
                label = f"{int(val)}ч" if val == int(val) else f"{val}ч"
                await update.message.reply_text(
                    f"✅ Фильтр возраста: не старше <b>{label}</b>",
                    parse_mode="HTML", reply_markup=main_kb()
                )
            else:
                await update.message.reply_text("Введи число больше 0", reply_markup=main_kb())
        except ValueError:
            await update.message.reply_text("Нужно число, например: 24", reply_markup=main_kb())
    else:
        await update.message.reply_text("Используй /start", reply_markup=main_kb())

def format_vinted_message(item, domain, title, title_ru, price, curr, link, photo_url, ts_d, brand_t, size, cond):
    country = domain.rsplit(".", 1)[-1].upper()
    seller = item.get("user", {}) or {}
    seller_name = html.escape(str(seller.get("login") or seller.get("username") or "не указан"))
    posted = format_msk_timestamp(ts_d)
    details = []
    if brand_t:
        details.append(str(brand_t))
    if size:
        details.append(str(size))
    if cond:
        details.append(str(cond))
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

def format_mercari_message(item, name, name_ru, price, price_str, link, thumb):
    seller = item.get("seller") if isinstance(item, dict) else None
    seller_name = html.escape(str((seller or {}).get("name") or (seller or {}).get("id") or "не указан"))
    title_safe = html.escape(str(name_ru or name))
    price_safe = str(price_str)
    link_safe = html.escape(str(link), quote=True)
    posted = format_msk_timestamp(item.get("created_at")) if isinstance(item, dict) else "не указано"
    return (
        "<b>Mercari JP</b>\n"
        f"<b>{title_safe}</b>\n\n"
        f"<b>Цена:</b> {price_safe}\n"
        f"<b>Публикация:</b> {posted}\n"
        f"<b>Продавец:</b> {seller_name}\n\n"
        f"<a href='{link_safe}'>Открыть объявление</a>"
    )

def _age_label(hours):
    return f"{int(hours)}ч" if hours == int(hours) else f"{hours:g}ч"

def _market_title(market=None):
    market = market or state.get("current_market") or "vinted"
    return "Mercari.jp" if market == "mercari" else "Vinted"

def _market_running(market=None):
    market = market or state.get("current_market") or "vinted"
    return state["mercari_running"] if market == "mercari" else state["vinted_running"]

def _market_stats(market=None):
    market = market or state.get("current_market") or "vinted"
    return state["mercari_stats"] if market == "mercari" else state["vinted_stats"]

def main_text():
    return (
        "<b>Parser #1</b>\n"
        "└ Выбери площадку для мониторинга\n\n"
        f"🇯🇵 <b>Mercari.jp</b>\n"
        f"└ Статус: {'работает' if state['mercari_running'] else 'остановлен'}\n"
        f"└ Цена: {state['mercari_min']:,}–{state['mercari_max']:,}¥\n"
        f"└ Публикация: {age_range_label(state['mercari_min_age_hours'], state['mercari_max_age_hours'])}\n\n"
        f"🌍 <b>Vinted</b>\n"
        f"└ Статус: {'работает' if state['vinted_running'] else 'остановлен'}\n"
        f"└ Цена: {state['vinted_min']}–{state['vinted_max']}€\n"
        f"└ Публикация: {age_range_label(state['vinted_min_age_hours'], state['vinted_max_age_hours'])}"
    )

def main_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🇯🇵 Mercari.jp", callback_data="pick_mercari"),
         InlineKeyboardButton("🌍 Vinted", callback_data="pick_vinted")],
        [InlineKeyboardButton("👕 Бренды", callback_data="brands_0"),
         InlineKeyboardButton("ⓘ Статус", callback_data="status")],
    ])

def market_text(market=None):
    market = market or state.get("current_market") or "vinted"
    stats = _market_stats(market)
    title = _market_title(market)
    status = "Работает" if _market_running(market) else "Остановлен"
    last = datetime.now(MSK_TZ).strftime("%H:%M МСК")
    if market == "mercari":
        area = "jp.mercari.com"
        filters = (
            f"Цена: {state['mercari_min']:,}–{state['mercari_max']:,}¥ | "
            f"Публикация: {age_range_label(state['mercari_min_age_hours'], state['mercari_max_age_hours'])}"
        )
    else:
        area = ".pl .lt .lv"
        filters = (
            f"Цена: {state['vinted_min']}–{state['vinted_max']}€ | "
            f"Публикация: {age_range_label(state['vinted_min_age_hours'], state['vinted_max_age_hours'])}"
        )
    return (
        f"<b>{title}</b>\n"
        f"└ {area}\n\n"
        f"ⓘ <b>Статус</b>\n"
        f"└ {status}\n\n"
        f"⚭ <b>Активных брендов</b>\n"
        f"└ {len(state['active_brands'])}\n\n"
        f"◷ <b>Последнее обновление</b>\n"
        f"└ {last}\n\n"
        f"⌁ <b>Фильтры</b>\n"
        f"└ {filters}\n"
        f"└ Найдено: {stats['found']} | Циклов: {stats['cycles']}"
    )

def market_kb(market=None):
    market = market or state.get("current_market") or "vinted"
    run_text = "⏹ Остановить" if _market_running(market) else "▶ Запустить"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(run_text, callback_data=f"toggle_{market}")],
        [InlineKeyboardButton("ⓘ Фильтры", callback_data=f"filters_{market}"),
         InlineKeyboardButton(f"ⓘ {_market_title(market)}", callback_data=f"pick_{market}")],
        [InlineKeyboardButton("↻ Сменить площадку", callback_data="back")],
    ])

def filters_text(market=None):
    market = market or state.get("current_market") or "vinted"
    if market == "mercari":
        return (
            "<b>Mercari.jp • Фильтры</b>\n\n"
            "🌐 <b>Страна</b>\n"
            "└ Япония\n\n"
            "▣ <b>Категории</b>\n"
            "└ Все\n\n"
            "▣ <b>Цена</b>\n"
            f"└ {state['mercari_min']:,}–{state['mercari_max']:,}¥\n\n"
            "◷ <b>Период публикации</b>\n"
            f"└ {age_range_label(state['mercari_min_age_hours'], state['mercari_max_age_hours'])}\n\n"
            "⊘ <b>Банворды</b>\n"
            f"└ {len(BAD_WORDS)}\n\n"
            "☮ <b>Фильтры продавца</b>\n"
            "┌ Объявления: до 10\n"
            "├ Продажи: 0\n"
            "├ Покупки: 0\n"
            "└ Отзывы: 0"
        )
    return (
        "<b>Vinted • Фильтры</b>\n\n"
        "🌐 <b>Страны</b>\n"
        f"└ {', '.join(VINTED_REGIONS.keys())}\n\n"
        "▣ <b>Категории</b>\n"
        "└ Одежда / обувь / аксессуары\n\n"
        "▣ <b>Цена</b>\n"
        f"└ {state['vinted_min']}–{state['vinted_max']}€\n\n"
        "◷ <b>Период публикации</b>\n"
        f"└ {age_range_label(state['vinted_min_age_hours'], state['vinted_max_age_hours'])}\n\n"
        "⊘ <b>Банворды</b>\n"
        f"└ {len(BAD_WORDS)}\n\n"
        "☮ <b>Фильтры продавца</b>\n"
        "┌ Объявления: до 10\n"
        "├ Продажи: 0\n"
        "├ Покупки: 0\n"
        "├ Отзывы: 0\n"
        "└ Регистрация: от 01-01-2025"
    )

def filters_kb(market=None):
    market = market or state.get("current_market") or "vinted"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌐", callback_data=f"noop_{market}_countries"),
         InlineKeyboardButton("▣", callback_data=f"noop_{market}_categories"),
         InlineKeyboardButton("▣ Цена", callback_data=f"price_{market}"),
         InlineKeyboardButton("◷ Публикации", callback_data=f"age_{market}"),
         InlineKeyboardButton("⊘", callback_data=f"noop_{market}_banwords"),
         InlineKeyboardButton("☮", callback_data=f"noop_{market}_seller")],
        [InlineKeyboardButton("⏹ Остановить" if _market_running(market) else "▶ Запустить", callback_data=f"toggle_{market}")],
        [InlineKeyboardButton("ⓘ Фильтры", callback_data=f"filters_{market}"),
         InlineKeyboardButton(f"ⓘ {_market_title(market)}", callback_data=f"pick_{market}")],
        [InlineKeyboardButton("↻ Сменить площадку", callback_data="back")],
    ])

def brands_kb(page=0):
    per_page = 5
    start = page * per_page
    chunk = ALL_BRANDS[start:start + per_page]
    rows = []
    for brand in chunk:
        active = brand in state["active_brands"]
        icon = "✅" if active else "☐"
        rows.append([InlineKeyboardButton(f"{icon} {brand.title()}", callback_data=f"brand_{brand}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("‹", callback_data=f"brands_{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{(len(ALL_BRANDS)-1)//per_page+1}", callback_data="noop_page"))
    if start + per_page < len(ALL_BRANDS):
        nav.append(InlineKeyboardButton("›", callback_data=f"brands_{page+1}"))
    rows.append(nav)
    rows.append([
        InlineKeyboardButton("✅ Все", callback_data="brands_all"),
        InlineKeyboardButton("☐ Снять все", callback_data="brands_none"),
    ])
    rows.append([InlineKeyboardButton("↻ Назад", callback_data=f"pick_{state.get('current_market') or 'vinted'}")])
    return InlineKeyboardMarkup(rows)

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    state["current_market"] = None
    await update.message.reply_text(main_text(), reply_markup=main_kb(), parse_mode="HTML")

async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    state["chat_id"] = q.message.chat_id
    data = q.data

    async def edit(text, kb=None):
        try:
            await q.edit_message_text(text, reply_markup=kb or main_kb(), parse_mode="HTML")
        except Exception:
            await q.message.reply_text(text, reply_markup=kb or main_kb(), parse_mode="HTML")

    if data in ("back", "main"):
        state["current_market"] = None
        await edit(main_text(), main_kb())
        return

    if data in ("pick_vinted", "pick_mercari"):
        market = data.split("_", 1)[1]
        state["current_market"] = market
        await edit(market_text(market), market_kb(market))
        return

    if data in ("toggle_vinted", "toggle_mercari"):
        market = data.split("_", 1)[1]
        state["current_market"] = market
        if market == "vinted":
            if state["vinted_running"]:
                state["vinted_running"] = False
            else:
                if not state["active_brands"]:
                    await q.answer("Выбери хотя бы один бренд", show_alert=True)
                    return
                state["vinted_running"] = True
                threading.Thread(target=vinted_loop, daemon=True).start()
        else:
            if state["mercari_running"]:
                state["mercari_running"] = False
            else:
                if not state["active_brands"]:
                    await q.answer("Выбери хотя бы один бренд", show_alert=True)
                    return
                state["mercari_running"] = True
                threading.Thread(target=mercari_loop, daemon=True).start()
        await edit(market_text(market), market_kb(market))
        return

    if data in ("filters_vinted", "filters_mercari", "vinted_settings", "mercari_settings"):
        market = "mercari" if "mercari" in data else "vinted"
        state["current_market"] = market
        await edit(filters_text(market), filters_kb(market))
        return

    if data in ("price_vinted", "vset_min"):
        state["awaiting"] = "vinted_min"
        state["current_market"] = "vinted"
        await edit(f"Введи минимальную цену Vinted (€)\nСейчас: <b>{state['vinted_min']}€</b>\n\nНапример: <code>10</code>", filters_kb("vinted"))
        return
    if data == "vset_max":
        state["awaiting"] = "vinted_max"
        state["current_market"] = "vinted"
        await edit(f"Введи максимальную цену Vinted (€)\nСейчас: <b>{state['vinted_max']}€</b>\n\nНапример: <code>500</code>", filters_kb("vinted"))
        return
    if data == "price_mercari":
        state["awaiting"] = "mercari_min"
        state["current_market"] = "mercari"
        await edit(f"Введи минимальную цену Mercari (¥)\nСейчас: <b>{state['mercari_min']:,}¥</b>\n\nНапример: <code>1000</code>", filters_kb("mercari"))
        return
    if data == "mset_max":
        state["awaiting"] = "mercari_max"
        state["current_market"] = "mercari"
        await edit(f"Введи максимальную цену Mercari (¥)\nСейчас: <b>{state['mercari_max']:,}¥</b>\n\nНапример: <code>50000</code>", filters_kb("mercari"))
        return
    if data in ("age_vinted", "vset_age"):
        state["awaiting"] = "vinted_age_range"
        state["current_market"] = "vinted"
        await edit(
            "Введи диапазон публикации Vinted в часах\n"
            f"Сейчас: <b>{age_range_label(state['vinted_min_age_hours'], state['vinted_max_age_hours'])}</b>\n\n"
            "Например: <code>24</code> или <code>6-48</code>",
            filters_kb("vinted")
        )
        return
    if data == "age_mercari":
        state["awaiting"] = "mercari_age_range"
        state["current_market"] = "mercari"
        await edit(
            "Введи диапазон публикации Mercari в часах\n"
            f"Сейчас: <b>{age_range_label(state['mercari_min_age_hours'], state['mercari_max_age_hours'])}</b>\n\n"
            "Например: <code>24</code> или <code>6-48</code>",
            filters_kb("mercari")
        )
        return

    if data.startswith("noop_"):
        await q.answer("Этот фильтр пока отображается как в шаблоне", show_alert=True)
        return

    if data == "status":
        tf = state.get("_vinted_ts_field") or "не определено"
        text = (
            "<b>Статус</b>\n\n"
            f"<b>Vinted</b> {'🟢' if state['vinted_running'] else '🔴'}\n"
            f"└ Циклов: {state['vinted_stats']['cycles']} | Находок: {state['vinted_stats']['found']}\n"
            f"└ Цена: {state['vinted_min']}–{state['vinted_max']}€ | {age_range_label(state['vinted_min_age_hours'], state['vinted_max_age_hours'])}\n"
            f"└ Поле времени: <code>{tf}</code>\n\n"
            f"<b>Mercari.jp</b> {'🟢' if state['mercari_running'] else '🔴'}\n"
            f"└ Циклов: {state['mercari_stats']['cycles']} | Находок: {state['mercari_stats']['found']}\n"
            f"└ Цена: {state['mercari_min']:,}–{state['mercari_max']:,}¥ | {age_range_label(state['mercari_min_age_hours'], state['mercari_max_age_hours'])}\n\n"
            f"Брендов: {len(state['active_brands'])}/{len(ALL_BRANDS)}"
        )
        await edit(text, main_kb())
        return

    if data.startswith("brands_") and data not in ("brands_all", "brands_none"):
        try:
            page = int(data.split("_")[1])
        except (IndexError, ValueError):
            page = 0
        await edit(
            f"<b>Бренды</b>\n\nАктивны: {len(state['active_brands'])}/{len(ALL_BRANDS)}\nСтраница {page+1}/{(len(ALL_BRANDS)-1)//5+1}",
            brands_kb(page)
        )
        return
    if data == "brands_all":
        state["active_brands"] = set(ALL_BRANDS)
        await edit(f"<b>Бренды</b>\n\nВсе {len(ALL_BRANDS)} брендов активны.", brands_kb(0))
        return
    if data == "brands_none":
        state["active_brands"] = set()
        await edit("<b>Бренды</b>\n\nВсе бренды отключены.", brands_kb(0))
        return
    if data.startswith("brand_"):
        brand = data[6:]
        if brand in state["active_brands"]:
            state["active_brands"].discard(brand)
        else:
            state["active_brands"].add(brand)
        page = next((i // 5 for i, b in enumerate(ALL_BRANDS) if b == brand), 0)
        await edit(f"<b>Бренды</b>\n\nАктивны: {len(state['active_brands'])}/{len(ALL_BRANDS)}", brands_kb(page))
        return

async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    aw = state.get("awaiting")
    text = update.message.text.strip().replace(",", ".")
    mapping = {
        "vinted_min": ("vinted_min", "€", False, "vinted_max"),
        "vinted_max": ("vinted_max", "€", False, None),
        "mercari_min": ("mercari_min", "¥", True, "mercari_max"),
        "mercari_max": ("mercari_max", "¥", True, None),
    }
    if aw in mapping:
        key, symbol, is_int, next_key = mapping[aw]
        try:
            val = int(float(text)) if is_int else float(text)
            if val <= 0:
                raise ValueError
            state[key] = val
            if next_key:
                state["awaiting"] = next_key
                market = "mercari" if key.startswith("mercari") else "vinted"
                max_key = "mercari_max" if market == "mercari" else "vinted_max"
                await update.message.reply_text(
                    f"Ок, минимум: <b>{val:,}{symbol}</b>\nТеперь введи максимум.\nСейчас: <b>{state[max_key]:,}{symbol}</b>",
                    parse_mode="HTML", reply_markup=filters_kb(market)
                )
                return
            state["awaiting"] = None
            market = "mercari" if key.startswith("mercari") else "vinted"
            await update.message.reply_text(
                f"✅ Установлено: <b>{val:,}{symbol}</b>\n\n{filters_text(market)}",
                parse_mode="HTML", reply_markup=filters_kb(market)
            )
        except ValueError:
            await update.message.reply_text("Нужно число больше 0", reply_markup=filters_kb(state.get("current_market")))
    elif aw in ("vinted_age", "vinted_age_range", "mercari_age_range"):
        market = "mercari" if aw == "mercari_age_range" else "vinted"
        try:
            min_age, max_age = parse_age_range(text)
            state[f"{market}_min_age_hours"] = min_age
            state[f"{market}_max_age_hours"] = max_age
            state["awaiting"] = None
            state["current_market"] = market
            label = age_range_label(min_age, max_age)
            await update.message.reply_text(
                f"✅ Период публикации: <b>{label}</b>\n\n{filters_text(market)}",
                parse_mode="HTML", reply_markup=filters_kb(market)
            )
        except ValueError:
            await update.message.reply_text(
                "Нужно число часов или диапазон. Например: 24 или 6-48",
                reply_markup=filters_kb(market)
            )
    else:
        await update.message.reply_text(main_text(), reply_markup=main_kb(), parse_mode="HTML")

async def setup_bot_commands(app):
    await app.bot.set_my_commands([
        BotCommand("start", "🤖 Главное меню"),
    ])

def main():
    global bot_app
    if not BOT_TOKEN:
        print("BOT_TOKEN не задан!")
        time.sleep(300)
        return
    log.info(f"Запуск | брендов: {len(ALL_BRANDS)}")
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
    bot_app = (
        builder
        .connect_timeout(30).read_timeout(30)
        .write_timeout(30).pool_timeout(30)
        .post_init(setup_bot_commands)
        .build()
    )
    bot_app.add_handler(CommandHandler("start", cmd_start))
    bot_app.add_handler(CallbackQueryHandler(on_button))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    bot_app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True, timeout=30)

if __name__ == "__main__":
    import asyncio
    while True:
        try:
            asyncio.set_event_loop(asyncio.new_event_loop())
            main()
        except Exception as e:
            log.error(f"Бот упал: {e}. Перезапуск через 15с...")
            time.sleep(15)
