#!/usr/bin/env python3
import logging, time, threading, os, random, requests, json as _json, gzip, re, html
from datetime import datetime, timezone
from collections import deque
import pytz
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
PROXY_URL  = os.environ.get("PROXY_URL", "")

VINTED_REGIONS = {
    "pl": "www.vinted.pl",
    "lt": "www.vinted.lt",
}
CATALOG_IDS = [1, 3, 5, 9, 7, 12]
MAX_AGE_HOURS = 24

# ОПТИМИЗАЦИЯ: Лимиты для предотвращения утечки памяти
MAX_SEEN_ITEMS = 5000
TRANSLATE_TIMEOUT = 5
VINTED_REQUEST_TIMEOUT = 15
MERCARI_REQUEST_TIMEOUT = 15

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

FAKE_WORDS = [
    "replica","fake","ua","1:1","copy","inspired",
    "budget version","reworked","custom","bootleg",
]

LUXURY_KEYWORDS = [
    "archive","runway","vintage","grail","rare",
    "fw","aw","ss","collection",
]

BRAND_ALIASES = {
    "stone island": ["stone island","stoneisland","stoney","ストーンアイランド"],
    "comme des garcons": ["comme des garcons","comme des garçons","cdg","play cdg","コムデギャルソン"],
    "raf simons": ["raf simons","rafsimons","ラフシモンズ"],
    "balenciaga": ["balenciaga","balen","バレンシアガ"],
    "undercover": ["undercover","under cover","アンダーカバー"],
}

BLACKLIST_SELLERS = []

def normalize_text(text):
    text = str(text).lower()
    text = text.replace("-", " ").replace("/", " ")
    text = re.sub(r"[^a-z0-9а-яё ]", " ", text)
    return " ".join(text.split())

def brand_match(text, brand):
    text = normalize_text(text)
    aliases = BRAND_ALIASES.get(brand.lower(), [brand.lower()])
    for alias in aliases:
        if normalize_text(alias) in text:
            return True
    return False

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_eur_rate_cache = {"rate": None, "ts": 0}

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

def translate_to_ru(text: str) -> str:
    """Перевод с таймаутом и проверкой на русский язык"""
    if not text or not text.strip():
        return text
    
    try:
        cyr = sum(1 for c in text if '\u0400' <= c <= '\u04FF')
        if cyr / max(len(text), 1) > 0.4:
            return text
    except:
        return text
    
    try:
        r = requests.get(
            "https://translate.googleapis.com/translate_a/single",
            params={"client": "gtx", "sl": "auto", "tl": "ru", "dt": "t", "q": text[:200]},
            timeout=TRANSLATE_TIMEOUT,
        )
        if r.status_code == 200:
            data = r.json()
            return "".join(part[0] for part in data[0] if part[0]).strip() or text
    except requests.exceptions.Timeout:
        log.warning(f"Перевод заблокирован (таймаут)")
        return text
    except Exception:
        pass
    
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
    "vinted_max_age_hours": MAX_AGE_HOURS,
    "vinted_interval": 600,  # Увеличено с 300 для одной машины
    "vinted_seen": deque(maxlen=MAX_SEEN_ITEMS),  # Автоматический лимит памяти
    "vinted_stats": {"found": 0, "cycles": 0},
    "_vinted_ts_field": None,
    "_vinted_debug_done": False,
    "mercari_running": False,
    "mercari_min": 1000,
    "mercari_max": 50000,
    "mercari_interval": 600,  # Увеличено с 300 для одной машины
    "mercari_seen": deque(maxlen=MAX_SEEN_ITEMS),  # Автоматический лимит памяти
    "mercari_stats": {"found": 0, "cycles": 0},
}

bot_app = None
USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]
vinted_sessions: dict = {}
mercari_api = None

MSK_TZ = pytz.timezone("Europe/Moscow")

def format_msk_time(ts):
    if not ts:
        return "только что"
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(MSK_TZ)
        return dt.strftime("%d-%m-%Y %H:%M МСК")
    except Exception:
        return "только что"

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
        s.get(f"https://{domain}/", timeout=VINTED_REQUEST_TIMEOUT)
        s.get(f"https://{domain}/catalog", timeout=VINTED_REQUEST_TIMEOUT)
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

def fetch_vinted(query, domain):
    s = get_vinted_session(domain)
    s.headers["User-Agent"] = random.choice(USER_AGENTS)
    try:
        params = [
            ("search_text", query), ("page", 1), ("per_page", 48),
            ("order", "newest_first"),
            ("price_from", state["vinted_min"]),
            ("price_to",   state["vinted_max"]),
            ("currency", "EUR"),
        ]
        for cid in CATALOG_IDS:
            params.append(("catalog_ids[]", cid))
        
        r = s.get(f"https://{domain}/api/v2/catalog/items", params=params, timeout=VINTED_REQUEST_TIMEOUT)
        
        if r.status_code == 200:
            items = decode_response(r).get("items", [])
            if items:
                log.info(f"Vinted {domain} -> {len(items)} товаров")
                if not state["_vinted_debug_done"]:
                    item0 = items[0]
                    log.info(f"DEBUG keys: {list(item0.keys())[:5]}")
                    state["_vinted_debug_done"] = True
            return items
        elif r.status_code in (403, 429):
            log.error(f"Vinted BAN {r.status_code} {domain}")
            vinted_sessions.pop(domain, None)
            return "BAN"
        return []
    except requests.exceptions.Timeout:
        log.warning(f"fetch_vinted {domain}: таймаут")
        return []
    except Exception as e:
        log.warning(f"fetch_vinted {domain}: {e}")
        return []

def _try_parse_ts(val) -> float | None:
    if val is None:
        return None
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
    ]
    for key in candidates:
        val = _get_nested(item, key)
        if val is None:
            continue
        ts = _try_parse_ts(val)
        if ts:
            if state.get("_vinted_ts_field") != key:
                state["_vinted_ts_field"] = key
                log.info(f"Поле времени Vinted: '{key}'")
            return ts
    return None

def is_relevant(item, brand):
    title = item.get("title", "").lower()
    brand2 = item.get("brand_title", "").lower()
    full_text = f"{title} {brand2}"

    if not brand_match(full_text, brand):
        return False

    if any(w in title for w in BAD_WORDS):
        return False

    if any(w in title for w in FAKE_WORDS):
        return False

    seller = item.get("user", {}) or {}
    seller_name = str(seller.get("login") or seller.get("username") or "").lower()

    if seller_name in BLACKLIST_SELLERS:
        return False

    feedback_count = int(seller.get("feedback_count", 0) or 0)
    positive_feedback = int(seller.get("positive_feedback_count", 0) or 0)

    if feedback_count < 3 or positive_feedback < 2:
        return False

    score = 0
    for kw in LUXURY_KEYWORDS:
        if kw in title:
            score += 1

    if score < 1:
        return False

    ts = parse_vinted_ts(item)
    if ts is None:
        return False

    age_hours = (time.time() - ts) / 3600

    if age_hours < -1 or age_hours > state["vinted_max_age_hours"]:
        return False

    return True

def vinted_loop():
    """Основной цикл Vinted с защитой от крашей"""
    import asyncio
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    except:
        loop = None
    
    for domain in VINTED_REGIONS.values():
        init_vinted(domain)
        time.sleep(1)
    
    log.info("Vinted мониторинг запущен")

    while state["vinted_running"]:
        try:
            brands = list(state["active_brands"])
            random.shuffle(brands)
            state["vinted_stats"]["cycles"] += 1

            for brand in brands:
                if not state["vinted_running"]: 
                    break
                
                for _, domain in VINTED_REGIONS.items():
                    if not state["vinted_running"]: 
                        break
                    
                    try:
                        items = fetch_vinted(brand, domain)
                        if items == "BAN":
                            time.sleep(random.randint(60, 120))
                            continue
                        
                        for item in (items or []):
                            try:
                                iid = item.get("id")
                                if not iid or iid in state["vinted_seen"]: 
                                    continue
                                
                                state["vinted_seen"].append(iid)
                                
                                if not is_relevant(item, brand): 
                                    continue

                                price = float(item.get("price", {}).get("amount", 0))
                                if not (state["vinted_min"] <= price <= state["vinted_max"]): 
                                    continue

                                pd      = item.get("price", {})
                                title   = item.get("title", "?")
                                curr    = pd.get("currency_code", "EUR")
                                size    = item.get("size_title", "")
                                brand_t = item.get("brand_title", "")
                                cond    = item.get("status", "")
                                url     = item.get("url", "")
                                link    = f"https://{domain}{url}" if url.startswith("/") else url
                                
                                title_ru = translate_to_ru(title)

                                ts_d = parse_vinted_ts(item)
                                age_str = ""
                                if ts_d:
                                    age_min = (time.time() - ts_d) / 60
                                    age_str = f"{int(age_min)} мин. назад" if age_min < 60 else f"{age_min/60:.1f} ч. назад"

                                photos = item.get("photos") or item.get("photo") or []
                                if isinstance(photos, dict): 
                                    photos = [photos]
                                
                                photo_url = ""
                                if photos:
                                    p = photos[0]
                                    photo_url = (
                                        p.get("high_resolution", {}).get("url")
                                        or p.get("full_size_url")
                                        or p.get("image", {}).get("url")
                                        or p.get("url")
                                        or p.get("thumb_url", "")
                                    )
                                    if "images" in photo_url:
                                        photo_url = photo_url.replace("thumbs", "images")

                                msg = format_vinted_message(item, domain, title, title_ru, price, curr, link, photo_url, ts_d, brand_t, size, cond)

                                state["vinted_stats"]["found"] += 1
                                log.info(f"FOUND Vinted: {title} — {price}€")
                                
                                if state["chat_id"] and bot_app:
                                    try:
                                        if photo_url:
                                            loop.run_until_complete(
                                                bot_app.bot.send_photo(
                                                    chat_id=state["chat_id"], 
                                                    photo=photo_url,
                                                    caption=msg[:1024], 
                                                    parse_mode="HTML",
                                                )
                                            )
                                        else:
                                            loop.run_until_complete(
                                                bot_app.bot.send_message(
                                                    chat_id=state["chat_id"], 
                                                    text=msg[:4096],
                                                    parse_mode="HTML", 
                                                    disable_web_page_preview=False,
                                                )
                                            )
                                    except Exception as e:
                                        log.warning(f"Ошибка отправки Vinted: {e}")
                            
                            except Exception as e:
                                log.warning(f"Ошибка обработки товара Vinted: {e}")
                        
                        time.sleep(random.uniform(5, 10))
                    
                    except Exception as e:
                        log.warning(f"Ошибка домена {domain}: {e}")
                        time.sleep(5)
                
                time.sleep(random.uniform(10, 20))

            if state["vinted_running"]:
                time.sleep(state["vinted_interval"])
        
        except Exception as e:
            log.error(f"Критическая ошибка Vinted: {e}")
            time.sleep(30)
    
    if loop:
        loop.close()
    log.info("Vinted мониторинг остановлен")

def mercari_loop():
    """Основной цикл Mercari с защитой от крашей"""
    import asyncio
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    except:
        loop = None
    
    log.info("Mercari мониторинг запущен")

    while state["mercari_running"]:
        try:
            brands = list(state["active_brands"])
            random.shuffle(brands)
            state["mercari_stats"]["cycles"] += 1

            for brand in brands:
                if not state["mercari_running"]: 
                    break
                
                try:
                    # Mercari отключен для одной машины
                    # items = await fetch_mercari(brand)
                    items = []
                    
                    for item in (items or []):
                        try:
                            iid = item.get("id")
                            if not iid or iid in state["mercari_seen"]: 
                                continue

                            name = item.get("name", "?")
                            price = item.get("price", 0)
                            try: 
                                price = int(price)
                            except (ValueError, TypeError):
                                continue
                            
                            if not (state["mercari_min"] <= price <= state["mercari_max"]):
                                continue

                            state["mercari_seen"].append(iid)
                            state["mercari_stats"]["found"] += 1
                            log.info(f"FOUND Mercari: {name} — ¥{price}")
                        
                        except Exception as e:
                            log.warning(f"Ошибка обработки товара Mercari: {e}")

                    time.sleep(random.uniform(8, 15))
                
                except Exception as e:
                    log.warning(f"Ошибка бренда Mercari {brand}: {e}")
                    time.sleep(5)

            if state["mercari_running"]:
                time.sleep(state["mercari_interval"])
        
        except Exception as e:
            log.error(f"Критическая ошибка Mercari: {e}")
            time.sleep(30)
    
    if loop:
        loop.close()
    log.info("Mercari мониторинг остановлен")

def format_vinted_message(item, domain, title, title_ru, price, curr, link, photo_url, ts_d, brand_t, size, cond):
    seller = item.get("user", {}) or {}
    seller_name = html.escape(str(seller.get("login") or seller.get("username") or "не указан"))
    feedback_count = int(seller.get("feedback_count", 0) or 0)
    positive_feedback = int(seller.get("positive_feedback_count", 0) or 0)
    items_count = int(seller.get("item_count", 0) or 0)

    posted = format_msk_time(ts_d)
    details = [d for d in [brand_t, size, cond] if d]
    category = html.escape(" / ".join(details) if details else "Все")
    title_safe = html.escape(str(title_ru or title)[:100])
    link_safe = html.escape(str(link), quote=True)

    return (
        f"🧥 <b>Vinted</b>\n"
        f"Название: {title_safe}\n"
        f"Цена: {price:g} {curr}\n"
        f"Категория: {category}\n"
        f"Продавец: {seller_name}\n"
        f"Продажи: {positive_feedback} | Отзывы: {feedback_count}\n"
        f"Время: {posted}\n\n"
        f"<a href='{link_safe}'>Открыть объявление</a>"
    )

def format_mercari_message(item, name, name_ru, price, price_str, link, thumb):
    title_safe = html.escape(str(name_ru or name)[:100])
    link_safe = html.escape(str(link), quote=True)
    return (
        f"🧥 <b>Mercari.jp</b>\n"
        f"Название: {title_safe}\n"
        f"Цена: {price_str}\n\n"
        f"<a href='{link_safe}'>Открыть объявление</a>"
    )

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    vs = "🟢" if state["vinted_running"] else "🔴"
    text = (
        f"<b>Vinted Monitor</b>\n\n"
        f"{vs} Vinted: {state['vinted_min']}–{state['vinted_max']}€ | до {state['vinted_max_age_hours']}ч\n"
        f"Брендов: {len(state['active_brands'])} из {len(ALL_BRANDS)}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Запустить" if not state["vinted_running"] else "⏹ Остановить", callback_data="toggle_vinted")],
        [InlineKeyboardButton("👕 Бренды", callback_data="brands_0"),
         InlineKeyboardButton("⚙️ Настройки", callback_data="settings")],
        [InlineKeyboardButton("📊 Статус", callback_data="status")],
    ])
    await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")

async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    state["chat_id"] = q.message.chat_id
    data = q.data

    async def edit(text, kb=None):
        try:
            await q.edit_message_text(text, reply_markup=kb or InlineKeyboardMarkup([]), parse_mode="HTML")
        except Exception:
            pass

    if data == "toggle_vinted":
        if state["vinted_running"]:
            state["vinted_running"] = False
            await edit("Vinted остановлен ⏹")
        else:
            if not state["active_brands"]:
                await q.answer("Выбери хотя бы один бренд!", show_alert=True)
                return
            state["vinted_running"] = True
            threading.Thread(target=vinted_loop, daemon=True).start()
            await edit(f"Vinted запущен ▶️\nБрендов: {len(state['active_brands'])}")

    elif data == "settings":
        await edit(
            f"<b>Настройки</b>\n\n"
            f"Цена Vinted: {state['vinted_min']}–{state['vinted_max']}€\n"
            f"Возраст: до {state['vinted_max_age_hours']}ч",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("Мин цена", callback_data="vset_min"),
                 InlineKeyboardButton("Макс цена", callback_data="vset_max")],
                [InlineKeyboardButton("Возраст", callback_data="vset_age")],
                [InlineKeyboardButton("🔙 Назад", callback_data="back")],
            ])
        )
    
    elif data == "vset_min":
        state["awaiting"] = "vinted_min"
        await edit(f"Введи минимальную цену (€):\nСейчас: {state['vinted_min']}€")
    
    elif data == "vset_max":
        state["awaiting"] = "vinted_max"
        await edit(f"Введи максимальную цену (€):\nСейчас: {state['vinted_max']}€")
    
    elif data == "vset_age":
        state["awaiting"] = "vinted_age"
        await edit(f"Введи максимальный возраст (часы):\nСейчас: {state['vinted_max_age_hours']}ч")

    elif data == "status":
        vs = state["vinted_stats"]
        await edit(
            f"<b>Статус</b>\n\n"
            f"Vinted: {'🟢 Работает' if state['vinted_running'] else '🔴 Остановлен'}\n"
            f"Циклов: {vs['cycles']} | Находок: {vs['found']}\n"
            f"Цена: {state['vinted_min']}–{state['vinted_max']}€\n"
            f"Брендов: {len(state['active_brands'])}",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
        )

    elif data.startswith("brands_"):
        try:
            page = int(data.split("_")[1])
        except:
            page = 0
        
        per_page = 5
        start = page * per_page
        chunk = ALL_BRANDS[start:start + per_page]
        
        rows = []
        for brand in chunk:
            icon = "✅" if brand in state["active_brands"] else "☐"
            rows.append([InlineKeyboardButton(f"{icon} {brand.title()}", callback_data=f"brand_{brand}")])
        
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("◀️", callback_data=f"brands_{page-1}"))
        if start + per_page < len(ALL_BRANDS):
            nav.append(InlineKeyboardButton("▶️", callback_data=f"brands_{page+1}"))
        if nav:
            rows.append(nav)
        
        rows.append([
            InlineKeyboardButton("✅ Все", callback_data="brands_all"),
            InlineKeyboardButton("☐ Нет", callback_data="brands_none"),
        ])
        rows.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
        
        await edit(f"Бренды: {len(state['active_brands'])}/{len(ALL_BRANDS)}", InlineKeyboardMarkup(rows))

    elif data == "brands_all":
        state["active_brands"] = set(ALL_BRANDS)
        await edit(f"✅ Все {len(ALL_BRANDS)} брендов активны", InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="brands_0")]]))

    elif data == "brands_none":
        state["active_brands"] = set()
        await edit("☐ Все бренды отключены", InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="brands_0")]]))

    elif data.startswith("brand_"):
        brand = data[6:]
        if brand in state["active_brands"]:
            state["active_brands"].discard(brand)
        else:
            state["active_brands"].add(brand)
        page = next((i // 5 for i, b in enumerate(ALL_BRANDS) if b == brand), 0)
        await q.answer(f"Брендов: {len(state['active_brands'])}")

    elif data == "back":
        vs = "🟢" if state["vinted_running"] else "🔴"
        text = (
            f"<b>Vinted Monitor</b>\n\n"
            f"{vs} Статус: {'Работает' if state['vinted_running'] else 'Остановлен'}\n"
            f"Цена: {state['vinted_min']}–{state['vinted_max']}€\n"
            f"Брендов: {len(state['active_brands'])}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Запустить" if not state["vinted_running"] else "⏹ Остановить", callback_data="toggle_vinted")],
            [InlineKeyboardButton("👕 Бренды", callback_data="brands_0"),
             InlineKeyboardButton("⚙️ Настройки", callback_data="settings")],
            [InlineKeyboardButton("📊 Статус", callback_data="status")],
        ])
        await edit(text, kb)

async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    aw = state.get("awaiting")
    text = update.message.text.strip().replace(",", ".")
    
    mapping = {
        "vinted_min": ("vinted_min", "€", False),
        "vinted_max": ("vinted_max", "€", False),
        "vinted_age": ("vinted_max_age_hours", "ч", False),
    }
    
    if aw in mapping:
        key, symbol, is_int = mapping[aw]
        try:
            val = int(float(text)) if is_int else float(text)
            if val > 0:
                state[key] = val
                state["awaiting"] = None
                await update.message.reply_text(f"✅ Установлено: {val}{symbol}")
            else:
                await update.message.reply_text("❌ Число должно быть больше 0")
        except ValueError:
            await update.message.reply_text("❌ Введи число")
    else:
        await update.message.reply_text("Используй /start")

async def setup_bot_commands(app):
    await app.bot.set_my_commands([
        BotCommand("start", "Главное меню"),
    ])

def main():
    global bot_app
    if not BOT_TOKEN:
        log.error("BOT_TOKEN не задан!")
        time.sleep(300)
        return
    
    log.info(f"Запуск | Брендов: {len(ALL_BRANDS)} | Таймауты: {TRANSLATE_TIMEOUT}s, {VINTED_REQUEST_TIMEOUT}s")
    
    builder = Application.builder().token(BOT_TOKEN)
    if PROXY_URL:
        if hasattr(builder, "proxy_url"):
            builder = builder.proxy_url(PROXY_URL)
    
    bot_app = (
        builder
        .connect_timeout(20).read_timeout(20)
        .write_timeout(20).pool_timeout(20)
        .post_init(setup_bot_commands)
        .build()
    )
    
    bot_app.add_handler(CommandHandler("start", cmd_start))
    bot_app.add_handler(CallbackQueryHandler(on_button))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    
    bot_app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True, timeout=20)

if __name__ == "__main__":
    max_restarts = 5
    restart_count = 0
    
    while restart_count < max_restarts:
        try:
            main()
        except KeyboardInterrupt:
            log.info("Завершение...")
            break
        except Exception as e:
            restart_count += 1
            log.error(f"Ошибка (попытка {restart_count}/{max_restarts}): {e}")
            if restart_count < max_restarts:
                time.sleep(20)
            else:
                log.error("Превышено максимальное количество перезапусков")
