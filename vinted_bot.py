#!/usr/bin/env python3
import logging, time, threading, os, random, requests, json as _json, gzip, re
from datetime import datetime, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
    "brands_page": 0,
    "active_brands": set(ALL_BRANDS),
    "vinted_running": False,
    "vinted_min": 10,
    "vinted_max": 500,
    "vinted_max_age_hours": MAX_AGE_HOURS,
    "vinted_interval": 300,
    "vinted_seen": set(),
    "vinted_stats": {"found": 0, "cycles": 0},
    "_vinted_ts_field": None,
    "_vinted_debug_done": False,
    "mercari_running": False,
    "mercari_min": 1000,
    "mercari_max": 50000,
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
    if isinstance(val, (int, float)):
        ts = float(val)
        if 1577836800 < ts < 1893456000:
            return ts
        return None
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        try:
            ts = float(val)
            if 1577836800 < ts < 1893456000:
                return ts
        except ValueError:
            pass
        try:
            v  = val.replace(" UTC", "+00:00").replace(" ", "T")
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

def parse_vinted_ts(item) -> float | None:
    cached = state.get("_vinted_ts_field")
    if cached and cached in item:
        ts = _try_parse_ts(item[cached])
        if ts:
            return ts
    candidates = [
        "created_at_ts","updated_at_ts","activation_ts",
        "created_at","updated_at","active_at","last_push_up_at",
    ]
    for key in candidates:
        val = item.get(key)
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
    if any(w in title for w in BAD_WORDS):
        return False
    ts = parse_vinted_ts(item)
    if ts is not None:
        age_hours = (time.time() - ts) / 3600
        if age_hours > state["vinted_max_age_hours"]:
            log.info(f"SKIP old {age_hours:.1f}h: {item.get('title','?')[:40]}")
            return False
    else:
        log.warning(f"NO_TS id={item.get('id')} '{item.get('title','?')[:40]}'")
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
                    if not (state["vinted_min"] <= price <= state["vinted_max"]): continue

                    pd      = item.get("price", {})
                    title   = item.get("title", "?")
                    curr    = pd.get("currency_code", "EUR")
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
                    msg = "\n".join(lines)

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
                                        parse_mode="HTML", disable_web_page_preview=False,
                                    )
                                )
                        else:
                            loop.run_until_complete(
                                bot_app.bot.send_message(
                                    chat_id=state["chat_id"], text=msg,
                                    parse_mode="HTML", disable_web_page_preview=False,
                                )
                            )
                time.sleep(random.uniform(10, 18))
            time.sleep(random.uniform(12, 25))

        if state["vinted_running"]:
            time.sleep(state["vinted_interval"])
    loop.close()

def fetch_mercari(query):
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
            items = fetch_mercari(brand)
            for item in (items or []):
                iid = item.get("id")
                if iid in state["mercari_seen"]: continue
                state["mercari_seen"].add(iid)

                name  = item.get("name", "?")
                price = item.get("price", 0)
                try: price = int(price)
                except (ValueError, TypeError): continue
                if not (state["mercari_min"] <= price <= state["mercari_max"]): continue

                name_l = name.lower()
                word   = brand.split()[0]
                if word not in name_l: continue

                thumbs    = item.get("thumbnails") or item.get("item_images") or []
                thumb     = (thumbs[0].get("url") or thumbs[0].get("image_url", "")) if thumbs else ""
                iid2      = item.get("id", "")
                link      = f"https://jp.mercari.com/item/{iid2}"
                name_ru   = translate_to_ru(name)
                rate      = get_jpy_to_eur()
                eur       = round(price * rate, 2) if rate else None
                if eur:
                    market_eur = round(eur * 1.3, 0)
                    price_str  = f"¥{price:,} = {eur:.0f}€ / рынок от {market_eur:.0f}€"
                else:
                    price_str = f"¥{price:,}"

                lines = [
                    "🔔 <b>Новый товар!</b>",
                    f"🧥 Mercari 🇯🇵 • {brand.lower()} меркари",
                    "",
                    name_ru,
                    f"💰 {price_str}",
                    f"<a href='{link}'>Открыть</a>",
                ]
                msg = "\n".join(lines)
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
                        except Exception:
                            loop.run_until_complete(
                                bot_app.bot.send_message(
                                    chat_id=state["chat_id"], text=msg,
                                    parse_mode="HTML", disable_web_page_preview=False,
                                )
                            )
                    else:
                        loop.run_until_complete(
                            bot_app.bot.send_message(
                                chat_id=state["chat_id"], text=msg,
                                parse_mode="HTML", disable_web_page_preview=False,
                            )
                        )

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

def main():
    global bot_app
    if not BOT_TOKEN:
        print("BOT_TOKEN не задан!")
        return
    log.info(f"Запуск | брендов: {len(ALL_BRANDS)}")
    builder = Application.builder().token(BOT_TOKEN)
    if PROXY_URL:
        builder = builder.proxy(PROXY_URL).get_updates_proxy(PROXY_URL)
    bot_app = (
        builder
        .connect_timeout(30).read_timeout(30)
        .write_timeout(30).pool_timeout(30)
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
