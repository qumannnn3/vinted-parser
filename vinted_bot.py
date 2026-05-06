#!/usr/bin/env python3
import logging, time, threading, os, random, requests, json as _json, gzip
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
PROXY_URL  = os.environ.get("PROXY_URL", "")

TARGET_REGIONS = {
    "pl": "www.vinted.pl",
    "lt": "www.vinted.lt",
    "lv": "www.vinted.lv",
}

# Категории: одежда муж/жен, обувь муж/жен, аксессуары муж/жен
CATALOG_IDS = [1, 3, 5, 9, 7, 12]

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

ALL_BRANDS = [
    "stone island", "balenciaga", "raf simons", "bape", "aape",
    "gucci", "chanel", "jeremy scott", "undercover", "comme des garcons",
    "yohji yamamoto", "vetements", "palm angels", "maison margiela",
    "givenchy", "burberry", "supreme", "amiri", "acne studios", "alyx",
]

state = {
    "running": False,
    "active_brands": set(ALL_BRANDS),  # активные бренды
    "min_price": 10,
    "max_price": 500,
    "interval": 300,
    "chat_id": None,
    "seen_ids": set(),
    "stats": {"found": 0, "cycles": 0},
    "awaiting": None,      # "min" / "max"
    "brands_page": 0,      # страница списка брендов
}

bot_app = None

USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

sessions: dict = {}

# ── VINTED ────────────────────────────────────────────────────────────────

def make_session(domain):
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


def init_session(domain):
    s = make_session(domain)
    try:
        s.get(f"https://{domain}/", timeout=15)
        s.get(f"https://{domain}/catalog", timeout=15)
        log.info(f"Сессия {domain}: OK")
    except Exception as e:
        log.warning(f"init_session {domain}: {e}")
    sessions[domain] = s
    return s


def get_session(domain):
    return sessions.get(domain) or init_session(domain)


def decode_response(r):
    encoding = r.headers.get("content-encoding", "").lower()
    content = r.content
    try:
        if encoding == "br":
            try:
                import brotli
                content = brotli.decompress(content)
            except ImportError:
                pass
        elif encoding == "gzip":
            content = gzip.decompress(content)
        return _json.loads(content)
    except Exception:
        try:
            return r.json()
        except Exception:
            return {}


def fetch_items(query, domain):
    s = get_session(domain)
    s.headers["User-Agent"] = random.choice(USER_AGENTS)
    try:
        params = [
            ("search_text", query),
            ("page", 1),
            ("per_page", 48),
            ("order", "newest_first"),
            ("price_from", state["min_price"]),
            ("price_to", state["max_price"]),
            ("currency", "EUR"),
        ]
        for cid in CATALOG_IDS:
            params.append(("catalog_ids[]", cid))

        r = s.get(
            f"https://{domain}/api/v2/catalog/items",
            params=params,
            timeout=20,
        )
        if r.status_code == 200:
            data = decode_response(r)
            items = data.get("items", [])
            if items:
                log.info(f"{domain} → {len(items)} товаров")
            return items
        elif r.status_code in (403, 429):
            log.error(f"❌ {r.status_code} на {domain}")
            sessions.pop(domain, None)
            return "BAN"
        else:
            log.warning(f"{domain} → {r.status_code}")
            return []
    except Exception as e:
        log.warning(f"fetch {domain}: {e}")
        return []


def is_relevant(item, brand):
    title      = item.get("title", "").lower()
    brand_name = item.get("brand_title", "").lower()
    first_word = brand.split()[0]
    return (first_word in title or first_word in brand_name) and not any(w in title for w in BAD_WORDS)


def format_msg(item, domain):
    title   = item.get("title", "Без названия")
    pd      = item.get("price", {})
    price   = pd.get("amount", "?")
    curr    = pd.get("currency_code", "")
    size    = item.get("size_title", "")
    brand_t = item.get("brand_title", "")
    cond    = item.get("status", "")
    url     = item.get("url", "")
    link    = f"https://{domain}{url}" if url.startswith("/") else url
    lines   = [f"🏷 <b>{brand_t.upper() if brand_t else title}</b>", f"📦 {title}"]
    if size: lines.append(f"📏 Размер: {size}")
    if cond: lines.append(f"✨ Состояние: {cond}")
    lines  += [f"💰 <b>{price} {curr}</b>", f"🔗 <a href='{link}'>СМОТРЕТЬ НА VINTED</a>"]
    return "\n".join(lines)


# ── МОНИТОРИНГ ────────────────────────────────────────────────────────────

def monitor_loop():
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for domain in TARGET_REGIONS.values():
        init_session(domain)
        time.sleep(3)
    log.info("Мониторинг запущен")

    while state["running"]:
        brands = list(state["active_brands"])
        random.shuffle(brands)
        state["stats"]["cycles"] += 1
        log.info(f"Цикл #{state['stats']['cycles']} | {len(brands)} брендов | {state['min_price']}–{state['max_price']} EUR")

        for brand in brands:
            if not state["running"]: break
            for region, domain in TARGET_REGIONS.items():
                if not state["running"]: break
                log.info(f"🔍 {brand} / {domain}")
                items = fetch_items(brand, domain)
                if items == "BAN":
                    time.sleep(random.randint(60, 120))
                    continue
                for item in (items or []):
                    iid = item.get("id")
                    if iid in state["seen_ids"]: continue
                    state["seen_ids"].add(iid)
                    if not is_relevant(item, brand): continue
                    try:
                        price = float(item.get("price", {}).get("amount", 0))
                    except (ValueError, TypeError):
                        continue
                    if not (state["min_price"] <= price <= state["max_price"]): continue
                    msg = format_msg(item, domain)
                    state["stats"]["found"] += 1
                    log.info(f"✅ {item.get('title')} — {price}")
                    if state["chat_id"] and bot_app:
                        loop.run_until_complete(
                            bot_app.bot.send_message(
                                chat_id=state["chat_id"], text=msg,
                                parse_mode="HTML", disable_web_page_preview=False,
                            )
                        )
                time.sleep(random.uniform(12, 22))
            time.sleep(random.uniform(15, 30))

        if state["running"]:
            time.sleep(state["interval"])
    loop.close()


# ── КЛАВИАТУРЫ ────────────────────────────────────────────────────────────

def main_kb():
    toggle = "⏹ Остановить" if state["running"] else "▶️ Запустить"
    status = "🟢" if state["running"] else "🔴"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{status} {toggle}", callback_data="toggle")],
        [InlineKeyboardButton(f"💶 Мин: {state['min_price']}€", callback_data="set_min"),
         InlineKeyboardButton(f"💶 Макс: {state['max_price']}€", callback_data="set_max")],
        [InlineKeyboardButton("👕 Бренды", callback_data="brands_0"),
         InlineKeyboardButton("📊 Статус", callback_data="status")],
    ])


def brands_kb(page=0):
    """Клавиатура выбора брендов — по 5 на странице."""
    per_page = 5
    start = page * per_page
    chunk = ALL_BRANDS[start:start + per_page]
    rows = []
    for brand in chunk:
        active = brand in state["active_brands"]
        icon   = "✅" if active else "☐"
        rows.append([InlineKeyboardButton(
            f"{icon} {brand.title()}",
            callback_data=f"brand_{brand}"
        )])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Назад", callback_data=f"brands_{page-1}"))
    if start + per_page < len(ALL_BRANDS):
        nav.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"brands_{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([
        InlineKeyboardButton("✅ Все", callback_data="brands_all"),
        InlineKeyboardButton("☐ Снять все", callback_data="brands_none"),
    ])
    rows.append([InlineKeyboardButton("🔙 Главное меню", callback_data="back")])
    return InlineKeyboardMarkup(rows)


def brands_text(page=0):
    per_page = 5
    start    = page * per_page
    total    = len(ALL_BRANDS)
    active   = len(state["active_brands"])
    return (
        f"<b>👕 Выбор брендов</b>\n\n"
        f"Активных: {active} из {total}\n"
        f"Страница {page+1}/{(total-1)//per_page+1}\n\n"
        f"Нажми на бренд чтобы включить/выключить:"
    )


# ── ОБРАБОТЧИКИ ───────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    active = len(state["active_brands"])
    st = "🟢 работает" if state["running"] else "🔴 остановлен"
    text = (
        f"<b>🛍 Vinted Monitor</b>\n\n"
        f"Статус: {st}\n"
        f"Активных брендов: {active}\n"
        f"Домены: .pl .lt .lv\n"
        f"Цена: {state['min_price']}–{state['max_price']} EUR\n\n"
        f"Ищет только одежду, обувь и аксессуары."
    )
    await update.message.reply_text(text, reply_markup=main_kb(), parse_mode="HTML")


async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    state["chat_id"] = q.message.chat_id
    data = q.data

    # ── Старт/стоп ──
    if data == "toggle":
        if state["running"]:
            state["running"] = False
            try: await q.edit_message_text("⏹ Мониторинг остановлен.", reply_markup=main_kb())
            except Exception: pass
        else:
            if not state["active_brands"]:
                await q.answer("⚠️ Выбери хотя бы один бренд!", show_alert=True)
                return
            state["running"] = True
            threading.Thread(target=monitor_loop, daemon=True).start()
            try:
                await q.edit_message_text(
                    f"▶️ <b>Мониторинг запущен!</b>\n\n"
                    f"Брендов: {len(state['active_brands'])}\n"
                    f"Цена: {state['min_price']}–{state['max_price']} EUR\n"
                    f"Домены: .pl .lt .lv",
                    reply_markup=main_kb(), parse_mode="HTML"
                )
            except Exception: pass

    # ── Цена ──
    elif data == "set_min":
        state["awaiting"] = "min"
        try:
            await q.edit_message_text(
                f"✏️ Введи <b>минимальную</b> цену в евро\n"
                f"Сейчас: <b>{state['min_price']}€</b>\n\n"
                f"Например: <code>10</code>",
                parse_mode="HTML"
            )
        except Exception: pass

    elif data == "set_max":
        state["awaiting"] = "max"
        try:
            await q.edit_message_text(
                f"✏️ Введи <b>максимальную</b> цену в евро\n"
                f"Сейчас: <b>{state['max_price']}€</b>\n\n"
                f"Например: <code>500</code>",
                parse_mode="HTML"
            )
        except Exception: pass

    # ── Статус ──
    elif data == "status":
        st    = state["stats"]
        status = "🟢 работает" if state["running"] else "🔴 остановлен"
        brands_list = ", ".join(b.title() for b in sorted(state["active_brands"])) or "нет"
        try:
            await q.edit_message_text(
                f"<b>📊 Статус</b>\n\n"
                f"{status}\n"
                f"Циклов: {st['cycles']}\n"
                f"Находок: {st['found']}\n"
                f"Цена: {state['min_price']}–{state['max_price']} EUR\n"
                f"Активных брендов: {len(state['active_brands'])}\n\n"
                f"<b>Бренды:</b>\n{brands_list}",
                reply_markup=main_kb(), parse_mode="HTML"
            )
        except Exception: pass

    # ── Список брендов ──
    elif data.startswith("brands_") and not data.startswith("brands_all") and not data.startswith("brands_none"):
        try:
            page = int(data.split("_")[1])
        except (IndexError, ValueError):
            page = 0
        try:
            await q.edit_message_text(brands_text(page), reply_markup=brands_kb(page), parse_mode="HTML")
        except Exception: pass

    elif data == "brands_all":
        state["active_brands"] = set(ALL_BRANDS)
        page = state.get("brands_page", 0)
        try:
            await q.edit_message_text(brands_text(page), reply_markup=brands_kb(page), parse_mode="HTML")
        except Exception: pass

    elif data == "brands_none":
        state["active_brands"] = set()
        page = state.get("brands_page", 0)
        try:
            await q.edit_message_text(brands_text(page), reply_markup=brands_kb(page), parse_mode="HTML")
        except Exception: pass

    # ── Переключить бренд ──
    elif data.startswith("brand_"):
        brand = data[6:]
        if brand in state["active_brands"]:
            state["active_brands"].discard(brand)
        else:
            state["active_brands"].add(brand)
        # Остаёмся на той же странице
        page = 0
        for i, b in enumerate(ALL_BRANDS):
            if b == brand:
                page = i // 5
                break
        state["brands_page"] = page
        try:
            await q.edit_message_text(brands_text(page), reply_markup=brands_kb(page), parse_mode="HTML")
        except Exception: pass

    # ── Назад ──
    elif data == "back":
        active = len(state["active_brands"])
        st     = "🟢 работает" if state["running"] else "🔴 остановлен"
        try:
            await q.edit_message_text(
                f"<b>🛍 Vinted Monitor</b>\n\n"
                f"Статус: {st}\n"
                f"Активных брендов: {active}\n"
                f"Цена: {state['min_price']}–{state['max_price']} EUR",
                reply_markup=main_kb(), parse_mode="HTML"
            )
        except Exception: pass


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    aw   = state.get("awaiting")
    text = update.message.text.strip().replace(",", ".")

    if aw == "min":
        try:
            val = float(text)
            if val >= 0:
                state["min_price"] = val
                state["awaiting"]  = None
                await update.message.reply_text(
                    f"✅ Минимальная цена: <b>{val}€</b>",
                    parse_mode="HTML", reply_markup=main_kb()
                )
            else:
                await update.message.reply_text("⚠️ Введи число больше 0", reply_markup=main_kb())
        except ValueError:
            await update.message.reply_text("⚠️ Нужно число, например: 10", reply_markup=main_kb())

    elif aw == "max":
        try:
            val = float(text)
            if val > 0:
                state["max_price"] = val
                state["awaiting"]  = None
                await update.message.reply_text(
                    f"✅ Максимальная цена: <b>{val}€</b>",
                    parse_mode="HTML", reply_markup=main_kb()
                )
            else:
                await update.message.reply_text("⚠️ Введи число больше 0", reply_markup=main_kb())
        except ValueError:
            await update.message.reply_text("⚠️ Нужно число, например: 500", reply_markup=main_kb())

    else:
        await update.message.reply_text("Используй /start для управления.", reply_markup=main_kb())


# ── ЗАПУСК ────────────────────────────────────────────────────────────────

def main():
    global bot_app
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN не задан!")
        return
    log.info(f"Запуск | брендов: {len(ALL_BRANDS)} | прокси: {'да' if PROXY_URL else 'нет'}")
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
