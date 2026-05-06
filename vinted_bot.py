#!/usr/bin/env python3
import logging, time, threading, os, random, requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

BOT_TOKEN  = os.environ.get("BOT_TOKEN", "")
PROXY_URL  = os.environ.get("PROXY_URL", "")

TARGET_REGIONS = {
    "pl": "www.vinted.pl",
    "lt": "www.vinted.lt",
    "lv": "www.vinted.lv",
}

BAD_WORDS = [
    "pieluchy","pampers","baby","dziecko","dla dzieci",
    "подгузники","детское","underwear","socks","bielizna",
    "majtki","skarpety","nosidełko","fotelik",
]

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

state = {
    "running": False,
    "brands": [
        "stone island","balenciaga","raf simons","bape","aape",
        "gucci","chanel","jeremy scott","undercover","comme des garcons",
        "yohji yamamoto","vetements","palm angels","maison margiela",
        "givenchy","burberry","supreme","amiri","acne studios","alyx",
    ],
    "min_price": 30,
    "max_price": 3000,
    "interval": 300,
    "chat_id": None,
    "seen_ids": set(),
    "stats": {"found": 0, "cycles": 0},
}

bot_app = None

USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

sessions: dict = {}


def make_session(domain: str) -> requests.Session:
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


def init_session(domain: str) -> requests.Session:
    s = make_session(domain)
    try:
        # Сначала грузим главную — получаем куки
        r = s.get(f"https://{domain}/", timeout=15)
        log.info(f"Сессия {domain}: {r.status_code}")
        # Потом грузим каталог — ещё куки
        s.get(f"https://{domain}/catalog", timeout=15)
    except Exception as e:
        log.warning(f"init_session {domain}: {e}")
    sessions[domain] = s
    return s


def get_session(domain: str) -> requests.Session:
    return sessions.get(domain) or init_session(domain)


def fetch_items(query: str, domain: str) -> list | str:
    s = get_session(domain)
    s.headers["User-Agent"] = random.choice(USER_AGENTS)

    try:
        r = s.get(
            f"https://{domain}/api/v2/catalog/items",
            params={
                "search_text": query,
                "page": 1,
                "per_page": 48,
                "order": "newest_first",
                "price_from": state["min_price"],
                "price_to": state["max_price"],
            },
            timeout=20,
        )
        if r.status_code == 200:
            ct = r.headers.get("content-type", "")
            if "json" not in ct:
                log.warning(f"{domain} вернул не JSON ({ct}) — пересоздаю сессию")
                init_session(domain)
                return []
            return r.json().get("items", [])
        elif r.status_code == 403:
            log.error(f"❌ 403 на {domain} — пересоздаю сессию")
            sessions.pop(domain, None)
            return "BAN"
        else:
            log.warning(f"{domain} → {r.status_code}")
            return []
    except Exception as e:
        log.warning(f"fetch {domain}: {e}")
        return []


def is_relevant(item: dict, brand: str) -> bool:
    title      = item.get("title", "").lower()
    brand_name = item.get("brand_title", "").lower()
    first_word = brand.split()[0]
    brand_match = first_word in title or first_word in brand_name
    no_bad      = not any(w in title for w in BAD_WORDS)
    return brand_match and no_bad


def format_msg(item: dict, domain: str) -> str:
    title   = item.get("title", "Без названия")
    pd      = item.get("price", {})
    price   = pd.get("amount", "?")
    curr    = pd.get("currency_code", "")
    size    = item.get("size_title", "")
    brand_t = item.get("brand_title", "")
    cond    = item.get("status", "")
    url     = item.get("url", "")
    link    = f"https://{domain}{url}" if url.startswith("/") else url
    lines = [f"🏷 <b>{brand_t.upper() if brand_t else title}</b>", f"📦 {title}"]
    if size: lines.append(f"📏 {size}")
    if cond: lines.append(f"✨ {cond}")
    lines += [f"💰 <b>{price} {curr}</b>", f"🔗 <a href='{link}'>СМОТРЕТЬ НА VINTED</a>"]
    return "\n".join(lines)


def monitor_loop():
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for domain in TARGET_REGIONS.values():
        init_session(domain)
        time.sleep(3)

    log.info("🚀 Мониторинг запущен")

    while state["running"]:
        brands = state["brands"][:]
        random.shuffle(brands)
        state["stats"]["cycles"] += 1
        log.info(f"🔄 Цикл #{state['stats']['cycles']}: {len(brands)} брендов")

        for brand in brands:
            if not state["running"]: break

            for region, domain in TARGET_REGIONS.items():
                if not state["running"]: break

                log.info(f"🔍 {brand} / {domain}")
                items = fetch_items(brand, domain)

                if items == "BAN":
                    wait = random.randint(60, 120)
                    log.warning(f"⏳ Жду {wait}с...")
                    time.sleep(wait)
                    continue

                for item in (items or []):
                    iid = item.get("id")
                    if iid in state["seen_ids"]: continue
                    state["seen_ids"].add(iid)
                    if not is_relevant(item, brand): continue
                    price = float(item.get("price", {}).get("amount", 0))
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

        log.info(f"✅ Цикл завершён. Находок всего: {state['stats']['found']}")
        if state["running"]:
            time.sleep(state["interval"])

    loop.close()


def main_kb():
    toggle = "⏹ Стоп" if state["running"] else "▶️ СТАРТ"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle, callback_data="toggle")],
        [InlineKeyboardButton("📊 Статус", callback_data="status")],
    ])


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    proxy = "✅" if PROXY_URL else "❌"
    text = (
        f"<b>🛍 Vinted Monitor</b>\n\n"
        f"Прокси: {proxy}\n"
        f"Брендов: {len(state['brands'])}\n"
        f"Домены: .pl .lt .lv\n"
        f"Цена: {state['min_price']}–{state['max_price']} PLN/EUR\n\n"
        f"Нажми СТАРТ:"
    )
    await update.message.reply_text(text, reply_markup=main_kb(), parse_mode="HTML")


async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    state["chat_id"] = q.message.chat_id

    if q.data == "toggle":
        if state["running"]:
            state["running"] = False
            try:
                await q.edit_message_text("⏹ Остановлен.", reply_markup=main_kb())
            except Exception: pass
        else:
            state["running"] = True
            threading.Thread(target=monitor_loop, daemon=True).start()
            try:
                await q.edit_message_text(
                    f"▶️ <b>Запущен!</b>\n\n"
                    f"Ищу {len(state['brands'])} брендов на .pl .lt .lv",
                    reply_markup=main_kb(), parse_mode="HTML"
                )
            except Exception: pass

    elif q.data == "status":
        st = state["stats"]
        status = "🟢 работает" if state["running"] else "🔴 остановлен"
        try:
            await q.edit_message_text(
                f"<b>📊 Статус</b>\n\n{status}\n"
                f"Циклов: {st['cycles']}\nНаходок: {st['found']}\n"
                f"Брендов: {len(state['brands'])}",
                reply_markup=main_kb(), parse_mode="HTML"
            )
        except Exception: pass


def main():
    global bot_app
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN не задан!")
        return
    log.info(f"Запуск | брендов: {len(state['brands'])} | прокси: {'да' if PROXY_URL else 'нет'}")
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
