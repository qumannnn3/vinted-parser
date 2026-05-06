#!/usr/bin/env python3
"""
Vinted Monitor Bot — Final Edition
Авторизуется через реальный аккаунт Vinted, банят намного реже.
"""
import logging, time, threading, os, random, requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# ── НАСТРОЙКИ ──────────────────────────────────────────────────────────────
BOT_TOKEN    = os.environ.get("BOT_TOKEN", "")
VINTED_USER  = os.environ.get("VINTED_USER", "")      # email от аккаунта Vinted
VINTED_PASS  = os.environ.get("VINTED_PASS", "")      # пароль от аккаунта Vinted
PROXY_URL    = os.environ.get("PROXY_URL", "")

TARGET_REGIONS = {
    "pl": "www.vinted.pl",
    "lt": "www.vinted.lt",
    "lv": "www.vinted.lv",
}

BAD_WORDS = [
    "pieluchy", "pampers", "baby", "dziecko", "dla dzieci",
    "подгузники", "детское", "underwear", "socks", "bielizna",
    "majtki", "skarpety", "nosidełko", "fotelik",
]

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ── СОСТОЯНИЕ ──────────────────────────────────────────────────────────────
state = {
    "running": False,
    "brands": [
        "stone island", "balenciaga", "raf simons", "bape", "aape",
        "gucci", "chanel", "jeremy scott", "undercover", "comme des garcons",
        "yohji yamamoto", "vetements", "palm angels", "maison margiela",
        "givenchy", "burberry", "supreme", "amiri", "acne studios", "alyx",
    ],
    "min_price": 30,
    "max_price": 3000,
    "interval": 300,
    "chat_id": None,
    "seen_ids": set(),
    "stats": {"found": 0, "cycles": 0},
}

bot_app = None

# ── СЕССИИ ПО ДОМЕНУ ───────────────────────────────────────────────────────
sessions: dict[str, requests.Session] = {}

USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

def make_session(domain: str) -> requests.Session:
    s = requests.Session()
    ua = random.choice(USER_AGENTS)
    s.headers.update({
        "User-Agent": ua,
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
    """Загружаем главную страницу чтобы получить куки."""
    s = make_session(domain)
    try:
        r = s.get(f"https://{domain}/", timeout=15)
        log.info(f"Сессия {domain}: {r.status_code}")
    except Exception as e:
        log.warning(f"init_session {domain}: {e}")
    sessions[domain] = s
    return s


def get_session(domain: str) -> requests.Session:
    if domain not in sessions:
        return init_session(domain)
    return sessions[domain]


def login_vinted(domain: str) -> bool:
    """Авторизация через аккаунт Vinted (если заданы VINTED_USER/PASS)."""
    if not VINTED_USER or not VINTED_PASS:
        return False
    s = get_session(domain)
    try:
        # Получаем CSRF токен
        r = s.get(f"https://{domain}/api/v2/users/login", timeout=10)
        csrf = s.cookies.get("XSRF-TOKEN", "")
        if csrf:
            s.headers["X-CSRF-Token"] = csrf

        r = s.post(
            f"https://{domain}/api/v2/sessions",
            json={"login": VINTED_USER, "password": VINTED_PASS},
            timeout=15,
        )
        if r.status_code == 200:
            log.info(f"✅ Авторизован на {domain}")
            return True
        else:
            log.warning(f"Авторизация {domain}: {r.status_code}")
            return False
    except Exception as e:
        log.warning(f"login {domain}: {e}")
        return False


# ── ПАРСИНГ ────────────────────────────────────────────────────────────────

def fetch_items(query: str, domain: str) -> list | str:
    s = get_session(domain)
    # Меняем User-Agent каждый раз
    s.headers["User-Agent"] = random.choice(USER_AGENTS)

    params = {
        "search_text": query,
        "page": 1,
        "per_page": 48,
        "order": "newest_first",
        "price_from": state["min_price"],
        "price_to": state["max_price"],
    }
    try:
        r = s.get(
            f"https://{domain}/api/v2/catalog/items",
            params=params,
            timeout=20,
        )
        if r.status_code == 200:
            return r.json().get("items", [])
        elif r.status_code == 403:
            log.error(f"❌ 403 на {domain} — пересоздаю сессию")
            init_session(domain)   # пересоздаём сессию при бане
            return "BAN"
        elif r.status_code == 401:
            log.warning(f"401 на {domain} — пробую войти")
            login_vinted(domain)
            return []
        else:
            log.warning(f"{domain} → {r.status_code}")
            return []
    except Exception as e:
        log.warning(f"fetch {domain}: {e}")
        return []


def is_relevant(item: dict, brand: str) -> bool:
    """Проверяем что товар реально относится к бренду."""
    title = item.get("title", "").lower()
    brand_name = item.get("brand_title", "").lower()
    first_word = brand.split()[0]

    # Бренд должен быть в названии ИЛИ в поле brand
    brand_match = first_word in title or first_word in brand_name

    # Нет мусорных слов
    no_bad = not any(w in title for w in BAD_WORDS)

    return brand_match and no_bad


def format_msg(item: dict, domain: str) -> str:
    title    = item.get("title", "Без названия")
    pd       = item.get("price", {})
    price    = pd.get("amount", "?")
    curr     = pd.get("currency_code", "")
    size     = item.get("size_title", "")
    brand_t  = item.get("brand_title", "")
    cond     = item.get("status", "")
    url      = item.get("url", "")
    link     = f"https://{domain}{url}" if url.startswith("/") else url

    lines = [f"🏷 <b>{brand_t.upper() if brand_t else title}</b>", f"📦 {title}"]
    if size: lines.append(f"📏 {size}")
    if cond: lines.append(f"✨ {cond}")
    lines += [
        f"💰 <b>{price} {curr}</b>",
        f"🔗 <a href='{link}'>СМОТРЕТЬ НА VINTED</a>",
    ]
    return "\n".join(lines)


# ── МОНИТОРИНГ ─────────────────────────────────────────────────────────────

def monitor_loop():
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Инициализируем сессии
    for domain in TARGET_REGIONS.values():
        init_session(domain)
        if VINTED_USER:
            login_vinted(domain)
        time.sleep(2)

    log.info("🚀 Мониторинг запущен")

    while state["running"]:
        brands = state["brands"][:]
        random.shuffle(brands)
        state["stats"]["cycles"] += 1
        log.info(f"🔄 Цикл #{state['stats']['cycles']}: {len(brands)} брендов")

        for brand in brands:
            if not state["running"]:
                break

            for region, domain in TARGET_REGIONS.items():
                if not state["running"]:
                    break

                log.info(f"🔍 {brand} / {domain}")
                items = fetch_items(brand, domain)

                if items == "BAN":
                    wait = random.randint(45, 90)
                    log.warning(f"⏳ Жду {wait}с после бана...")
                    time.sleep(wait)
                    continue

                if items:
                    for item in items:
                        iid = item.get("id")
                        if iid in state["seen_ids"]:
                            continue
                        state["seen_ids"].add(iid)

                        if not is_relevant(item, brand):
                            continue

                        price = float(item.get("price", {}).get("amount", 0))
                        if not (state["min_price"] <= price <= state["max_price"]):
                            continue

                        msg = format_msg(item, domain)
                        state["stats"]["found"] += 1
                        log.info(f"✅ НАХОДКА: {item.get('title')} — {price}")

                        if state["chat_id"] and bot_app:
                            loop.run_until_complete(
                                bot_app.bot.send_message(
                                    chat_id=state["chat_id"],
                                    text=msg,
                                    parse_mode="HTML",
                                    disable_web_page_preview=False,
                                )
                            )

                # Случайная пауза между регионами
                time.sleep(random.uniform(10, 20))

            # Пауза между брендами
            time.sleep(random.uniform(15, 35))

        log.info(f"✅ Цикл завершён. Найдено за всё время: {state['stats']['found']}")
        if state["running"]:
            time.sleep(state["interval"])

    loop.close()
    log.info("⏹ Мониторинг остановлен")


# ── TELEGRAM БОТ ───────────────────────────────────────────────────────────

def main_kb():
    toggle = "⏹ Стоп" if state["running"] else "▶️ СТАРТ"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle, callback_data="toggle")],
        [InlineKeyboardButton("📊 Статус", callback_data="status")],
    ])


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    auth = "✅ авторизован" if VINTED_USER else "❌ без авторизации (анонимно)"
    proxy = "✅ прокси" if PROXY_URL else "❌ без прокси"
    text = (
        f"<b>🛍 Vinted Monitor</b>\n\n"
        f"Аккаунт: {auth}\n"
        f"Прокси: {proxy}\n"
        f"Брендов: {len(state['brands'])}\n"
        f"Цена: {state['min_price']}–{state['max_price']} PLN/EUR\n\n"
        f"Нажми СТАРТ чтобы начать:"
    )
    await update.message.reply_text(text, reply_markup=main_kb(), parse_mode="HTML")


async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    state["chat_id"] = q.message.chat_id

    if q.data == "toggle":
        if state["running"]:
            state["running"] = False
            await q.edit_message_text("⏹ Остановлен.", reply_markup=main_kb())
        else:
            state["running"] = True
            threading.Thread(target=monitor_loop, daemon=True).start()
            await q.edit_message_text(
                f"▶️ <b>Запущен!</b>\n\nИщу {len(state['brands'])} брендов на .pl, .lt, .de\n"
                f"Диапазон цен: {state['min_price']}–{state['max_price']} PLN/EUR",
                reply_markup=main_kb(), parse_mode="HTML"
            )

    elif q.data == "status":
        st = state["stats"]
        status = "🟢 работает" if state["running"] else "🔴 остановлен"
        text = (
            f"<b>📊 Статус</b>\n\n"
            f"{status}\n"
            f"Циклов: {st['cycles']}\n"
            f"Находок: {st['found']}\n"
            f"Брендов: {len(state['brands'])}\n"
            f"Цена: {state['min_price']}–{state['max_price']}"
        )
        await q.edit_message_text(text, reply_markup=main_kb(), parse_mode="HTML")


def main():
    global bot_app
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN не задан!")
        return

    log.info(f"Запуск | брендов: {len(state['brands'])} | прокси: {'да' if PROXY_URL else 'нет'} | аккаунт: {'да' if VINTED_USER else 'нет'}")

    proxy_url = PROXY_URL
    builder = Application.builder().token(BOT_TOKEN)
    if proxy_url:
        builder = builder.proxy(proxy_url).get_updates_proxy(proxy_url)
    bot_app = (
        builder
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(30)
        .build()
    )
    bot_app.add_handler(CommandHandler("start", cmd_start))
    bot_app.add_handler(CallbackQueryHandler(on_button))
    bot_app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        timeout=30,
    )


if __name__ == "__main__":
    import asyncio
    while True:
        try:
            asyncio.set_event_loop(asyncio.new_event_loop())
            main()
        except Exception as e:
            log.error(f"Бот упал: {e}. Перезапуск через 15с...")
            time.sleep(15)
