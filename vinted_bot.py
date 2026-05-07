#!/usr/bin/env python3
import logging, time, asyncio, os, random, requests, threading
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from mercapi import Mercapi

# --- КОНФИГУРАЦИЯ ---
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
PROXY_URL = os.environ.get("PROXY_URL", "") # Опционально

# Настройки регионов Vinted
VINTED_REGIONS = {
    "pl": "www.vinted.pl",
    "lt": "www.vinted.lt",
    "lv": "www.vinted.lv",
}

# --- ГЛУБОКАЯ ФИЛЬТРАЦИЯ ---
# Бот будет присылать товары ТОЛЬКО этих брендов
ALLOWED_BRANDS = [
    "nike", "adidas", "stone island", "cp company", "carhartt", 
    "stussy", "arcteryx", "patagonia", "jordan", "tnf", "the north face",
    "dickies", "levis", "ralph lauren", "polo", "lacoste"
]

# Бренды-исключения (масс-маркет и мусор)
BANNED_BRANDS = ["zara", "hm", "h&m", "bershka", "pull&bear", "reserved", "shein"]

# Стоп-слова в названии
BAD_WORDS = [
    "pieluchy", "pampers", "baby", "dziecko", "подгузники", "детское",
    "underwear", "socks", "bielizna", "majtki", "skarpety", "figi", "bra"
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

class VintedScanner:
    def __init__(self, domain):
        self.domain = domain
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
        })
        self.known_ids = set()
        self._cookie_ok = False

    def refresh_cookies(self):
        try:
            self.session.get(f"https://{self.domain}/", timeout=10)
            self._cookie_ok = True
        except Exception as e:
            log.error(f"Cookie error {self.domain}: {e}")

    def fetch_items(self):
        if not self._cookie_ok: self.refresh_cookies()
        
        # Случайная категория из популярных (мужское/женское)
        cat = random.choice([1, 4, 16, 5, 2, 3]) 
        url = f"https://{self.domain}/api/v2/catalog/items?per_page=50&catalog_ids={cat}&order=newest_first"
        
        results = []
        try:
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 401:
                self._cookie_ok = False
                return []
            
            data = resp.json()
            for it in data.get("items", []):
                iid = str(it.get("id"))
                if iid in self.known_ids: continue
                self.known_ids.add(iid)

                title = str(it.get("title", "")).lower()
                brand = str(it.get("brand_title", "")).lower()

                # ГЛУБОКИЙ ФИЛЬТР
                is_brand_ok = any(b in brand for b in ALLOWED_BRANDS) or any(b in title for b in ALLOWED_BRANDS)
                is_banned = any(b in brand for b in BANNED_BRANDS)
                has_bad_word = any(w in title for w in BAD_WORDS)

                if is_brand_ok and not is_banned and not has_bad_word:
                    # Извлечение фото высокого качества
                    photo_obj = it.get("photo", {})
                    # full_size_url — это оригинал фото
                    img_url = photo_obj.get("full_size_url") or photo_obj.get("url")

                    # Данные продавца
                    user = it.get("user", {})
                    results.append({
                        "id": iid,
                        "title": it.get("title"),
                        "price": f"{it.get('total_item_price')} {it.get('currency')}",
                        "brand": it.get("brand_title", "No Brand"),
                        "url": it.get("url"),
                        "photo": img_url,
                        "source": f"Vinted [{self.domain.split('.')[-1].upper()}]",
                        "seller": {
                            "name": user.get("login", "N/A"),
                            "rating": round(user.get("feedback_reputation", 0) * 5, 1),
                            "reviews": user.get("feedback_count", 0),
                            "items_count": user.get("item_count", 0)
                        }
                    })
        except Exception as e:
            log.error(f"Vinted {self.domain} error: {e}")
        return results

async def fetch_mercari():
    merc = Mercapi()
    results = []
    try:
        # Ищем по случайному бренду из списка
        query = random.choice(ALLOWED_BRANDS)
        res = await merc.search(query=query, sort="created_time", order="desc")
        for it in res.items:
            # Фильтр для Mercari
            title = it.name.lower()
            if any(w in title for w in BAD_WORDS): continue

            results.append({
                "id": it.id,
                "title": it.name,
                "price": f"{it.price} JPY",
                "brand": query.upper(),
                "url": f"https://jp.mercari.com/item/{it.id}",
                "photo": it.thumbnails[0] if it.thumbnails else None,
                "source": "Mercari JP",
                "seller": {
                    "name": "Mercari Seller",
                    "rating": "N/A",
                    "reviews": "Check Web",
                    "items_count": "N/A"
                }
            })
    except Exception as e:
        log.error(f"Mercari error: {e}")
    return results

# --- ТЕЛЕГРАМ ЛОГИКА ---
active_users = set()

def format_message(item):
    rating = item['seller']['rating']
    stars = "⭐" * int(rating) if rating != "N/A" and rating > 0 else "🆕"
    
    return (
        f"<b>🔥 {item['title']}</b>\n\n"
        f"<b>💰 Цена:</b> <code>{item['price']}</code>\n"
        f"<b>🏷 Бренд:</b> #{item['brand'].replace(' ', '_')}\n"
        f"<b>📍 Маркет:</b> {item['source']}\n"
        f"───────────────────\n"
        f"<b>👤 Продавец:</b> {item['seller']['name']}\n"
        f"<b>📊 Рейтинг:</b> {stars} ({item['seller']['reviews']} отз.)\n"
        f"<b>📦 Вещей в профиле:</b> {item['seller']['items_count']}\n"
    )

async def scanner_task(context: ContextTypes.DEFAULT_TYPE):
    scanners = [VintedScanner(d) for d in VINTED_REGIONS.values()]
    while True:
        if not active_users:
            await asyncio.sleep(10)
            continue

        # Vinted
        for s in scanners:
            items = s.fetch_items()
            for it in items:
                for uid in active_users:
                    try:
                        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Открыть товар", url=it['url'])]])
                        if it['photo']:
                            await context.bot.send_photo(uid, it['photo'], caption=format_message(it), parse_mode="HTML", reply_markup=kb)
                        else:
                            await context.bot.send_message(uid, format_message(it), parse_mode="HTML", reply_markup=kb)
                    except Exception: pass
            await asyncio.sleep(2)

        # Mercari
        m_items = await fetch_mercari()
        for it in m_items:
            for uid in active_users:
                try:
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Открыть на Mercari", url=it['url'])]])
                    await context.bot.send_photo(uid, it['photo'], caption=format_message(it), parse_mode="HTML", reply_markup=kb)
                except Exception: pass
        
        await asyncio.sleep(15)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    active_users.add(update.effective_chat.id)
    await update.message.reply_text("🔎 Поиск запущен! Я буду присылать только брендовые вещи с хорошим качеством фото.")

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    active_users.discard(update.effective_chat.id)
    await update.message.reply_text("🛑 Поиск остановлен.")

async def post_init(application: Application):
    # Запускаем бесконечный цикл сканера как фоновую задачу сразу после старта
    asyncio.create_task(scanner_task(application))

def main():
    if not BOT_TOKEN:
        log.error("CRITICAL: BOT_TOKEN is missing!")
        return

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop))
    
    log.info("Бот запущен...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
