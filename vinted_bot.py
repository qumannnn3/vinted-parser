#!/usr/bin/env python3
import logging, time, threading, os, random, requests, json as _json, gzip, re, html
from datetime import datetime, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
PROXY_URL  = os.environ.get("PROXY_URL", "")

VINTED_REGIONS = {
    "pl": "www.vinted.pl",
    "lt": "www.vinted.lt",
    "lv": "www.vinted.lv",
}

# Бренды для ГЛУБОКОЙ фильтрации (добавьте свои)
ALLOWED_BRANDS = ["nike", "adidas", "stone island", "cp company", "carhartt", "stussy", "arcteryx", "patagonia", "jordan"]
# Бренды-исключения (которые не берем ни при каких условиях)
BANNED_BRANDS = ["hm", "zara", "bershka", "pull&bear", "reserved"]

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
    "pościel","poduszka","kołdra","ręcznik"
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
            "Accept-Language": "en-US,en;q=0.9",
        })
        self.known_ids = set()
        self._cookie_ok = False

    def refresh_cookies(self):
        try:
            url = f"https://{self.domain}/"
            self.session.get(url, timeout=15)
            self._cookie_ok = True
        except:
            self._cookie_ok = False

    def fetch_items(self):
        if not self._cookie_ok:
            self.refresh_cookies()
        
        results = []
        # Выбираем случайную категорию для разнообразия
        cat = random.choice(CATALOG_IDS)
        url = f"https://{self.domain}/api/v2/catalog/items?per_page=50&catalog_ids={cat}&order=newest_first"
        
        try:
            resp = self.session.get(url, timeout=15)
            if resp.status_code == 401:
                self.refresh_cookies()
                return []
            
            data = resp.json()
            items = data.get("items", [])
            for it in items:
                iid = str(it.get("id"))
                if iid in self.known_ids: continue
                self.known_ids.add(iid)
                
                # --- ГЛУБОКАЯ ФИЛЬТРАЦИЯ ---
                brand = str(it.get("brand_title", "")).lower()
                title = str(it.get("title", "")).lower()
                
                # 1. Проверка на мусорные слова
                if any(w in title for w in BAD_WORDS): continue
                
                # 2. Проверка бренда (Whitelist & Blacklist)
                is_allowed = any(b in brand for b in ALLOWED_BRANDS) or any(b in title for b in ALLOWED_BRANDS)
                is_banned = any(b in brand for b in BANNED_BRANDS)
                
                if not is_allowed or is_banned:
                    continue

                # --- ДАННЫЕ ПРОДАВЦА ---
                user = it.get("user", {})
                seller_data = {
                    "name": user.get("login", "N/A"),
                    "feedback_count": user.get("feedback_count", 0),
                    "rating": user.get("feedback_reputation", 0) * 5, # Конвертация в 5-звездочную шкалу
                    "item_count": user.get("item_count", 0),
                    "city": it.get("city", "N/A")
                }

                # --- УЛУЧШЕННОЕ ФОТО ---
                # Пытаемся найти самое крупное фото
                photo_obj = it.get("photo", {})
                photo_url = photo_obj.get("full_size_url") or photo_obj.get("url")

                results.append({
                    "id": iid,
                    "title": it.get("title"),
                    "price": f"{it.get('total_item_price', 'N/A')} {it.get('currency', '')}",
                    "brand": it.get("brand_title", "No Brand"),
                    "size": it.get("size_title", "N/A"),
                    "url": it.get("url"),
                    "photo": photo_url,
                    "seller": seller_data,
                    "source": f"Vinted [{self.domain.split('.')[-1].upper()}]"
                })
        except Exception as e:
            log.error(f"Vinted error {self.domain}: {e}")
        return results

# Для Mercari (через mercapi)
from mercapi import Mercapi
merc_api = Mercapi()

async def fetch_mercari():
    # Глубокая фильтрация для Mercari
    results = []
    try:
        # Поиск по ключевому слову из списка брендов для релевантности
        brand_query = random.choice(ALLOWED_BRANDS)
        res = await merc_api.search(query=brand_query, sort="created_time", order="desc")
        for it in res.items:
            # Извлекаем подробности о продавце (требует доп. запроса в mercapi, если нужно очень детально)
            # Но базово возьмем то, что есть в объекте
            
            # Фильтр брендов
            title = it.name.lower()
            if not any(b in title for b in ALLOWED_BRANDS): continue
            if any(w in title for w in BAD_WORDS): continue

            results.append({
                "id": it.id,
                "title": it.name,
                "price": f"{it.price} JPY",
                "brand": brand_query.capitalize(),
                "size": "N/A",
                "url": f"https://jp.mercari.com/item/{it.id}",
                "photo": it.thumbnails[0] if it.thumbnails else None,
                "seller": {
                    "name": "Mercari User",
                    "feedback_count": "Check Web",
                    "rating": "N/A",
                    "item_count": "N/A"
                },
                "source": "Mercari JP"
            })
    except Exception as e:
        log.error(f"Mercari error: {e}")
    return results

# --- ТЕЛЕГРАМ БОТ ---
users_config = {}

def format_item_msg(item):
    # Генерация "звезд" рейтинга
    rating_val = item['seller']['rating']
    stars = "⭐" * int(round(float(rating_val))) if rating_val != "N/A" else "Нет отзывов"
    
    msg = (
        f"<b>🔥 {item['title']}</b>\n\n"
        f"<b>💰 Цена:</b> {item['price']}\n"
        f"<b>🏷 Бренд:</b> {item['brand']}\n"
        f"<b>📏 Размер:</b> {item['size']}\n"
        f"<b>📍 Площадка:</b> {item['source']}\n"
        f"───────────────────\n"
        f"<b>👤 Продавец:</b> {item['seller']['name']}\n"
        f"<b>📊 Рейтинг:</b> {stars} ({item['seller']['feedback_count']} отзывов)\n"
        f"<b>📦 Товаров в профиле:</b> {item['seller']['item_count']}\n"
        f"───────────────────\n"
    )
    return msg

async def scanner_loop(context: ContextTypes.DEFAULT_TYPE):
    scanners = [VintedScanner(d) for d in VINTED_REGIONS.values()]
    while True:
        # Vinted
        for s in scanners:
            items = s.fetch_items()
            for it in items:
                for uid, cfg in users_config.items():
                    if cfg.get("active"):
                        try:
                            # Кнопка для перехода
                            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Открыть товар", url=it['url'])]])
                            if it['photo']:
                                await context.bot.send_photo(uid, it['photo'], caption=format_item_msg(it), parse_mode="HTML", reply_markup=kb)
                            else:
                                await context.bot.send_message(uid, format_item_msg(it), parse_mode="HTML", reply_markup=kb)
                        except: pass
            time.sleep(2)
        
        # Mercari
        m_items = await fetch_mercari()
        for it in m_items:
             for uid, cfg in users_config.items():
                if cfg.get("active"):
                    try:
                        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Открыть товар", url=it['url'])]])
                        await context.bot.send_photo(uid, it['photo'], caption=format_item_msg(it), parse_mode="HTML", reply_markup=kb)
                    except: pass
        
        time.sleep(10)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_chat.id
    users_config[uid] = {"active": True}
    await update.message.reply_text("🚀 Парсер запущен!\n\nФильтрация: ТОЛЬКО брендовые вещи.\nДанные продавца: включены.\n\nИспользуйте /stop чтобы приостановить.")

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_chat.id
    if uid in users_config:
        users_config[uid]["active"] = False
    await update.message.reply_text("🛑 Парсер остановлен.")

def main():
    if not BOT_TOKEN:
        print("BOT_TOKEN is missing!")
        return
    
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Запуск цикла сканирования в отдельном потоке (через job_queue)
    app.job_queue.run_once(lambda ctx: threading.Thread(target=lambda: None).start(), 0) 
    # В библиотеке PTB лучше использовать job_queue для асинхронных задач:
    app.job_queue.run_repeating(lambda ctx: None, interval=60, first=1)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    
    # Важное примечание: так как scanner_loop бесконечный, 
    # в реальном продакшене его лучше запустить через asyncio.create_task в post_init
    async def start_background_tasks(application):
        import asyncio
        asyncio.create_task(scanner_loop(application))

    app.post_init = start_background_tasks
    
    print("Бот запущен...")
    app.run_polling()

if __name__ == "__main__":
    main()
