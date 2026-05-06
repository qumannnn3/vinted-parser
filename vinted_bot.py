#!/usr/bin/env python3
import logging
import time
import threading
import os
import random
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
TARGET_REGIONS = {"pl": "www.vinted.pl", "lt": "www.vinted.lt"}
# Расширил список мусора, чтобы не летели левые шмотки
BAD_WORDS = ["pieluchy", "pampers", "baby", "dziecko", "dla dzieci", "подгузники", "детское", "triko", "underwear", "socks", "nosidełko"]

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# --- СОСТОЯНИЕ ---
state = {
    "running": False,
    "brands": ["vetements", "balenciaga", "racer worldwide", "raf simons", "palm angels", "bape", "aape", "givenchy", "dolce gabana", "maison margiela", "gucci", "burberry", "number nine", "undercover", "acne studio", "supreme", "alyx", "amiri"],
    "min_price": 30, # Поднял до 30, чтобы отсечь совсем дешман
    "max_price": 2000,
    "interval": 300, 
    "chat_id": None,
    "seen_ids": set()
}

HTTP = requests.Session()

def update_session(domain):
    """Имитация захода реального юзера"""
    try:
        HTTP.cookies.clear()
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ]
        headers = {"User-Agent": random.choice(user_agents), "Accept-Language": "en-US,en;q=0.9"}
        HTTP.get(f"https://{domain}/", headers=headers, timeout=15)
        return True
    except:
        return False

def fetch_items(query, domain):
    try:
        headers = {
            "User-Agent": HTTP.headers.get("User-Agent"),
            "Accept": "application/json",
            "Referer": f"https://{domain}/catalog?search_text={query}",
            "X-Requested-With": "XMLHttpRequest"
        }
        r = HTTP.get(
            f"https://{domain}/api/v2/catalog/items",
            params={"search_text": query, "page": 1, "per_page": 50, "order": "newest_first"},
            headers=headers,
            timeout=15
        )
        if r.status_code == 200:
            return r.json().get("items", [])
        elif r.status_code == 403:
            log.error(f"❌ Бан 403 на {domain}. Нужно ждать...")
            return "BAN"
        return None
    except:
        return None

def monitor_loop():
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    while state["running"]:
        random.shuffle(state["brands"]) # Перемешиваем, чтобы не палиться
        
        for brand in state["brands"]:
            if not state["running"]: break
            
            for _, domain in TARGET_REGIONS.items():
                if not state["running"]: break
                
                log.info(f"🔍 Проверка: {brand} ({domain})")
                update_session(domain)
                time.sleep(random.randint(2, 5)) # Случайная пауза
                
                items = fetch_items(brand, domain)
                
                if items == "BAN":
                    time.sleep(60) # Если словили 403, курим минуту
                    continue
                
                if items:
                    for item in items:
                        iid = item.get("id")
                        if iid in state["seen_ids"]: continue
                        state["seen_ids"].add(iid)

                        title = item.get("title", "").lower()
                        price = float(item.get("price", {}).get("amount", 0))

                        # ФИЛЬТР: бренд должен быть в названии + не быть мусором
                        if brand.split()[0] in title and not any(w in title for w in BAD_WORDS):
                            if state["min_price"] <= price <= state["max_price"]:
                                url = item.get("url", "")
                                link = f"https://{domain}{url}" if url.startswith("/") else url
                                msg = f"🏷 <b>{brand.upper()}</b>\n📦 {item.get('title')}\n💰 <b>{price} {item.get('price',{}).get('currency_code')}</b>\n🔗 <a href='{link}'>КУПИТЬ</a>"
                                if state["chat_id"]:
                                    loop.run_until_complete(bot_app.bot.send_message(chat_id=state["chat_id"], text=msg, parse_mode="HTML"))
                
                time.sleep(random.randint(15, 25)) # Длинная пауза между странами
            time.sleep(random.randint(20, 40)) # Пауза между брендами

        if state["running"]:
            log.info("Круг завершен. Отдыхаем...")
            time.sleep(state["interval"])

# --- ТГ БОТ ---
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ ПОЕХАЛИ", callback_data="toggle")]])
    await update.message.reply_text(f"Бот готов. Ищу бренды от {state['min_price']} PLN/EUR", reply_markup=kb)

async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query.data == "toggle":
        state["running"] = not state["running"]
        if state["running"]: threading.Thread(target=monitor_loop, daemon=True).start()
        await update.callback_query.edit_message_text("✅ Работаю!" if state["running"] else "⏹ Стоп.")

def main():
    global bot_app
    bot_app = (
        Application.builder()
        .token(BOT_TOKEN)
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
    while True:
        try:
            main()
        except Exception as e:
            log.error(f"Бот упал: {e}. Перезапуск через 10с...")
            time.sleep(10)
