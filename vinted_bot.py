#!/usr/bin/env python3
import logging
import time
import threading
import os
from datetime import datetime
from typing import Optional

import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)

# ─────────────────────────────────────────────
#  НАСТРОЙКИ
# ─────────────────────────────────────────────

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

TARGET_REGIONS = {
    "pl": "www.vinted.pl",
    "lt": "www.vinted.lt"
}

# Список мусорных слов, чтобы не прилетала "хуйня"
BAD_WORDS = ["pieluchy", "pampers", "baby", "dziecko", "dla dzieci", "подгузники", "детское", "triko"]

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO, datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

HTTP = requests.Session()
HTTP.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
})

# ─── Состояние ────────────────────────────────
state = {
    "running": False,
    "brands": [
        "vetements", "balenciaga", "racer worldwide", "raf simons", 
        "palm angels", "bape", "aape", "givenchy", "dolce gabana", 
        "d&g", "maison margiela", "gucci", "burberry", "number nine", 
        "undercover", "acne studio", "supreme", "alyx", "amiri"
    ],
    "min_price": 20,       
    "max_price": 1000,      
    "interval": 300,       
    "pause_brands": 15,    
    "chat_id": None,
    "seen_ids": set(),
    "awaiting_input": None,
}

bot_app: Optional[Application] = None

# ─────────────────────────────────────────────
#  ЛОГИКА
# ─────────────────────────────────────────────

def fetch_items(query: str, domain: str) -> list:
    try:
        # Проверка и обновление сессии
        if not HTTP.cookies.get_dict(domain=domain):
            HTTP.get(f"https://{domain}/", timeout=10)
            time.sleep(2)

        r = HTTP.get(
            f"https://{domain}/api/v2/catalog/items",
            params={"search_text": query, "page": 1, "per_page": 50, "order": "newest_first"},
            timeout=15,
        )
        if r.status_code == 401:
            HTTP.cookies.clear()
            return []
        r.raise_for_status()
        return r.json().get("items", [])
    except Exception as e:
        log.warning(f"Ошибка {domain} при поиске {query}: {e}")
        return []

def is_garbage(title: str) -> bool:
    title_lower = title.lower()
    return any(word in title_lower for word in BAD_WORDS)

# ─────────────────────────────────────────────
#  ПОТОК МОНИТОРИНГА
# ─────────────────────────────────────────────

def monitor_loop():
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    while state["running"]:
        current_brands = list(state["brands"])
        log.info(f"Начинаю поиск по {len(current_brands)} брендам")

        for brand in current_brands:
            if not state["running"]: break
            
            for _, domain in TARGET_REGIONS.items():
                if not state["running"]: break
                
                log.info(f"Ищу {brand} на {domain}...")
                items = fetch_items(brand, domain)
                
                for item in (items or []):
                    iid = item.get("id")
                    if iid in state["seen_ids"]: continue
                    state["seen_ids"].add(iid)
                    
                    title = item.get("title", "")
                    price = float(item.get("price", {}).get("amount", 0))

                    # Фильтр: Бренд в названии + отсутствие мусора + цена
                    if brand in title.lower() and not is_garbage(title):
                        if state["min_price"] <= price <= state["max_price"]:
                            if state["chat_id"] and bot_app:
                                url = item.get("url", "")
                                link = f"https://{domain}{url}" if url.startswith("/") else url
                                msg = (f"🔥 <b>{brand.upper()}</b>\n📦 {title}\n"
                                       f"💰 <b>{price} {item.get('price',{}).get('currency_code','')}</b>\n"
                                       f"🔗 <a href='{link}'>ОТКРЫТЬ</a>")
                                loop.run_until_complete(bot_app.bot.send_message(chat_id=state["chat_id"], text=msg, parse_mode="HTML"))
                
                time.sleep(10) # Пауза между регионами
            time.sleep(state["pause_brands"]) # Пауза между брендами

        if state["running"]:
            log.info(f"Цикл завершен. Сплю {state['interval']}с")
            time.sleep(state["interval"])

# ─────────────────────────────────────────────
#  БОТ
# ─────────────────────────────────────────────

def main_kb():
    toggle = "⏹ Стоп" if state["running"] else "▶️ Старт"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle, callback_data="toggle")],
        [InlineKeyboardButton("💰 Мин", callback_data="set_min"), InlineKeyboardButton("💰 Макс", callback_data="set_max")]
    ])

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    await update.message.reply_text("Монитор брендов запущен. Используй кнопки:", reply_markup=main_kb())

async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "toggle":
        state["running"] = not state["running"]
        if state["running"]: threading.Thread(target=monitor_loop, daemon=True).start()
        await q.edit_message_text(f"Статус: {'Работает' if state['running'] else 'Спит'}", reply_markup=main_kb())
    elif q.data == "set_min":
        state["awaiting_input"] = "min"
        await q.message.reply_text("Введи мин цену:")
    elif q.data == "set_max":
        state["awaiting_input"] = "max"
        await q.message.reply_text("Введи макс цену:")

async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if state["awaiting_input"] in ["min", "max"]:
        try:
            num = int(update.message.text)
            if state["awaiting_input"] == "min": state["min_price"] = num
            else: state["max_price"] = num
            await update.message.reply_text("✅ Обновлено", reply_markup=main_kb())
        except: await update.message.reply_text("⚠️ Введи число")
        state["awaiting_input"] = None

def main():
    if not BOT_TOKEN: return
    app = Application.builder().token(BOT_TOKEN).build()
    global bot_app
    bot_app = app
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.run_polling()

if __name__ == "__main__":
    main()
