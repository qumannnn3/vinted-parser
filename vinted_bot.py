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

# Регионы мониторинга (Польша и Литва/Латвия)
TARGET_REGIONS = {
    "pl": "www.vinted.pl",
    "lt": "www.vinted.lt"
}

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
    "min_price": 10,       
    "max_price": 300,      
    "interval": 600,       
    "pause_brands": 15,    
    "chat_id": None,
    "seen_ids": set(),
    "stats": {"cycles": 0, "found": 0, "started_at": None},
    "awaiting_input": None,
}

monitor_thread: Optional[threading.Thread] = None
bot_app: Optional[Application] = None

# ─────────────────────────────────────────────
#  ЛОГИКА ПАРСИНГА
# ─────────────────────────────────────────────

def fetch_items(query: str, domain: str) -> list:
    try:
        # Инициализация сессии для получения cookie (защита от 401 ошибки)
        if not HTTP.cookies.get_dict(domain=domain):
            log.info(f"Инициализация сессии для {domain}...")
            HTTP.get(f"https://{domain}/", timeout=15)
            time.sleep(2)

        HTTP.headers.update({
            "Referer": f"https://{domain}/catalog?search_text={query}",
            "X-Requested-With": "XMLHttpRequest"
        })

        r = HTTP.get(
            f"https://{domain}/api/v2/catalog/items",
            params={
                "search_text": query, 
                "page": 1, 
                "per_page": 50, 
                "order": "newest_first"
            },
            timeout=15,
        )

        if r.status_code == 401:
            log.warning(f"Доступ 401 на {domain}. Сброс сессии...")
            HTTP.cookies.clear()
            return []

        r.raise_for_status()
        return r.json().get("items", [])
    except Exception as e:
        log.warning(f"Ошибка при запросе к {domain}: {e}")
        return []

def check_price_range(item: dict) -> bool:
    try:
        price_data = item.get("price", {})
        price = float(price_data.get("amount", 0))
        return state["min_price"] <= price <= state["max_price"]
    except (TypeError, ValueError):
        return False

def format_find(item: dict, brand: str, domain: str) -> str:
    title = item.get("title", "Без названия")
    pd = item.get("price", {})
    price, curr = pd.get("amount", "?"), pd.get("currency_code", "")
    url = item.get("url", "")
    link = f"https://{domain}{url}" if url.startswith("/") else url

    return (f"✅ <b>Найдено: {brand.upper()}</b>\n"
            f"📦 {title}\n"
            f"💰 <b>Цена: {price} {curr}</b>\n"
            f"🌐 Регион: {domain.split('.')[-1].upper()}\n"
            f"🔗 <a href='{link}'>Открыть на Vinted</a>")

# ─────────────────────────────────────────────
#  МОНИТОРИНГ
# ─────────────────────────────────────────────

def monitor_loop():
    import asyncio
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    while state["running"]:
        state["stats"]["cycles"] += 1
        current_brands = list(state["brands"])
        
        for brand in current_brands:
            if not state["running"]: break

            for _, domain in TARGET_REGIONS.items():
                if not state["running"]: break
                
                items = fetch_items(brand, domain)
                if not items:
                    time.sleep(5)
                    continue

                for item in items:
                    iid = item.get("id")
                    if iid in state["seen_ids"]: continue
                    state["seen_ids"].add(iid)

                    if check_price_range(item):
                        msg = format_find(item, brand, domain)
                        state["stats"]["found"] += 1
                        if state["chat_id"] and bot_app:
                            loop.run_until_complete(
                                bot_app.bot.send_message(
                                    chat_id=state["chat_id"],
                                    text=msg, parse_mode="HTML",
                                    disable_web_page_preview=False
                                )
                            )
                time.sleep(8) 
            time.sleep(state["pause_brands"]) 

        if state["running"]:
            log.info(f"Цикл завершен. Пауза {state['interval']}с")
            time.sleep(state["interval"])

# ─────────────────────────────────────────────
#  ИНТЕРФЕЙС БОТА
# ─────────────────────────────────────────────

def main_kb():
    toggle = "⏹ Остановить" if state["running"] else "▶️ Запустить"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle, callback_data="toggle")],
        [InlineKeyboardButton("💰 Мин. цена", callback_data="set_min"),
         InlineKeyboardButton("💰 Макс. цена", callback_data="set_max")],
        [InlineKeyboardButton("📋 Бренды", callback_data="brands"),
         InlineKeyboardButton("📊 Статус", callback_data="status")]
    ])

def home_text():
    st = "🟢 работает" if state["running"] else "🔴 остановлен"
    return (f"<b>Vinted Monitor (PL/LT/LV)</b>\n\n"
            f"Статус: {st}\n"
            f"Диапазон: <b>{state['min_price']} — {state['max_price']}</b>\n"
            f"Интервал круга: {state['interval']}с")

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    await update.message.reply_text(home_text(), reply_markup=main_kb(), parse_mode="HTML")

async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    
    if q.data == "toggle":
        if state["running"]:
            state["running"] = False
        else:
            state["running"] = True
            state["stats"]["started_at"] = datetime.now()
            threading.Thread(target=monitor_loop, daemon=True).start()
        await q.edit_message_text(home_text(), reply_markup=main_kb(), parse_mode="HTML")

    elif q.data == "set_min":
        state["awaiting_input"] = "min"
        await q.message.reply_text("Введите минимальную цену:")

    elif q.data == "set_max":
        state["awaiting_input"] = "max"
        await q.message.reply_text("Введите максимальную цену:")
    
    elif q.data == "status":
        await q.message.reply_text(f"Находок: {state['stats']['found']}\nЦиклов: {state['stats']['cycles']}")

    elif q.data == "brands":
        bl = "\n".join(f"• {b.upper()}" for b in state["brands"])
        await q.message.reply_text(f"<b>📋 Список брендов:</b>\n\n{bl}", parse_mode="HTML")

async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    val = update.message.text
    if state["awaiting_input"] in ["min", "max"]:
        try:
            num = int(val)
            if state["awaiting_input"] == "min": state["min_price"] = num
            else: state["max_price"] = num
            await update.message.reply_text(f"✅ Цена обновлена!", reply_markup=main_kb())
        except:
            await update.message.reply_text("⚠️ Введите число.")
        state["awaiting_input"] = None

def main():
    if not BOT_TOKEN: return
    app = Application.builder().token(BOT_TOKEN).build()
    global bot_app
    bot_app = app
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    log.info("Бот запущен")
    app.run_polling()

if __name__ == "__main__":
    main()
