#!/usr/bin/env python3
import logging
import time
import statistics
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

# Список доменов для мониторинга (Латвия и Литва объединены на .lt)
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
})

# ─── Состояние ────────────────────────────────
state = {
    "running": False,
    "brands": [
        "stone island", "raf simons", "adidas", "undercover", 
        "gucci", "balenciaga", "comme des garcons", "bape"
    ],
    "discount": 30,
    "interval": 600,       # 10 минут между полными циклами
    "pause_brands": 12,    # 12 секунд между брендами (защита от бана)
    "chat_id": None,
    "seen_ids": set(),
    "stats": {"cycles": 0, "found": 0, "started_at": None},
    "awaiting_input": None,
}

monitor_thread: Optional[threading.Thread] = None
bot_app: Optional[Application] = None

# ─────────────────────────────────────────────
#  VINTED LOGIC
# ─────────────────────────────────────────────

def fetch_items(query: str, domain: str, page: int = 1) -> list:
    try:
        # Обновляем Referer для каждого домена отдельно
        HTTP.headers.update({"Referer": f"https://{domain}/", "Origin": f"https://{domain}"})
        r = HTTP.get(
            f"https://{domain}/api/v2/catalog/items",
            params={"search_text": query, "page": page,
                    "per_page": 96, "order": "newest_first"},
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("items", [])
    except Exception as e:
        log.warning(f"Ошибка fetch ({domain} | {query}): {e}")
        return []

def market_median(items: list) -> Optional[float]:
    prices = []
    for it in items:
        try:
            p = float(it.get("price", {}).get("amount", 0))
            if p > 0: prices.append(p)
        except (TypeError, ValueError): pass
    return statistics.median(prices) if len(prices) >= 5 else None

def check_price(item: dict, median: Optional[float]) -> tuple:
    try:
        pd = item.get("price", {})
        price = float(pd.get("amount", 0))
        curr = pd.get("currency_code", "")
    except (TypeError, ValueError): return False, ""
    
    if price <= 0 or not median: return False, ""
    
    disc = (1 - price / median) * 100
    if disc >= state["discount"]:
        return True, f"скидка <b>{disc:.0f}%</b> (рынок ~{median:.0f} {curr})"
    return False, ""

def format_find(item: dict, brand: str, reason: str, domain: str) -> str:
    title = item.get("title", "Без названия")
    pd = item.get("price", {})
    price, curr = pd.get("amount", "?"), pd.get("currency_code", "")
    brand_t = item.get("brand_title", "") or brand.title()
    url = item.get("url", "")
    link = f"https://{domain}{url}" if url.startswith("/") else url

    return (f"🛍 <b>{brand_t.upper()}</b> ({domain.split('.')[-1].upper()})\n"
            f"📦 {title}\n"
            f"💰 <b>{price} {curr}</b>\n"
            f"📊 {reason}\n"
            f"🔗 {link}")

# ─────────────────────────────────────────────
#  MONITORING THREAD
# ─────────────────────────────────────────────

def monitor_loop():
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    log.info("Мониторинг PL/LT/LV запущен")

    while state["running"]:
        brands = state["brands"][:]
        state["stats"]["cycles"] += 1
        
        for brand in brands:
            if not state["running"]: break

            for region_code, domain in TARGET_REGIONS.items():
                if not state["running"]: break
                
                items = fetch_items(brand, domain, 1)
                if not items:
                    time.sleep(5)
                    continue

                median = market_median(items)
                
                for item in items:
                    iid = item.get("id")
                    if iid in state["seen_ids"]: continue
                    state["seen_ids"].add(iid)

                    below, reason = check_price(item, median)
                    if below:
                        msg = format_find(item, brand, reason, domain)
                        state["stats"]["found"] += 1
                        if state["chat_id"] and bot_app:
                            loop.run_until_complete(
                                bot_app.bot.send_message(
                                    chat_id=state["chat_id"],
                                    text=msg, parse_mode="HTML"
                                )
                            )
                
                # Короткая пауза между регионами одного бренда
                time.sleep(4)

            # Пауза между разными брендами
            time.sleep(state["pause_brands"])

        if state["running"]:
            log.info(f"Цикл завершен. Спим {state['interval']}с")
            time.sleep(state["interval"])
    loop.close()

def start_monitor():
    global monitor_thread
    if not state["running"]:
        state["running"] = True
        state["stats"]["started_at"] = datetime.now()
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()

def stop_monitor():
    state["running"] = False

# ─────────────────────────────────────────────
#  TELEGRAM HANDLERS
# ─────────────────────────────────────────────

def main_kb():
    toggle = "⏹ Остановить" if state["running"] else "▶️ Запустить"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle, callback_data="toggle")],
        [InlineKeyboardButton("📋 Бренды", callback_data="brands"),
         InlineKeyboardButton("📊 Статус", callback_data="status")],
        [InlineKeyboardButton("⚙️ Скидка", callback_data="set_discount"),
         InlineKeyboardButton("⏱ Интервал", callback_data="set_interval")]
    ])

def home_text():
    st = "🟢 работает" if state["running"] else "🔴 остановлен"
    return (f"<b>Vinted Monitor (PL/LT/LV)</b>\n\n"
            f"Статус: {st}\n"
            f"Регионы: Польша, Литва, Латвия\n"
            f"Брендов: {len(state['brands'])}\n"
            f"Скидка: ≥{state['discount']}%\n"
            f"Интервал: {state['interval']}с")

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    await update.message.reply_text(home_text(), reply_markup=main_kb(), parse_mode="HTML")

async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    state["chat_id"] = q.message.chat_id

    if q.data == "toggle":
        if state["running"]: stop_monitor()
        else: start_monitor()
        await q.edit_message_text(home_text(), reply_markup=main_kb(), parse_mode="HTML")

    elif q.data == "status":
        st = state["stats"]
        txt = (f"<b>📊 Статистика</b>\n\n"
               f"Циклов: {st['cycles']}\n"
               f"Находок: {st['found']}\n"
               f"ID в базе: {len(state['seen_ids'])}\n"
               f"Пауза бренда: {state['pause_brands']}с")
        await q.edit_message_text(txt, reply_markup=main_kb(), parse_mode="HTML")

    elif q.data == "set_interval":
        state["awaiting_input"] = "set_interval"
        await q.edit_message_text("Введите интервал между циклами в секундах (мин. 300):")

    elif q.data == "brands":
        bl = "\n".join(f"• {b.title()}" for b in state["brands"])
        await q.edit_message_text(f"<b>📋 Список брендов:</b>\n\n{bl}", reply_markup=main_kb(), parse_mode="HTML")

async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if state["awaiting_input"] == "set_interval":
        try:
            val = int(update.message.text)
            if val >= 300:
                state["interval"] = val
                await update.message.reply_text(f"✅ Интервал обновлен: {val}с")
            else:
                await update.message.reply_text("⚠️ Минимум 300 секунд.")
        except: await update.message.reply_text("⚠️ Введите число.")
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
