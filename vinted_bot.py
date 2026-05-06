#!/usr/bin/env python3
import logging
import time
import threading
import os
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
TARGET_REGIONS = {"pl": "www.vinted.pl", "lt": "www.vinted.lt"}
BAD_WORDS = ["pieluchy", "pampers", "baby", "dziecko", "dla dzieci", "подгузники", "детское", "triko", "underwear"]

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# --- СОСТОЯНИЕ ---
state = {
    "running": False,
    "brands": ["vetements", "balenciaga", "racer worldwide", "raf simons", "palm angels", "bape", "aape", "givenchy", "dolce gabana", "maison margiela", "gucci", "burberry", "number nine", "undercover", "acne studio", "supreme", "alyx", "amiri"],
    "min_price": 25,
    "max_price": 1500,
    "interval": 400, 
    "chat_id": None,
    "seen_ids": set()
}

HTTP = requests.Session()

def get_vinted_cookies(domain):
    """Обновляет сессию, чтобы избежать ошибок 401/403"""
    try:
        HTTP.cookies.clear()
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"}
        res = HTTP.get(f"https://{domain}/", headers=headers, timeout=10)
        return res.status_code == 200
    except:
        return False

def fetch_items(query, domain):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Referer": f"https://{domain}/catalog?search_text={query}"
        }
        r = HTTP.get(
            f"https://{domain}/api/v2/catalog/items",
            params={"search_text": query, "page": 1, "per_page": 50, "order": "newest_first"},
            headers=headers,
            timeout=15
        )
        if r.status_code == 200:
            return r.json().get("items", [])
        return None
    except:
        return None

def monitor_loop():
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    while state["running"]:
        for brand in list(state["brands"]):
            if not state["running"]: break
            
            for _, domain in TARGET_REGIONS.items():
                if not state["running"]: break
                
                log.info(f"Проверяю {brand} на {domain}...")
                get_vinted_cookies(domain) # Обновляем куки перед каждым брендом
                time.sleep(2)
                
                items = fetch_items(brand, domain)
                if items is None:
                    log.warning(f"Блокировка на {domain}, пропускаю бренд...")
                    time.sleep(10)
                    continue

                for item in items:
                    iid = item.get("id")
                    if iid in state["seen_ids"]: continue
                    state["seen_ids"].add(iid)

                    title = item.get("title", "").lower()
                    price = float(item.get("price", {}).get("amount", 0))

                    # ЖЕСТКАЯ ФИЛЬТРАЦИЯ
                    is_brand_in_title = brand.split()[0] in title 
                    has_garbage = any(word in title for word in BAD_WORDS)

                    if is_brand_in_title and not has_garbage and state["min_price"] <= price <= state["max_price"]:
                        url = item.get("url", "")
                        link = f"https://{domain}{url}" if url.startswith("/") else url
                        msg = f"🔥 <b>{brand.upper()}</b>\n📦 {item.get('title')}\n💰 <b>{price} {item.get('price',{}).get('currency_code')}</b>\n🔗 <a href='{link}'>ОТКРЫТЬ</a>"
                        if state["chat_id"]:
                            loop.run_until_complete(bot_app.bot.send_message(chat_id=state["chat_id"], text=msg, parse_mode="HTML"))
                
                time.sleep(15) # Пауза между доменами (защита от бана)
            time.sleep(20) # Пауза между брендами
            
        if state["running"]:
            log.info(f"Цикл завершен. Сплю {state['interval']}с")
            time.sleep(state["interval"])

# --- ИНТЕРФЕЙС ---
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Старт", callback_data="toggle")]])
    await update.message.reply_text("Бот настроен. Минимум мусора, максимум брендов.", reply_markup=kb)

async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "toggle":
        state["running"] = not state["running"]
        if state["running"]: threading.Thread(target=monitor_loop, daemon=True).start()
        txt = "✅ Мониторинг ЗАПУЩЕН" if state["running"] else "⏹ Мониторинг ОСТАНОВЛЕН"
        await q.edit_message_text(txt)

def main():
    global bot_app
    bot_app = Application.builder().token(BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", cmd_start))
    bot_app.add_handler(CallbackQueryHandler(on_button))
    bot_app.run_polling()

if __name__ == "__main__":
    main()
