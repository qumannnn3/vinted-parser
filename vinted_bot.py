#!/usr/bin/env python3
"""
Vinted Monitor Bot — Railway Edition
"""

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

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")  # задаётся через Railway Variables
COUNTRY   = os.environ.get("VINTED_COUNTRY", "ru")  # ru, de, fr, pl, uk...

# ─────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO, datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

VINTED_DOMAINS = {
    "ru": "www.vinted.ru", "de": "www.vinted.de", "fr": "www.vinted.fr",
    "pl": "www.vinted.pl", "lt": "www.vinted.lt", "lv": "www.vinted.lv",
    "uk": "www.vinted.co.uk", "us": "www.vinted.com", "be": "www.vinted.be",
    "nl": "www.vinted.nl", "es": "www.vinted.es", "it": "www.vinted.it",
}
DOMAIN = VINTED_DOMAINS.get(COUNTRY, "www.vinted.com")

HTTP = requests.Session()
HTTP.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": f"https://{DOMAIN}/",
    "Origin":  f"https://{DOMAIN}",
})

# ─── Состояние ────────────────────────────────
state = {
    "running": False,
    "brands": [
        "stone island", "raf simons", "adidas", "jeremy scott",
        "undercover", "gucci", "chanel", "balenciaga",
        "comme des garcons", "yohji yamamoto", "bape", "aape",
    ],
    "discount": 30,
    "interval": 300,
    "pause_brands": 8,
    "chat_id": None,
    "seen_ids": set(),
    "stats": {"cycles": 0, "found": 0, "started_at": None},
    "awaiting_input": None,
}

monitor_thread: Optional[threading.Thread] = None
bot_app: Optional[Application] = None


# ─────────────────────────────────────────────
#  VINTED
# ─────────────────────────────────────────────

def init_vinted():
    try:
        HTTP.get(f"https://{DOMAIN}/", timeout=10)
    except Exception:
        pass


def fetch_items(query: str, page: int = 1) -> list:
    try:
        r = HTTP.get(
            f"https://{DOMAIN}/api/v2/catalog/items",
            params={"search_text": query, "page": page,
                    "per_page": 96, "order": "newest_first"},
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("items", [])
    except Exception as e:
        log.warning(f"fetch({query} p{page}): {e}")
        return []


def market_median(items: list) -> Optional[float]:
    prices = []
    for it in items:
        try:
            p = float(it.get("price", {}).get("amount", 0))
            if p > 0:
                prices.append(p)
        except (TypeError, ValueError):
            pass
    return statistics.median(prices) if len(prices) >= 5 else None


def check_price(item: dict, median: Optional[float]) -> tuple:
    try:
        pd    = item.get("price", {})
        price = float(pd.get("amount", 0))
        curr  = pd.get("currency_code", "")
    except (TypeError, ValueError):
        return False, ""
    if price <= 0 or not median:
        return False, ""
    disc = (1 - price / median) * 100
    if disc >= state["discount"]:
        return True, f"скидка <b>{disc:.0f}%</b> от рынка ~{median:.0f} {curr}"
    return False, ""


def format_find(item: dict, brand: str, reason: str) -> str:
    title   = item.get("title", "Без названия")
    pd      = item.get("price", {})
    price   = pd.get("amount", "?")
    curr    = pd.get("currency_code", "")
    size    = item.get("size_title", "")
    brand_t = item.get("brand_title", "") or brand.title()
    cond    = item.get("status", "")
    url     = item.get("url", "")
    link    = f"https://{DOMAIN}{url}" if url.startswith("/") else url

    lines = [f"🛍 <b>{brand_t.upper()}</b>", f"📦 {title}"]
    if size: lines.append(f"📏 {size}")
    if cond: lines.append(f"✨ {cond}")
    lines += [f"💰 <b>{price} {curr}</b>", f"📊 {reason}", f"🔗 {link}"]
    return "\n".join(lines)


# ─────────────────────────────────────────────
#  ПОТОК МОНИТОРИНГА
# ─────────────────────────────────────────────

def monitor_loop():
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    log.info("Мониторинг запущен")
    init_vinted()

    while state["running"]:
        brands = state["brands"][:]
        state["stats"]["cycles"] += 1
        log.info(f"Цикл #{state['stats']['cycles']}: {len(brands)} брендов")

        for i, brand in enumerate(brands):
            if not state["running"]:
                break

            items = fetch_items(brand, 1)
            if not items:
                time.sleep(state["pause_brands"])
                continue

            all_items = items[:]
            if len(items) >= 96:
                all_items += fetch_items(brand, 2)

            median = market_median(all_items)
            log.info(f"  {brand.title()}: {len(items)} шт, медиана={f'{median:.0f}' if median else 'н/д'}")

            for item in items:
                iid = item.get("id")
                if iid in state["seen_ids"]:
                    continue
                state["seen_ids"].add(iid)

                below, reason = check_price(item, median)
                if below:
                    msg = format_find(item, brand, reason)
                    state["stats"]["found"] += 1
                    log.info(f"  → НАХОДКА: {item.get('title','')}")
                    if state["chat_id"] and bot_app:
                        loop.run_until_complete(
                            bot_app.bot.send_message(
                                chat_id=state["chat_id"],
                                text=msg,
                                parse_mode="HTML",
                                disable_web_page_preview=False,
                            )
                        )

            if i < len(brands) - 1:
                time.sleep(state["pause_brands"])

        if state["running"]:
            log.info(f"Цикл завершён. Пауза {state['interval']}с")
            time.sleep(state["interval"])

    loop.close()
    log.info("Мониторинг остановлен")


def start_monitor():
    global monitor_thread
    if state["running"]:
        return
    state["running"] = True
    state["stats"]["started_at"] = datetime.now()
    monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
    monitor_thread.start()


def stop_monitor():
    state["running"] = False


# ─────────────────────────────────────────────
#  КЛАВИАТУРЫ
# ─────────────────────────────────────────────

def main_kb():
    toggle = "⏹ Остановить" if state["running"] else "▶️ Запустить"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle, callback_data="toggle")],
        [InlineKeyboardButton("📋 Бренды",   callback_data="brands"),
         InlineKeyboardButton("📊 Статус",   callback_data="status")],
        [InlineKeyboardButton("⚙️ Скидка",   callback_data="set_discount"),
         InlineKeyboardButton("⏱ Интервал",  callback_data="set_interval")],
    ])


def brands_kb():
    rows = [[InlineKeyboardButton(f"❌ {b.title()}", callback_data=f"del_{b}")]
            for b in state["brands"]]
    rows.append([InlineKeyboardButton("➕ Добавить", callback_data="add_brand")])
    rows.append([InlineKeyboardButton("🔙 Назад",   callback_data="back")])
    return InlineKeyboardMarkup(rows)


def home_text():
    st = "🟢 работает" if state["running"] else "🔴 остановлен"
    return (
        f"<b>Vinted Price Monitor</b>\n\n"
        f"Статус: {st}\n"
        f"Брендов: {len(state['brands'])}\n"
        f"Скидка: ≥{state['discount']}%\n"
        f"Интервал: {state['interval']}с"
    )


# ─────────────────────────────────────────────
#  ОБРАБОТЧИКИ
# ─────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    await update.message.reply_text(
        home_text() + "\n\nИспользуй кнопки для управления:",
        reply_markup=main_kb(), parse_mode="HTML"
    )


async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    await q.answer()
    data = q.data
    state["chat_id"] = q.message.chat_id

    if data == "toggle":
        if state["running"]:
            stop_monitor()
            txt = "⏹ Мониторинг <b>остановлен</b>."
        else:
            start_monitor()
            txt = (f"▶️ Мониторинг <b>запущен</b>!\n\n"
                   f"Проверяю {len(state['brands'])} брендов каждые {state['interval']}с.\n"
                   f"Буду слать товары со скидкой ≥{state['discount']}% от медианы.")
        await q.edit_message_text(txt, reply_markup=main_kb(), parse_mode="HTML")

    elif data == "status":
        st = state["stats"]
        started = st["started_at"].strftime("%d.%m %H:%M") if st["started_at"] else "—"
        txt = (f"<b>📊 Статус</b>\n\n"
               f"{'🟢 работает' if state['running'] else '🔴 остановлен'}\n"
               f"Запущен: {started}\n"
               f"Циклов: {st['cycles']}\n"
               f"Находок: {st['found']}\n\n"
               f"Скидка: ≥{state['discount']}%\n"
               f"Интервал: {state['interval']}с\n"
               f"Брендов: {len(state['brands'])}")
        await q.edit_message_text(txt, reply_markup=main_kb(), parse_mode="HTML")

    elif data == "brands":
        bl = "\n".join(f"• {b.title()}" for b in state["brands"])
        await q.edit_message_text(
            f"<b>📋 Бренды:</b>\n\n{bl}\n\nНажми ❌ чтобы удалить:",
            reply_markup=brands_kb(), parse_mode="HTML"
        )

    elif data.startswith("del_"):
        brand = data[4:]
        if brand in state["brands"]:
            state["brands"].remove(brand)
        bl = "\n".join(f"• {b.title()}" for b in state["brands"]) or "(пусто)"
        await q.edit_message_text(
            f"<b>📋 Бренды:</b>\n\n{bl}",
            reply_markup=brands_kb(), parse_mode="HTML"
        )

    elif data == "add_brand":
        state["awaiting_input"] = "add_brand"
        await q.edit_message_text("✏️ Напиши название бренда:", parse_mode="HTML")

    elif data == "set_discount":
        state["awaiting_input"] = "set_discount"
        await q.edit_message_text(
            f"✏️ Текущая скидка: <b>{state['discount']}%</b>\n\nНапиши новое значение (10–90):",
            parse_mode="HTML"
        )

    elif data == "set_interval":
        state["awaiting_input"] = "set_interval"
        await q.edit_message_text(
            f"✏️ Текущий интервал: <b>{state['interval']}с</b>\n\nНапиши новое значение (мин. 60):",
            parse_mode="HTML"
        )

    elif data == "back":
        await q.edit_message_text(home_text(), reply_markup=main_kb(), parse_mode="HTML")


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    aw   = state.get("awaiting_input")
    text = update.message.text.strip()

    if aw == "add_brand":
        brand = text.lower()
        if brand and brand not in state["brands"]:
            state["brands"].append(brand)
            msg = f"✅ <b>{brand.title()}</b> добавлен! Брендов: {len(state['brands'])}"
        else:
            msg = "⚠️ Такой бренд уже есть или пустая строка."
        state["awaiting_input"] = None
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=main_kb())

    elif aw == "set_discount":
        try:
            val = int(text)
            if 5 <= val <= 95:
                state["discount"] = val
                msg = f"✅ Скидка: <b>≥{val}%</b>"
            else:
                msg = "⚠️ Введи число от 5 до 95."
        except ValueError:
            msg = "⚠️ Нужно число, например: 30"
        state["awaiting_input"] = None
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=main_kb())

    elif aw == "set_interval":
        try:
            val = int(text)
            if val >= 60:
                state["interval"] = val
                msg = f"✅ Интервал: <b>{val}с</b>"
            else:
                msg = "⚠️ Минимум 60 секунд."
        except ValueError:
            msg = "⚠️ Нужно число, например: 300"
        state["awaiting_input"] = None
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=main_kb())

    else:
        await update.message.reply_text(
            "Используй /start для управления.", reply_markup=main_kb()
        )


# ─────────────────────────────────────────────
#  ЗАПУСК
# ─────────────────────────────────────────────

def main():
    global bot_app

    if not BOT_TOKEN:
        print("❌ Переменная BOT_TOKEN не задана!")
        print("   На Railway: Settings → Variables → добавь BOT_TOKEN")
        return

    log.info(f"Запуск бота | домен: {DOMAIN} | брендов: {len(state['brands'])}")

    bot_app = Application.builder().token(BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", cmd_start))
    bot_app.add_handler(CallbackQueryHandler(on_button))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    bot_app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
