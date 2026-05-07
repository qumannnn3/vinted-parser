import logging
import threading
import time
from datetime import datetime

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, MenuButtonCommands, ReplyKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from mercari_platform import mercari_loop
from shared import (
    ALL_BRANDS,
    BOT_TOKEN,
    MSK_TZ,
    PROXY_URL,
    VINTED_REGIONS,
    age_range_label,
    keywords_label,
    log,
    mercari_price_range_label,
    parse_age_range,
    parse_keywords,
    parse_price_range,
    state,
    vinted_price_range_label,
)
from vinted_platform import vinted_loop

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

bot_app = None


def _market_title(market=None):
    market = market or state.get("current_market") or "vinted"
    return "Mercari.jp" if market == "mercari" else "Vinted"


def _market_running(market=None):
    market = market or state.get("current_market") or "vinted"
    return state["mercari_running"] if market == "mercari" else state["vinted_running"]


def _market_stats(market=None):
    market = market or state.get("current_market") or "vinted"
    return state["mercari_stats"] if market == "mercari" else state["vinted_stats"]


def _price_label(market):
    return mercari_price_range_label() if market == "mercari" else vinted_price_range_label()


def _age_label(market):
    return age_range_label(
        state[f"{market}_min_age_hours"],
        state[f"{market}_max_age_hours"],
    )


def _keywords_label(market):
    return keywords_label(market)


def main_text():
    return (
        "<b>Parser #1</b>\n"
        "└ Выбери площадку для мониторинга\n\n"
        f"🇯🇵 <b>Mercari.jp</b>\n"
        f"└ Статус: {'работает' if state['mercari_running'] else 'остановлен'}\n"
        f"└ Цена: {mercari_price_range_label()}\n"
        f"└ Публикация: {_age_label('mercari')}\n"
        f"└ Ключи: {_keywords_label('mercari')}\n\n"
        f"🌍 <b>Vinted</b>\n"
        f"└ Статус: {'работает' if state['vinted_running'] else 'остановлен'}\n"
        f"└ Цена: {vinted_price_range_label()}\n"
        f"└ Публикация: {_age_label('vinted')}\n"
        f"└ Ключи: {_keywords_label('vinted')}"
    )


def main_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🇯🇵 Mercari.jp", callback_data="pick_mercari"),
            InlineKeyboardButton("🌍 Vinted", callback_data="pick_vinted"),
        ],
        [
            InlineKeyboardButton("👕 Бренды", callback_data="brands_0"),
            InlineKeyboardButton("ⓘ Статус", callback_data="status"),
        ],
    ])


def quick_kb():
    return ReplyKeyboardMarkup(
        [
            ["Меню", "Статус"],
            ["Mercari.jp", "Vinted"],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def market_text(market=None):
    market = market or state.get("current_market") or "vinted"
    stats = _market_stats(market)
    title = _market_title(market)
    status = "Работает" if _market_running(market) else "Остановлен"
    area = "jp.mercari.com" if market == "mercari" else " ".join(f".{r}" for r in VINTED_REGIONS)
    last = datetime.now(MSK_TZ).strftime("%H:%M МСК")
    return (
        f"<b>{title}</b>\n"
        f"└ {area}\n\n"
        f"ⓘ <b>Статус</b>\n"
        f"└ {status}\n\n"
        f"⚭ <b>Активных брендов</b>\n"
        f"└ {len(state['active_brands'])}\n\n"
        f"◷ <b>Последнее обновление</b>\n"
        f"└ {last}\n\n"
        f"⌁ <b>Фильтры</b>\n"
        f"└ Цена: {_price_label(market)} | Публикация: {_age_label(market)}\n"
        f"└ Ключи: {_keywords_label(market)}\n"
        f"└ Найдено: {stats['found']} | Циклов: {stats['cycles']}"
    )


def market_kb(market=None):
    market = market or state.get("current_market") or "vinted"
    run_text = "⏹ Остановить" if _market_running(market) else "▶ Запустить"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(run_text, callback_data=f"toggle_{market}")],
        [
            InlineKeyboardButton("ⓘ Фильтры", callback_data=f"filters_{market}"),
            InlineKeyboardButton(f"ⓘ {_market_title(market)}", callback_data=f"pick_{market}"),
        ],
        [InlineKeyboardButton("↻ Сменить площадку", callback_data="back")],
    ])


def filters_text(market=None):
    market = market or state.get("current_market") or "vinted"
    return (
        f"<b>{_market_title(market)} • Фильтры</b>\n\n"
        "<b>Цена</b>\n"
        f"└ {_price_label(market)}\n\n"
        "<b>Время публикации</b>\n"
        f"└ {_age_label(market)}\n\n"
        "<b>Ключевые слова</b>\n"
        f"└ {_keywords_label(market)}"
    )


def filters_kb(market=None):
    market = market or state.get("current_market") or "vinted"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Цена", callback_data=f"price_{market}"),
            InlineKeyboardButton("Время", callback_data=f"age_{market}"),
            InlineKeyboardButton("Ключи", callback_data=f"keywords_{market}"),
        ],
        [InlineKeyboardButton("⏹ Остановить" if _market_running(market) else "▶ Запустить", callback_data=f"toggle_{market}")],
        [
            InlineKeyboardButton("ⓘ Фильтры", callback_data=f"filters_{market}"),
            InlineKeyboardButton(f"ⓘ {_market_title(market)}", callback_data=f"pick_{market}"),
        ],
        [InlineKeyboardButton("↻ Сменить площадку", callback_data="back")],
    ])


def status_text():
    tf = state.get("_vinted_ts_field") or "не определено"
    return (
        "<b>Статус</b>\n\n"
        f"<b>Vinted</b> {'🟢' if state['vinted_running'] else '🔴'}\n"
        f"└ Циклов: {state['vinted_stats']['cycles']} | Находок: {state['vinted_stats']['found']}\n"
        f"└ Цена: {vinted_price_range_label()} | {_age_label('vinted')}\n"
        f"└ Ключи: {_keywords_label('vinted')}\n"
        f"└ Поле времени: <code>{tf}</code>\n\n"
        f"<b>Mercari.jp</b> {'🟢' if state['mercari_running'] else '🔴'}\n"
        f"└ Циклов: {state['mercari_stats']['cycles']} | Находок: {state['mercari_stats']['found']}\n"
        f"└ Цена: {mercari_price_range_label()} | {_age_label('mercari')}\n"
        f"└ Ключи: {_keywords_label('mercari')}\n\n"
        f"Брендов: {len(state['active_brands'])}/{len(ALL_BRANDS)}"
    )


def brands_kb(page=0):
    per_page = 5
    start = page * per_page
    chunk = ALL_BRANDS[start:start + per_page]
    rows = []
    for brand in chunk:
        active = brand in state["active_brands"]
        icon = "✅" if active else "☐"
        rows.append([InlineKeyboardButton(f"{icon} {brand.title()}", callback_data=f"brand_{brand}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("‹", callback_data=f"brands_{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page + 1}/{(len(ALL_BRANDS) - 1) // per_page + 1}", callback_data="noop_page"))
    if start + per_page < len(ALL_BRANDS):
        nav.append(InlineKeyboardButton("›", callback_data=f"brands_{page + 1}"))
    rows.append(nav)
    rows.append([
        InlineKeyboardButton("✅ Все", callback_data="brands_all"),
        InlineKeyboardButton("☐ Снять все", callback_data="brands_none"),
    ])
    rows.append([InlineKeyboardButton("↻ Назад", callback_data=f"pick_{state.get('current_market') or 'vinted'}")])
    return InlineKeyboardMarkup(rows)


def _start_market_thread(market):
    if market == "vinted":
        threading.Thread(target=vinted_loop, args=(bot_app,), daemon=True).start()
    elif market == "mercari":
        threading.Thread(target=mercari_loop, args=(bot_app,), daemon=True).start()


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    state["current_market"] = None
    await update.message.reply_text("Панель команд включена", reply_markup=quick_kb())
    await update.message.reply_text(main_text(), reply_markup=main_kb(), parse_mode="HTML")


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    await update.message.reply_text("Панель команд включена", reply_markup=quick_kb())
    await update.message.reply_text(status_text(), reply_markup=main_kb(), parse_mode="HTML")


async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    state["chat_id"] = q.message.chat_id
    data = q.data

    async def edit(text, kb=None):
        try:
            await q.edit_message_text(text, reply_markup=kb or main_kb(), parse_mode="HTML")
        except Exception:
            await q.message.reply_text(text, reply_markup=kb or main_kb(), parse_mode="HTML")

    if data in ("back", "main"):
        state["current_market"] = None
        await edit(main_text(), main_kb())
        return

    if data in ("pick_vinted", "pick_mercari"):
        market = data.split("_", 1)[1]
        state["current_market"] = market
        await edit(market_text(market), market_kb(market))
        return

    if data in ("toggle_vinted", "toggle_mercari"):
        market = data.split("_", 1)[1]
        state["current_market"] = market
        running_key = f"{market}_running"
        if state[running_key]:
            state[running_key] = False
        else:
            if not state["active_brands"]:
                await q.answer("Выбери хотя бы один бренд", show_alert=True)
                return
            state[running_key] = True
            _start_market_thread(market)
        await edit(market_text(market), market_kb(market))
        return

    if data in ("filters_vinted", "filters_mercari", "vinted_settings", "mercari_settings"):
        market = "mercari" if "mercari" in data else "vinted"
        state["current_market"] = market
        await edit(filters_text(market), filters_kb(market))
        return

    if data in ("price_vinted", "vset_min", "vset_max"):
        state["awaiting"] = "vinted_price_range"
        state["current_market"] = "vinted"
        await edit(
            "Введи диапазон цены Vinted (€)\n"
            f"Сейчас: <b>{vinted_price_range_label()}</b>\n\n"
            "Например: <code>10-500</code>",
            filters_kb("vinted"),
        )
        return

    if data in ("price_mercari", "mset_min", "mset_max"):
        state["awaiting"] = "mercari_price_range"
        state["current_market"] = "mercari"
        await edit(
            "Введи диапазон цены Mercari (¥)\n"
            f"Сейчас: <b>{mercari_price_range_label()}</b>\n\n"
            "Например: <code>1000-50000</code>",
            filters_kb("mercari"),
        )
        return

    if data in ("age_vinted", "vset_age"):
        state["awaiting"] = "vinted_age_range"
        state["current_market"] = "vinted"
        await edit(
            "Введи время публикации Vinted в часах\n"
            f"Сейчас: <b>{_age_label('vinted')}</b>\n\n"
            "Например: <code>24</code> или <code>6-48</code>",
            filters_kb("vinted"),
        )
        return

    if data == "age_mercari":
        state["awaiting"] = "mercari_age_range"
        state["current_market"] = "mercari"
        await edit(
            "Введи время публикации Mercari в часах\n"
            f"Сейчас: <b>{_age_label('mercari')}</b>\n\n"
            "Например: <code>24</code> или <code>6-48</code>",
            filters_kb("mercari"),
        )
        return

    if data in ("keywords_vinted", "keywords_mercari"):
        market = data.split("_", 1)[1]
        state["awaiting"] = f"{market}_keywords"
        state["current_market"] = market
        await edit(
            f"Введи ключевые слова для {_market_title(market)} через запятую\n"
            f"Сейчас: <b>{_keywords_label(market)}</b>\n\n"
            "Например: <code>hoodie, jacket, bag</code>\n"
            "Чтобы очистить: <code>-</code>",
            filters_kb(market),
        )
        return

    if data == "status":
        await edit(status_text(), main_kb())
        return

    if data.startswith("brands_") and data not in ("brands_all", "brands_none"):
        try:
            page = int(data.split("_")[1])
        except (IndexError, ValueError):
            page = 0
        await edit(
            f"<b>Бренды</b>\n\nАктивны: {len(state['active_brands'])}/{len(ALL_BRANDS)}\n"
            f"Страница {page + 1}/{(len(ALL_BRANDS) - 1) // 5 + 1}",
            brands_kb(page),
        )
        return

    if data == "brands_all":
        state["active_brands"] = set(ALL_BRANDS)
        await edit(f"<b>Бренды</b>\n\nВсе {len(ALL_BRANDS)} брендов активны.", brands_kb(0))
        return

    if data == "brands_none":
        state["active_brands"] = set()
        await edit("<b>Бренды</b>\n\nВсе бренды отключены.", brands_kb(0))
        return

    if data.startswith("brand_"):
        brand = data[6:]
        if brand in state["active_brands"]:
            state["active_brands"].discard(brand)
        else:
            state["active_brands"].add(brand)
        page = next((i // 5 for i, b in enumerate(ALL_BRANDS) if b == brand), 0)
        await edit(f"<b>Бренды</b>\n\nАктивны: {len(state['active_brands'])}/{len(ALL_BRANDS)}", brands_kb(page))
        return

    if data.startswith("noop_"):
        await q.answer("Только навигация", show_alert=True)
        return


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    aw = state.get("awaiting")
    raw_text = update.message.text.strip()
    button_text = raw_text.lower()

    if button_text in ("меню", "menu", "/menu", "/start"):
        state["awaiting"] = None
        state["current_market"] = None
        await update.message.reply_text(main_text(), reply_markup=main_kb(), parse_mode="HTML")
        return

    if button_text in ("статус", "status", "/status"):
        state["awaiting"] = None
        await update.message.reply_text(status_text(), reply_markup=main_kb(), parse_mode="HTML")
        return

    if button_text in ("mercari", "mercari.jp", "меркари"):
        state["awaiting"] = None
        state["current_market"] = "mercari"
        await update.message.reply_text(market_text("mercari"), reply_markup=market_kb("mercari"), parse_mode="HTML")
        return

    if button_text in ("vinted", "винтед"):
        state["awaiting"] = None
        state["current_market"] = "vinted"
        await update.message.reply_text(market_text("vinted"), reply_markup=market_kb("vinted"), parse_mode="HTML")
        return

    text = raw_text.replace(",", ".")

    if aw in ("vinted_keywords", "mercari_keywords"):
        market = "mercari" if aw == "mercari_keywords" else "vinted"
        keywords = parse_keywords(raw_text)
        state[f"{market}_keywords"] = keywords
        state["awaiting"] = None
        state["current_market"] = market
        await update.message.reply_text(
            f"✅ Ключевые слова: <b>{_keywords_label(market)}</b>\n\n{filters_text(market)}",
            parse_mode="HTML",
            reply_markup=filters_kb(market),
        )
        return

    if aw in ("vinted_price_range", "mercari_price_range"):
        market = "mercari" if aw == "mercari_price_range" else "vinted"
        try:
            min_price, max_price = parse_price_range(text, is_int=(market == "mercari"))
            state[f"{market}_min"] = min_price
            state[f"{market}_max"] = max_price
            state["awaiting"] = None
            state["current_market"] = market
            await update.message.reply_text(
                f"✅ Диапазон цены: <b>{_price_label(market)}</b>\n\n{filters_text(market)}",
                parse_mode="HTML",
                reply_markup=filters_kb(market),
            )
        except ValueError:
            example = "1000-50000" if market == "mercari" else "10-500"
            await update.message.reply_text(f"Нужен диапазон цены. Например: {example}", reply_markup=filters_kb(market))
        return

    if aw in ("vinted_age", "vinted_age_range", "mercari_age_range"):
        market = "mercari" if aw == "mercari_age_range" else "vinted"
        try:
            min_age, max_age = parse_age_range(text)
            state[f"{market}_min_age_hours"] = min_age
            state[f"{market}_max_age_hours"] = max_age
            state["awaiting"] = None
            state["current_market"] = market
            await update.message.reply_text(
                f"✅ Время публикации: <b>{_age_label(market)}</b>\n\n{filters_text(market)}",
                parse_mode="HTML",
                reply_markup=filters_kb(market),
            )
        except ValueError:
            await update.message.reply_text(
                "Нужно число часов или диапазон. Например: 24 или 6-48",
                reply_markup=filters_kb(market),
            )
        return

    await update.message.reply_text(main_text(), reply_markup=main_kb(), parse_mode="HTML")


async def setup_bot_commands(app):
    commands = [
        BotCommand("start", "Запустить бота"),
        BotCommand("menu", "Главное меню"),
        BotCommand("status", "Статус мониторинга"),
    ]
    await app.bot.set_my_commands(commands)
    await app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())


def main():
    global bot_app
    if not BOT_TOKEN:
        print("BOT_TOKEN не задан!")
        time.sleep(300)
        return

    log.info("Запуск | брендов: %s", len(ALL_BRANDS))
    builder = Application.builder().token(BOT_TOKEN)
    if PROXY_URL:
        if hasattr(builder, "proxy_url"):
            builder = builder.proxy_url(PROXY_URL)
        elif hasattr(builder, "proxy"):
            builder = builder.proxy(PROXY_URL)
        if hasattr(builder, "get_updates_proxy_url"):
            builder = builder.get_updates_proxy_url(PROXY_URL)
        elif hasattr(builder, "get_updates_proxy"):
            builder = builder.get_updates_proxy(PROXY_URL)

    bot_app = (
        builder
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(30)
        .post_init(setup_bot_commands)
        .build()
    )
    bot_app.add_handler(CommandHandler(["start", "menu"], cmd_start))
    bot_app.add_handler(CommandHandler("status", cmd_status))
    bot_app.add_handler(CallbackQueryHandler(on_button))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    bot_app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True, timeout=30)


if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            log.error("Бот упал: %s. Перезапуск через 15с...", e)
            time.sleep(15)
