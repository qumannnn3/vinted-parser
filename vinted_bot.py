import asyncio
import logging
import threading
import time
from datetime import datetime

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, MenuButtonCommands, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

try:
    from fruits_platform import fruits_loop
except Exception as e:
    logging.getLogger("parser").error("FruitsFamily не загружен: %s", e)
    fruits_loop = None
from mercari_platform import mercari_loop
try:
    from grailed_platform import grailed_loop
except Exception as e:
    logging.getLogger("parser").error("Grailed не загружен: %s", e)
    grailed_loop = None
from shared import (
    ALL_BRANDS,
    BOT_TOKEN,
    MSK_TZ,
    PROXY_URL,
    VINTED_REGIONS,
    age_range_label,
    brand_aliases,
    fruits_price_range_label,
    grailed_price_range_label,
    keywords_label,
    log,
    mercari_price_range_label,
    parse_age_range,
    parse_keywords,
    parse_price_range,
    register_chat_id,
    current_user_id,
    save_current_user_state,
    set_telegram_loop,
    set_current_user,
    state,
    vinted_price_range_label,
)
from access_control import access_enabled, access_prompt_text, authorize_by_code, is_authorized
from vinted_platform import vinted_loop

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

bot_app = None
START_BRANDING_TEXT = "parser by t.me/huntparser"


def _market_title(market=None):
    market = market or state.get("current_market") or "vinted"
    if market == "fruits":
        return "FruitsFamily"
    if market == "grailed":
        return "Grailed"
    return "Mercari.jp" if market == "mercari" else "Vinted"


def _market_running(market=None):
    market = market or state.get("current_market") or "vinted"
    if market == "fruits":
        return state["fruits_running"]
    if market == "grailed":
        return state["grailed_running"]
    return state["mercari_running"] if market == "mercari" else state["vinted_running"]


def _market_stats(market=None):
    market = market or state.get("current_market") or "vinted"
    if market == "fruits":
        return state["fruits_stats"]
    if market == "grailed":
        return state["grailed_stats"]
    return state["mercari_stats"] if market == "mercari" else state["vinted_stats"]


def _price_label(market):
    if market == "fruits":
        return fruits_price_range_label()
    if market == "grailed":
        return grailed_price_range_label()
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
        "<b>huntparser</b>\n"
        "Choose a marketplace for monitoring\n\n"
        f"<b>Mercari.jp</b>\n"
        f"- Status: {'running' if state['mercari_running'] else 'stopped'}\n"
        f"- Price: {mercari_price_range_label()}\n"
        f"- Age: {_age_label('mercari')}\n"
        f"- Keywords: {_keywords_label('mercari')}\n\n"
        f"<b>FruitsFamily</b>\n"
        f"- Status: {'running' if state['fruits_running'] else 'stopped'}\n"
        f"- Price: {fruits_price_range_label()}\n"
        f"- Age: {_age_label('fruits')}\n"
        f"- Keywords: {_keywords_label('fruits')}\n\n"
        f"<b>Grailed</b>\n"
        f"- Status: {'running' if state['grailed_running'] else 'stopped'}\n"
        f"- Price: {grailed_price_range_label()}\n"
        f"- Age: {_age_label('grailed')}\n"
        f"- Keywords: {_keywords_label('grailed')}\n\n"
        f"<b>Vinted</b>\n"
        f"- Status: {'running' if state['vinted_running'] else 'stopped'}\n"
        f"- Price: {vinted_price_range_label()}\n"
        f"- Age: {_age_label('vinted')}\n"
        f"- Keywords: {_keywords_label('vinted')}"
    )

def main_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Mercari.jp", callback_data="pick_mercari"),
            InlineKeyboardButton("FruitsFamily", callback_data="pick_fruits"),
        ],
        [
            InlineKeyboardButton("Vinted", callback_data="pick_vinted"),
            InlineKeyboardButton("Grailed", callback_data="pick_grailed"),
        ],
        [
            InlineKeyboardButton("Brands", callback_data="brands_0"),
            InlineKeyboardButton("Status", callback_data="status"),
        ],
    ])

def quick_kb():
    return ReplyKeyboardMarkup(
        [
            ["Меню", "Статус"],
            ["⏹ Остановить"],
            ["Mercari.jp", "FruitsFamily", "Vinted"],
            ["Grailed"],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def _running_markets():
    return [
        market for market in ("mercari", "fruits", "vinted", "grailed")
        if state.get(f"{market}_running")
    ]


def _stop_all_markets():
    stopped = _running_markets()
    for market in ("mercari", "fruits", "vinted", "grailed"):
        state[f"{market}_running"] = False
    return stopped


def _stopped_markets_label(markets):
    if not markets:
        return "парсинг уже остановлен"
    return ", ".join(_market_title(market) for market in markets)


def market_text(market=None):
    market = market or state.get("current_market") or "vinted"
    stats = _market_stats(market)
    title = _market_title(market)
    status = "Работает" if _market_running(market) else "Остановлен"
    if market == "mercari":
        area = "jp.mercari.com"
    elif market == "fruits":
        area = "fruitsfamily.com"
    elif market == "grailed":
        area = "grailed.com"
    else:
        area = " ".join(f".{r}" for r in VINTED_REGIONS)
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
    return (
        "<b>Статус</b>\n\n"
        "<b>Vinted</b> online\n"
        "<b>Mercari</b> online\n"
        "<b>FruitsFamily</b> online\n"
        "<b>Grailed</b> online"
    )


BRANDS_PER_PAGE = 12
BRAND_NAME_OVERRIDES = {
    "aape": "Aape",
    "acronym": "ACRONYM",
    "alyx": "ALYX",
    "amiri": "AMIRI",
    "bape": "Bape",
    "cp company": "C.P. Company",
    "dolce&gabbana": "Dolce & Gabbana",
    "dsquared2": "Dsquared2",
    "erd": "ERD",
    "lgb": "LGB",
    "mcm": "MCM",
    "y-3": "Y-3",
}


def _brand_name(brand):
    brand = str(brand or "").strip()
    return BRAND_NAME_OVERRIDES.get(brand.lower(), brand.title())


def _brand_matches_query(brand, query):
    query = str(query or "").lower().strip()
    if not query:
        return True
    texts = [brand, *brand_aliases(brand)]
    return any(query in str(text or "").lower() for text in texts)


def _visible_brands():
    query = state.get("brands_query", "")
    active_only = bool(state.get("brands_active_only"))
    brands = [brand for brand in ALL_BRANDS if _brand_matches_query(brand, query)]
    if active_only:
        brands = [brand for brand in brands if brand in state["active_brands"]]
    return brands


def _brands_pages_count():
    return max(1, (len(_visible_brands()) - 1) // BRANDS_PER_PAGE + 1)


def _normalize_brands_page(page):
    try:
        page = int(page)
    except (TypeError, ValueError):
        page = 0
    return max(0, min(page, _brands_pages_count() - 1))


def brands_text(page=0):
    page = _normalize_brands_page(page)
    visible = _visible_brands()
    query = state.get("brands_query", "").strip()
    active_only = bool(state.get("brands_active_only"))
    active_count = len(state["active_brands"])
    total_count = len(ALL_BRANDS)

    parts = [
        "<b>Бренды</b>",
        f"Активны: <b>{active_count}</b>/<b>{total_count}</b>",
    ]
    if query:
        parts.append(f"Поиск: <code>{query}</code>")
    if active_only:
        parts.append("Режим: показываю только выбранные")
    parts.append(f"Страница: <b>{page + 1}</b>/<b>{_brands_pages_count()}</b>")
    parts.append("")
    parts.append("Нажимай на бренд, чтобы включить или выключить его.")
    if not visible:
        parts.append("\nНичего не найдено. Сбрось поиск или выбери другой запрос.")
    return "\n".join(parts)


def brands_kb(page=0):
    page = _normalize_brands_page(page)
    state["brands_page"] = page

    visible = _visible_brands()
    start = page * BRANDS_PER_PAGE
    chunk = visible[start:start + BRANDS_PER_PAGE]
    rows = []

    buttons = []
    for brand in chunk:
        active = brand in state["active_brands"]
        icon = "✅" if active else "▫️"
        buttons.append(InlineKeyboardButton(f"{icon} {_brand_name(brand)}", callback_data=f"brand_{brand}"))

    for i in range(0, len(buttons), 2):
        rows.append(buttons[i:i + 2])

    if not buttons:
        rows.append([InlineKeyboardButton("Ничего не найдено", callback_data="noop_empty_brands")])

    pages = _brands_pages_count()
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("‹ Назад", callback_data=f"brands_{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop_page"))
    if page + 1 < pages:
        nav.append(InlineKeyboardButton("Вперёд ›", callback_data=f"brands_{page + 1}"))
    rows.append(nav)

    rows.append([
        InlineKeyboardButton("🔎 Поиск", callback_data="brands_search"),
        InlineKeyboardButton("🧹 Сброс", callback_data="brands_clear"),
    ])
    rows.append([
        InlineKeyboardButton("📌 Выбранные" if not state.get("brands_active_only") else "📋 Все бренды", callback_data="brands_active_only"),
    ])
    rows.append([
        InlineKeyboardButton("✅ Выбрать показанные", callback_data="brands_all"),
        InlineKeyboardButton("☐ Снять показанные", callback_data="brands_none"),
    ])
    rows.append([InlineKeyboardButton("↻ Назад", callback_data=f"pick_{state.get('current_market') or 'vinted'}")])
    return InlineKeyboardMarkup(rows)


def _run_market_loop(loop_func, app, user_id):
    set_current_user(user_id)
    loop_func(app)


def _start_market_thread(market):
    user_id = current_user_id()
    if market == "vinted":
        threading.Thread(target=_run_market_loop, args=(vinted_loop, bot_app, user_id), daemon=True).start()
    elif market == "mercari":
        threading.Thread(target=_run_market_loop, args=(mercari_loop, bot_app, user_id), daemon=True).start()
    elif market == "fruits":
        if fruits_loop is None:
            log.error("FruitsFamily не запущен: не удалось импортировать fruits_loop из fruits_platform.py")
            return
        threading.Thread(target=_run_market_loop, args=(fruits_loop, bot_app, user_id), daemon=True).start()
    elif market == "grailed":
        if grailed_loop is None:
            log.error("Grailed не запущен: не удалось импортировать grailed_loop")
            state["grailed_running"] = False
            return
        threading.Thread(target=_run_market_loop, args=(grailed_loop, bot_app, user_id), daemon=True).start()


def _update_user_id(update):
    user = getattr(update, "effective_user", None)
    return user.id if user else None


def _activate_update_user(update):
    user_id = _update_user_id(update)
    chat = getattr(update, "effective_chat", None)
    chat_id = chat.id if chat else None
    return set_current_user(user_id, chat_id)


def _activate_callback_user(q):
    user_id = q.from_user.id if q and q.from_user else None
    chat_id = q.message.chat_id if q and q.message else None
    return set_current_user(user_id, chat_id)


async def _send_main_menu(update: Update, first_line="Панель команд включена"):
    register_chat_id(update.effective_chat.id)
    state["current_market"] = None
    await update.message.reply_text(first_line, reply_markup=quick_kb())
    await update.message.reply_text(main_text(), reply_markup=main_kb(), parse_mode="HTML")


async def _send_access_prompt(update: Update, prefix=None):
    text = access_prompt_text()
    if prefix:
        text = f"{prefix}\n\n{text}"
    await update.message.reply_text(text, reply_markup=ReplyKeyboardRemove(), parse_mode="HTML")


async def _try_unlock_access(update: Update, code_text):
    user_id = _update_user_id(update)
    ok, reason = authorize_by_code(user_id, code_text)
    if not ok and reason == "used":
        await _send_access_prompt(update, "❌ Этот личный код уже использован другим аккаунтом.")
        return False
    if not ok and reason == "save":
        await update.message.reply_text(
            "✅ Код верный, но я не смог сохранить доступ. Проверь логи сервера.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return False
    if not ok:
        await _send_access_prompt(update, "❌ Код не подошел.")
        return False

    log.info("Доступ к боту выдан Telegram user_id=%s", user_id)
    await _send_main_menu(update, "✅ Доступ открыт")
    return True


async def _ensure_message_access(update: Update):
    if is_authorized(_update_user_id(update)):
        return True
    await _send_access_prompt(update)
    return False


async def _ensure_callback_access(q):
    user_id = q.from_user.id if q and q.from_user else None
    if is_authorized(user_id):
        await q.answer()
        return True
    await q.answer("Сначала введи код доступа в чат", show_alert=True)
    return False


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _activate_update_user(update)
    if not is_authorized(_update_user_id(update)):
        await _send_access_prompt(update, START_BRANDING_TEXT)
        return
    await _send_main_menu(update, START_BRANDING_TEXT)


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _activate_update_user(update)
    if not await _ensure_message_access(update):
        return
    register_chat_id(update.effective_chat.id)
    await update.message.reply_text("Панель команд включена", reply_markup=quick_kb())
    await update.message.reply_text(status_text(), reply_markup=main_kb(), parse_mode="HTML")


async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _activate_update_user(update)
    if not await _ensure_message_access(update):
        return
    register_chat_id(update.effective_chat.id)
    stopped = _stop_all_markets()
    await update.message.reply_text(
        f"⏹ Остановлено: <b>{_stopped_markets_label(stopped)}</b>",
        reply_markup=quick_kb(),
        parse_mode="HTML",
    )
    await update.message.reply_text(main_text(), reply_markup=main_kb(), parse_mode="HTML")


async def cmd_access(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _activate_update_user(update)
    if is_authorized(_update_user_id(update)):
        await _send_main_menu(update)
        return

    code = " ".join(ctx.args).strip()
    if code:
        await _try_unlock_access(update, code)
        return
    await _send_access_prompt(update)


async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    _activate_callback_user(q)
    if not await _ensure_callback_access(q):
        return
    register_chat_id(q.message.chat_id)
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

    if data in ("pick_vinted", "pick_mercari", "pick_fruits", "pick_grailed"):
        market = data.split("_", 1)[1]
        state["current_market"] = market
        await edit(market_text(market), market_kb(market))
        return

    if data in ("toggle_vinted", "toggle_mercari", "toggle_fruits", "toggle_grailed"):
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

    if data in ("filters_vinted", "filters_mercari", "filters_fruits", "filters_grailed", "vinted_settings", "mercari_settings", "fruits_settings", "grailed_settings"):
        market = "grailed" if "grailed" in data else ("fruits" if "fruits" in data else ("mercari" if "mercari" in data else "vinted"))
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

    if data in ("price_fruits", "fset_min", "fset_max"):
        state["awaiting"] = "fruits_price_range"
        state["current_market"] = "fruits"
        await edit(
            "Введи диапазон цены FruitsFamily (₩)\n"
            f"Сейчас: <b>{fruits_price_range_label()}</b>\n\n"
            "Например: <code>10000-1000000</code>",
            filters_kb("fruits"),
        )
        return
    if data == "price_grailed":
        state["awaiting"] = "grailed_price_range"
        state["current_market"] = "grailed"
        await edit(
            "Enter Grailed price range ($)\n"
            f"Current: <b>{grailed_price_range_label()}</b>\n\n"
            "Example: <code>10-500</code>",
            filters_kb("grailed"),
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

    if data == "age_fruits":
        state["awaiting"] = "fruits_age_range"
        state["current_market"] = "fruits"
        await edit(
            "Введи время публикации FruitsFamily в часах\n"
            f"Сейчас: <b>{_age_label('fruits')}</b>\n\n"
            "Например: <code>24</code> или <code>6-48</code>",
            filters_kb("fruits"),
        )
        return
    if data == "age_grailed":
        state["awaiting"] = "grailed_age_range"
        state["current_market"] = "grailed"
        await edit(
            "Enter Grailed publish age in hours\n"
            f"Current: <b>{_age_label('grailed')}</b>\n\n"
            "Example: <code>24</code> or <code>6-48</code>",
            filters_kb("grailed"),
        )
        return

    if data in ("keywords_vinted", "keywords_mercari", "keywords_fruits", "keywords_grailed"):
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

    if data == "brands_search":
        state["awaiting"] = "brand_search"
        await edit(
            "<b>Поиск бренда</b>\n\n"
            "Введи часть названия или алиаса.\n"
            "Например: <code>rick</code>, <code>margiela</code>, <code>cdg</code>.\n\n"
            "Чтобы сбросить поиск, отправь <code>-</code>.",
            brands_kb(state.get("brands_page", 0)),
        )
        return

    if data == "brands_clear":
        state["brands_query"] = ""
        state["brands_active_only"] = False
        await edit(brands_text(0), brands_kb(0))
        return

    if data == "brands_active_only":
        state["brands_active_only"] = not bool(state.get("brands_active_only"))
        await edit(brands_text(0), brands_kb(0))
        return

    if data.startswith("brands_") and data not in ("brands_all", "brands_none"):
        try:
            page = int(data.split("_")[1])
        except (IndexError, ValueError):
            page = 0
        page = _normalize_brands_page(page)
        await edit(brands_text(page), brands_kb(page))
        return

    if data == "brands_all":
        targets = _visible_brands()
        if not targets:
            await q.answer("Нет брендов для выбора", show_alert=True)
            return
        state["active_brands"].update(targets)
        await edit(brands_text(state.get("brands_page", 0)), brands_kb(state.get("brands_page", 0)))
        return

    if data == "brands_none":
        targets = _visible_brands()
        if not targets:
            await q.answer("Нет брендов для снятия", show_alert=True)
            return
        state["active_brands"].difference_update(targets)
        await edit(brands_text(state.get("brands_page", 0)), brands_kb(state.get("brands_page", 0)))
        return

    if data.startswith("brand_"):
        brand = data[6:]
        if brand in state["active_brands"]:
            state["active_brands"].discard(brand)
            await q.answer(f"Выключено: {_brand_name(brand)}")
        else:
            state["active_brands"].add(brand)
            await q.answer(f"Включено: {_brand_name(brand)}")
        page = next((i // BRANDS_PER_PAGE for i, b in enumerate(_visible_brands()) if b == brand), state.get("brands_page", 0))
        await edit(brands_text(page), brands_kb(page))
        return

    if data.startswith("noop_"):
        await q.answer("Только навигация", show_alert=True)
        return


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _activate_update_user(update)
    raw_text = update.message.text.strip()
    button_text = raw_text.lower()

    if not is_authorized(_update_user_id(update)):
        await _try_unlock_access(update, raw_text)
        return

    aw = state.get("awaiting")

    if button_text in ("меню", "menu", "/menu", "/start"):
        state["awaiting"] = None
        state["current_market"] = None
        await update.message.reply_text(main_text(), reply_markup=main_kb(), parse_mode="HTML")
        return

    if button_text in ("статус", "status", "/status"):
        state["awaiting"] = None
        await update.message.reply_text(status_text(), reply_markup=main_kb(), parse_mode="HTML")
        return

    if button_text in ("⏹ остановить", "остановить", "стоп", "stop", "/stop"):
        state["awaiting"] = None
        stopped = _stop_all_markets()
        await update.message.reply_text(
            f"⏹ Остановлено: <b>{_stopped_markets_label(stopped)}</b>\n\n{main_text()}",
            reply_markup=main_kb(),
            parse_mode="HTML",
        )
        return

    if button_text in ("mercari", "mercari.jp", "меркари"):
        state["awaiting"] = None
        state["current_market"] = "mercari"
        await update.message.reply_text(market_text("mercari"), reply_markup=market_kb("mercari"), parse_mode="HTML")
        return

    if button_text in ("fruits", "fruitsfamily", "fruits family", "фрутс", "фрутсфэмили"):
        state["awaiting"] = None
        state["current_market"] = "fruits"
        await update.message.reply_text(market_text("fruits"), reply_markup=market_kb("fruits"), parse_mode="HTML")
        return

    if button_text in ("vinted", "винтед"):
        state["awaiting"] = None
        state["current_market"] = "vinted"
        await update.message.reply_text(market_text("vinted"), reply_markup=market_kb("vinted"), parse_mode="HTML")
        return
    if button_text in ("grailed", "грейлд"):
        state["awaiting"] = None
        state["current_market"] = "grailed"
        await update.message.reply_text(market_text("grailed"), reply_markup=market_kb("grailed"), parse_mode="HTML")
        return

    text = raw_text.replace(",", ".")

    if aw == "brand_search":
        query = raw_text.strip()
        state["brands_query"] = "" if query.lower() in ("-", "—", "сброс", "clear") else query
        state["brands_active_only"] = False
        state["awaiting"] = None
        await update.message.reply_text(
            brands_text(0),
            parse_mode="HTML",
            reply_markup=brands_kb(0),
        )
        return

    if aw in ("vinted_keywords", "mercari_keywords", "fruits_keywords", "grailed_keywords"):
        market = "grailed" if aw == "grailed_keywords" else ("fruits" if aw == "fruits_keywords" else ("mercari" if aw == "mercari_keywords" else "vinted"))
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

    if aw in ("vinted_price_range", "mercari_price_range", "fruits_price_range", "grailed_price_range"):
        market = "grailed" if aw == "grailed_price_range" else ("fruits" if aw == "fruits_price_range" else ("mercari" if aw == "mercari_price_range" else "vinted"))
        try:
            min_price, max_price = parse_price_range(text, is_int=(market in ("mercari", "fruits")))
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
            example = "10000-1000000" if market == "fruits" else ("1000-50000" if market == "mercari" else "10-500")
            await update.message.reply_text(f"Нужен диапазон цены. Например: {example}", reply_markup=filters_kb(market))
        return

    if aw in ("vinted_age", "vinted_age_range", "mercari_age_range", "fruits_age_range", "grailed_age_range"):
        market = "grailed" if aw == "grailed_age_range" else ("fruits" if aw == "fruits_age_range" else ("mercari" if aw == "mercari_age_range" else "vinted"))
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
    set_telegram_loop(asyncio.get_running_loop())
    commands = [
        BotCommand("start", "Запустить бота"),
        BotCommand("menu", "Главное меню"),
        BotCommand("status", "Статус мониторинга"),
        BotCommand("stop", "Остановить парсинг"),
        BotCommand("access", "Ввести код доступа"),
    ]
    try:
        await app.bot.set_my_commands(commands)
        await app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
    except Exception as e:
        log.warning("Telegram command setup failed, continuing startup: %s", e)


async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    if ctx.error:
        log.error(
            "Telegram handler failed",
            exc_info=(type(ctx.error), ctx.error, ctx.error.__traceback__),
        )
    else:
        log.error("Telegram handler failed without exception details")


def _autosave(handler):
    async def wrapped(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        try:
            return await handler(update, ctx)
        finally:
            save_current_user_state()
    return wrapped


def main():
    global bot_app
    if not BOT_TOKEN:
        print("BOT_TOKEN не задан!")
        time.sleep(300)
        return

    log.info("Запуск | брендов: %s | приватный доступ: %s", len(ALL_BRANDS), "включен" if access_enabled() else "выключен")
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
    bot_app.add_handler(CommandHandler(["start", "menu"], _autosave(cmd_start)))
    bot_app.add_handler(CommandHandler("status", _autosave(cmd_status)))
    bot_app.add_handler(CommandHandler("stop", _autosave(cmd_stop)))
    bot_app.add_handler(CommandHandler("access", _autosave(cmd_access)))
    bot_app.add_handler(CallbackQueryHandler(_autosave(on_button)))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _autosave(on_text)))
    bot_app.add_error_handler(on_error)
    bot_app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True, timeout=30)


if __name__ == "__main__":
    while True:
        try:
            main()
            break
        except (KeyboardInterrupt, SystemExit):
            log.info("Бот остановлен")
            break
        except RuntimeError as e:
            if "Event loop is closed" in str(e):
                log.info("Event loop closed during shutdown, exiting")
                break
            log.exception("Бот упал: %s. Перезапуск через 15с...", e)
            time.sleep(15)
        except Exception as e:
            log.exception("Бот упал: %s. Перезапуск через 15с...", e)
            time.sleep(15)
