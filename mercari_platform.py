import asyncio
import logging
import threading
import time
from datetime import datetime

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, MenuButtonCommands, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from fruits_platform import fruits_loop
try:
    from goofish_platform import gofish_loop
except ModuleNotFoundError:
    try:
        from gofish_platform import gofish_loop
    except ModuleNotFoundError:
        gofish_loop = None
from mercari_platform import mercari_loop
from shared import (
    ALL_BRANDS,
    BOT_TOKEN,
    MSK_TZ,
    PROXY_URL,
    VINTED_REGIONS,
    age_range_label,
    brand_aliases,
    fruits_price_range_label,
    gofish_price_range_label,
    keywords_label,
    log,
    mercari_price_range_label,
    parse_age_range,
    parse_keywords,
    parse_price_range,
    register_chat_id,
    set_telegram_loop,
    state,
    vinted_price_range_label,
)
from access_control import access_enabled, access_prompt_text, authorize_by_code, is_authorized
from vinted_platform import vinted_loop

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

bot_app = None


def _market_title(market=None):
    market = market or state.get("current_market") or "vinted"
    if market == "fruits":
        return "FruitsFamily"
    if market == "gofish":
        return "Gofish"
    return "Mercari.jp" if market == "mercari" else "Vinted"


def _market_running(market=None):
    market = market or state.get("current_market") or "vinted"
    if market == "fruits":
        return state["fruits_running"]
    if market == "gofish":
        return state["gofish_running"]
    return state["mercari_running"] if market == "mercari" else state["vinted_running"]


def _market_stats(market=None):
    market = market or state.get("current_market") or "vinted"
    if market == "fruits":
        return state["fruits_stats"]
    if market == "gofish":
        return state["gofish_stats"]
    return state["mercari_stats"] if market == "mercari" else state["vinted_stats"]


def _price_label(market):
    if market == "fruits":
        return fruits_price_range_label()
    if market == "gofish":
        return gofish_price_range_label()
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
        "└ Выбери площадку для мониторинга\n\n"
        f"🇯🇵 <b>Mercari.jp</b>\n"
