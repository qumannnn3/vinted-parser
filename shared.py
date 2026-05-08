import logging
import os
import re
import time
import asyncio
import concurrent.futures
from datetime import datetime, timedelta, timezone

import requests

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
PROXY_URL = os.environ.get("PROXY_URL", "")

VINTED_REGIONS = {
    "pl": "www.vinted.pl",
    "lt": "www.vinted.lt",
    "lv": "www.vinted.lv",
}
CATALOG_IDS = [1, 3, 5, 9, 7, 12]
MAX_AGE_HOURS = 24

try:
    MERCARI_MIN_MARKET_SAMPLES = max(1, int(os.environ.get("MERCARI_MIN_MARKET_SAMPLES", "1")))
except ValueError:
    MERCARI_MIN_MARKET_SAMPLES = 1

try:
    MERCARI_MAX_MARKET_RATIO = float(os.environ.get("MERCARI_MAX_MARKET_RATIO", "0.90"))
except ValueError:
    MERCARI_MAX_MARKET_RATIO = 0.90

BAD_WORDS = [
    "pieluchy", "pampers", "baby", "dziecko", "dla dzieci", "подгузники", "детское",
    "nosidelko", "fotelik", "wozek", "kocyk", "smoczek", "lozeczko",
    "underwear", "socks", "bielizna", "majtki", "skarpety", "rajstopy",
    "biustonosz", "bokserki", "stringi", "figi",
    "kask", "rower", "hulajnoga", "rolki", "narty", "deska",
    "telefon", "laptop", "tablet", "konsola",
    "perfumy", "krem", "szampon",
    "ksiazka", "zabawka", "puzzle", "klocki",
    "posciel", "poduszka", "koldra", "recznik", "zaslona",
]

DEEP_FASHION_BLOCKED_WORDS = [
    "novelty", "ノベルティ", "sample", "gift", "promo", "limited gift", "付録",
    "mirror", "ミラー", "鏡", "basket", "バスケット", "籠", "かご",
    "cosmetic", "makeup", "化粧", "メイク", "ポーチのみ", "case only",
    "tableware", "plate", "cup", "mug", "bottle", "glass", "皿", "カップ", "マグ",
    "interior", "home", "room", "blanket", "pillow", "towel", "rug",
    "キッチン", "インテリア", "タオル", "ブランケット", "クッション",
    "baby", "kids", "child", "children", "ベビー", "キッズ", "子供",
]

DEEP_FASHION_SIZE_PATTERN = re.compile(
    r"(?<![a-z0-9])("
    r"xxs|xs|s|m|l|xl|xxl|xxxl|"
    r"it\s?\d{2}|eu\s?\d{2}|jp\s?\d{1,2}|us\s?\d{1,2}|"
    r"\d{2}(?:\.\d)?\s?cm"
    r")(?![a-z0-9])",
    re.IGNORECASE,
)

ALL_BRANDS = [
    "stone island", "balenciaga", "raf simons", "bape", "aape",
    "gucci", "chanel", "jeremy scott", "undercover", "comme des garcons",
    "yohji yamamoto", "vetements", "palm angels", "maison margiela",
    "givenchy", "burberry", "supreme", "amiri", "acne studios", "alyx",
    "tornado mart", "14th addiction", "project g/r", "hysteric glamour",
    "dolce&gabbana", "number nine", "grailz project", "y-3", "lgb",
    "ed hardy", "mcm", "true religion", "guiseppe zanotti", "arcteryx",
    "rick owens", "evisu", "saint laurent", "neighborhood", "prada",
    "dior", "jaded london", "diesel", "alpha industries", "glory boyz",
    "ralph lauren", "louis vuitton", "phillipp plein", "versace",
    "rock revival", "armani", "mastermind", "alexander mqueen",
    "cav empt", "buffalo bobs", "billionaire boys club", "acronym",
    "swear", "vivienne westwood", "balmain", "issey miyake",
    "if six was nine", "20471120", "cp company", "laoboutin",
    "robin jeans", "гоша рубчинский", "ferragamo", "salem",
    "marcelo burlon", "erd", "chrome hearts", "isabel marant",
    "mihara yasuhiro", "carol cristian poell", "alice hollywood",
    "moncler", "valentino", "hysterics", "helmut lang",
    "maison martin margiela", "dsquared2",
]

BRAND_ALIASES = {
    "stone island": ["stoneisland", "stone isl", "ストーンアイランド"],
    "balenciaga": ["バレンシアガ", "발렌시아가"],
    "raf simons": ["rafsimons", "ラフシモンズ"],
    "bape": ["a bathing ape", "abathingape", "エイプ", "베이프"],
    "aape": ["aape by a bathing ape"],
    "gucci": ["グッチ", "구찌"],
    "chanel": ["シャネル", "샤넬"],
    "jeremy scott": ["jeremyscott"],
    "undercover": ["under cover", "アンダーカバー"],
    "comme des garcons": ["comme des garçons", "comme des garcon", "cdg", "コムデギャルソン", "꼼데가르송"],
    "yohji yamamoto": ["yohji", "ヨウジヤマモト", "요지 야마모토"],
    "vetements": ["vetement", "ヴェトモン", "베트멍"],
    "palm angels": ["palmangels"],
    "maison margiela": ["margiela", "maison martin margiela", "martin margiela", "마르지엘라"],
    "givenchy": ["ジバンシィ", "ジバンシー"],
