import statistics
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence


MIN_REQUIRED_PRICES = 3
MIN_REASONABLE_PRICE = 1
PROFITABLE_DISCOUNT_PERCENT = 20


@dataclass(frozen=True)
class MarketPriceResult:
    market_price: int
    discount_percent: int
    comparable_count: int


def _to_int_price(value) -> Optional[int]:
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        price = int(value)
        return price if price >= MIN_REASONABLE_PRICE else None

    if isinstance(value, str):
        cleaned = (
            value.replace(',', '')
            .replace('¥', '')
            .replace('₩', '')
            .replace('€', '')
            .replace('$', '')
            .replace(' ', '')
            .strip()
        )
        digits = ''.join(ch for ch in cleaned if ch.isdigit())
        if not digits:
            return None
        price = int(digits)
        return price if price >= MIN_REASONABLE_PRICE else None

    return None


def normalize_prices(prices: Iterable) -> list[int]:
    normalized: list[int] = []
    for price in prices or []:
        value = _to_int_price(price)
        if value is not None:
            normalized.append(value)
    return normalized


def remove_outliers(prices: Sequence[int]) -> list[int]:
    prices = sorted(int(p) for p in prices if p and p > 0)

    if len(prices) < 4:
        return prices

    q1, _, q3 = statistics.quantiles(prices, n=4, method='inclusive')
    iqr = q3 - q1

    if iqr <= 0:
        return prices

    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr

    return [p for p in prices if lower <= p <= upper]


def calculate_market_price(prices: Iterable, min_required: int = MIN_REQUIRED_PRICES) -> Optional[int]:
    normalized = normalize_prices(prices)

    if len(normalized) < min_required:
        return None

    filtered = remove_outliers(normalized)

    if len(filtered) < min_required:
        return None

    median_price = statistics.median(filtered)
    average_price = statistics.mean(filtered)

    return int(round((median_price * 0.7) + (average_price * 0.3)))


def calculate_discount(price: int, market_price: int) -> int:
    current = _to_int_price(price)
    market = _to_int_price(market_price)

    if not current or not market or market <= 0:
        return 0

    return int(round((1 - (current / market)) * 100))


def build_market_result(current_price: int, comparable_prices: Iterable) -> Optional[MarketPriceResult]:
    prices = normalize_prices(comparable_prices)
    market_price = calculate_market_price(prices)

    if not market_price:
        return None

    discount_percent = calculate_discount(current_price, market_price)

    return MarketPriceResult(
        market_price=market_price,
        discount_percent=discount_percent,
        comparable_count=len(prices),
    )


def is_profitable(current_price: int, market_price: int, min_discount_percent: int = PROFITABLE_DISCOUNT_PERCENT) -> bool:
    return calculate_discount(current_price, market_price) >= min_discount_percent


def market_line_jpy(current_price: int, comparable_prices: Iterable) -> str:
    result = build_market_result(current_price, comparable_prices)
    if not result:
        return ''

    return f'Рынок: ~¥{result.market_price:,}, ниже на {result.discount_percent}%'


def market_line_krw(current_price: int, comparable_prices: Iterable) -> str:
    result = build_market_result(current_price, comparable_prices)
    if not result:
        return ''

    return f'Рынок: ~₩{result.market_price:,}, ниже на {result.discount_percent}%'


def market_line_eur(current_price: int, comparable_prices: Iterable) -> str:
    result = build_market_result(current_price, comparable_prices)
    if not result:
        return ''

    return f'Рынок: ~{result.market_price} EUR, ниже на {result.discount_percent}%'
