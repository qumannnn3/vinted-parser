import statistics
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, Sequence, Any


MIN_REQUIRED_PRICES = 3
MIN_REASONABLE_PRICE = 1
PROFITABLE_DISCOUNT_PERCENT = 20


@dataclass(frozen=True)
class MarketPriceResult:
    market_price: int
    discount_percent: int
    comparable_count: int


def _to_int_price(value) -> Optional[int]:
    if value is None or isinstance(value, bool):
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


def _get_id(value: Any, id_getter: Optional[Callable[[Any], Any]] = None):
    if id_getter:
        try:
            return id_getter(value)
        except Exception:
            return None

    if isinstance(value, dict):
        return value.get('id') or value.get('item_id') or value.get('product_id') or value.get('url')

    return getattr(value, 'id', None) or getattr(value, 'item_id', None)


def normalize_prices(
    prices: Iterable,
    price_getter: Optional[Callable[[Any], Any]] = None,
    id_getter: Optional[Callable[[Any], Any]] = None,
    exclude_id: Any = None,
) -> list[int]:
    normalized: list[int] = []

    for item in prices or []:
        item_id = _get_id(item, id_getter=id_getter)
        if exclude_id is not None and item_id is not None and str(item_id) == str(exclude_id):
            continue

        raw_price = price_getter(item) if price_getter else item
        value = _to_int_price(raw_price)
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


def calculate_market_price(
    prices: Iterable,
    min_required: int = MIN_REQUIRED_PRICES,
    price_getter: Optional[Callable[[Any], Any]] = None,
    id_getter: Optional[Callable[[Any], Any]] = None,
    exclude_id: Any = None,
    **kwargs,
) -> Optional[int]:
    normalized = normalize_prices(
        prices,
        price_getter=price_getter,
        id_getter=id_getter,
        exclude_id=exclude_id,
    )

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


def build_market_result(
    current_price: int,
    comparable_prices: Iterable,
    price_getter: Optional[Callable[[Any], Any]] = None,
    id_getter: Optional[Callable[[Any], Any]] = None,
    exclude_id: Any = None,
) -> Optional[MarketPriceResult]:
    prices = normalize_prices(
        comparable_prices,
        price_getter=price_getter,
        id_getter=id_getter,
        exclude_id=exclude_id,
    )
    market_price = calculate_market_price(prices)

    if not market_price:
        return None

    discount_percent = calculate_discount(current_price, market_price)

    return MarketPriceResult(
        market_price=market_price,
        discount_percent=discount_percent,
        comparable_count=len(prices),
    )


def is_profitable(
    current_price: int,
    market_price: int,
    min_discount_percent: int = PROFITABLE_DISCOUNT_PERCENT,
) -> bool:
    return calculate_discount(current_price, market_price) >= min_discount_percent


def market_line_jpy(current_price: int, comparable_prices: Iterable, price_getter=None, id_getter=None, exclude_id=None) -> str:
    result = build_market_result(current_price, comparable_prices, price_getter=price_getter, id_getter=id_getter, exclude_id=exclude_id)
    if not result:
        return ''
    return f'Рынок: ~¥{result.market_price:,}, ниже на {result.discount_percent}%'


def market_line_krw(current_price: int, comparable_prices: Iterable, price_getter=None, id_getter=None, exclude_id=None) -> str:
    result = build_market_result(current_price, comparable_prices, price_getter=price_getter, id_getter=id_getter, exclude_id=exclude_id)
    if not result:
        return ''
    return f'Рынок: ~₩{result.market_price:,}, ниже на {result.discount_percent}%'


def market_line_eur(current_price: int, comparable_prices: Iterable, price_getter=None, id_getter=None, exclude_id=None) -> str:
    result = build_market_result(current_price, comparable_prices, price_getter=price_getter, id_getter=id_getter, exclude_id=exclude_id)
    if not result:
        return ''
    return f'Рынок: ~{result.market_price} EUR, ниже на {result.discount_percent}%'
