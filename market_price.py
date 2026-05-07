# market_price.py

```python
import statistics
from typing import List, Optional


MIN_REQUIRED_PRICES = 3


class MarketPriceResult:
    def __init__(self, market_price: int, discount_percent: int):
        self.market_price = market_price
        self.discount_percent = discount_percent



def remove_outliers(prices: List[int]) -> List[int]:
    if len(prices) < 4:
        return prices

    prices = sorted(prices)

    q1 = statistics.quantiles(prices, n=4)[0]
    q3 = statistics.quantiles(prices, n=4)[2]

    iqr = q3 - q1

    lower = q1 - (1.5 * iqr)
    upper = q3 + (1.5 * iqr)

    return [p for p in prices if lower <= p <= upper]



def calculate_discount(price: int, market_price: int) -> int:
    if market_price <= 0:
        return 0

    return round((1 - (price / market_price)) * 100)



def calculate_market_price(prices: List[int]) -> Optional[int]:
    prices = [int(p) for p in prices if p and p > 0]

    if len(prices) < MIN_REQUIRED_PRICES:
        return None

    filtered = remove_outliers(prices)

    if len(filtered) < MIN_REQUIRED_PRICES:
        return None

    median_price = statistics.median(filtered)
    average_price = statistics.mean(filtered)

    market_price = int((median_price * 0.7) + (average_price * 0.3))

    return market_price



def build_market_result(current_price: int, comparable_prices: List[int]):
    market_price = calculate_market_price(comparable_prices)

    if not market_price:
        return None

    discount_percent = calculate_discount(current_price, market_price)

    return MarketPriceResult(
        market_price=market_price,
        discount_percent=discount_percent,
    )


async def format_market_text(current_price: int, comparable_prices: List[int]):
    result = build_market_result(current_price, comparable_prices)

    if not result:
        return ""

    emoji = ""

    if result.discount_percent >= 60:
        emoji = "🔥"
    elif result.discount_percent >= 40:
        emoji = "✅"

    return (
        f"Рынок: ~¥{result.market_price} "
        f"(ниже на {result.discount_percent}%) {emoji}"
    )
```

Использование в:

```python
from market_price import format_market_text
```

Mercari:

```python
market_text = await format_market_text(price, comparable_prices)
```

Fruits:

```python
market_text = await format_market_text(price, comparable_prices)
```

Vinted:

```python
market_text = await format_market_text(price, comparable_prices)
```
