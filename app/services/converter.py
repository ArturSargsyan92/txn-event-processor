"""Currency conversion. Pure, synchronous, no I/O — one of the two spec-required test targets.

Split out of `processor` precisely so it can be tested without mocking a rate service or a
database: given an amount and a rate, the arithmetic and the rounding rule are the whole story.
"""

from decimal import ROUND_HALF_EVEN, Decimal

USD_QUANT = Decimal("0.01")
"""Stored USD amounts are exact to the cent."""

ROUNDING = ROUND_HALF_EVEN
"""Banker's rounding: unbiased over many transactions, unlike ROUND_HALF_UP which drifts high."""


def convert_to_usd(amount: Decimal, rate: Decimal) -> Decimal:
    """Convert `amount` to USD at `rate` (USD per 1 unit of the source currency).

    Quantized to cents with `ROUNDING`. Multiplication happens at full Decimal precision and is
    rounded exactly once, at the end, so no intermediate rounding error accumulates.
    """
    ...
