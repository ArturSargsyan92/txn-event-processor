"""Currency conversion — required by the assignment.

Expected values are computed via Python's decimal module directly (see the commit that added
this file) rather than derived by hand — financial rounding is exactly the kind of arithmetic
where a mental calculation is easy to get subtly wrong.
"""

from decimal import Decimal

from app.services.converter import convert_to_usd


def test_converts_at_rate():
    """A plain conversion: amount * rate, quantized to cents."""
    assert convert_to_usd(Decimal("10.00"), Decimal("1.10")) == Decimal("11.00")


def test_usd_passthrough_is_exact():
    """Rate 1 leaves the amount untouched, with no rounding artefacts."""
    assert convert_to_usd(Decimal("42.37"), Decimal("1")) == Decimal("42.37")


def test_rounds_half_to_even():
    """Exact .xx5 cases round to even, not always up — the documented rounding rule.

    1 * 0.125 = 0.125: the digit before the 5 is 2 (even), so it stays 0.12.
    1 * 0.135 = 0.135: the digit before the 5 is 3 (odd), so it rounds up to 0.14 — the same
    distance from 0.135 as 0.12 is from 0.125, but the *direction* differs, which is the
    signature of round-half-even rather than round-half-up (which would give 0.13/0.14 both
    times, never landing on an even digit by rule).
    """
    assert convert_to_usd(Decimal("1"), Decimal("0.125")) == Decimal("0.12")
    assert convert_to_usd(Decimal("1"), Decimal("0.135")) == Decimal("0.14")


def test_no_intermediate_rounding():
    """A long-precision rate rounds once at the end, not at each step.

    3 * 0.335 = 1.005, which rounds to 1.00 (0 is even). If the rate were rounded to cents
    *before* multiplying (0.335 -> 0.34, since 3 is odd and rounds up), 3 * 0.34 = 1.02 instead
    — a different, wrong answer. This is the case that would catch a common bug: converting
    with an already-rounded rate rather than the full-precision one.
    """
    assert convert_to_usd(Decimal("3"), Decimal("0.335")) == Decimal("1.00")


def test_returns_decimal_not_float():
    """The result stays Decimal; a float anywhere here would silently lose cents."""
    result = convert_to_usd(Decimal("10.00"), Decimal("1.10"))
    assert isinstance(result, Decimal)


def test_handles_large_amounts():
    """Values near the Numeric(18, 4) column limit convert without overflow."""
    result = convert_to_usd(Decimal("99999999999999.9999"), Decimal("1.0001"))
    assert result == Decimal("100010000000000.00")
    assert isinstance(result, Decimal)
