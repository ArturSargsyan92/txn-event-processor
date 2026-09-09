"""Currency conversion — required by the assignment.

Implemented in step 4, alongside app/services/converter.py.
"""

import pytest

pytestmark = pytest.mark.skip(reason="scaffold: implemented in step 4")


def test_converts_at_rate() -> None:
    """A plain conversion: amount * rate, quantized to cents."""


def test_usd_passthrough_is_exact() -> None:
    """Rate 1 leaves the amount untouched, with no rounding artefacts."""


def test_rounds_half_to_even() -> None:
    """Exact .005 cases round to even, not always up — the documented rounding rule."""


def test_no_intermediate_rounding() -> None:
    """A long-precision rate rounds once at the end, not at each step."""


def test_returns_decimal_not_float() -> None:
    """The result stays Decimal; a float anywhere here would silently lose cents."""


def test_handles_large_amounts() -> None:
    """Values near the Numeric(18, 4) column limit convert without overflow."""
