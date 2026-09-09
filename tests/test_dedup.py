"""Deduplication by event id — required by the assignment.

Implemented in step 3, alongside app/db/repository.py.
"""

import pytest

pytestmark = pytest.mark.skip(reason="scaffold: implemented in step 3")


async def test_first_insert_returns_true() -> None:
    """A new id is inserted and reported as inserted."""


async def test_duplicate_insert_returns_false() -> None:
    """Re-inserting the same id reports False and leaves exactly one row."""


async def test_duplicate_does_not_overwrite() -> None:
    """ON CONFLICT DO NOTHING keeps the original row; a second delivery cannot mutate it."""


async def test_concurrent_inserts_of_same_id() -> None:
    """Two concurrent inserts of one id: exactly one True, one False, one row."""


async def test_distinct_ids_both_inserted() -> None:
    """Dedup is keyed on id alone and does not collapse unrelated events."""
