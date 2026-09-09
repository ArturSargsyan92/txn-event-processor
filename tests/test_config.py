"""Regression tests for the config fixes made in step 2's follow-up commit (977cc23).

Not exhaustive — just enough that a future refactor re-introducing an independent
dlq_stream_name setting, or loosening extra="forbid" or the attempts/max_deliveries
floor, fails here immediately.
"""

import pytest
from pydantic import ValidationError

from app.core.config import Settings


def test_dlq_stream_name_derives_from_stream_name():
    """dlq_stream_name is computed, not stored — overriding stream_name alone can never
    leave it pointing at the wrong stream (the bug this replaced)."""
    assert Settings(stream_name="custom").dlq_stream_name == "custom:dlq"
    assert Settings().dlq_stream_name == "transactions:dlq"


def test_unknown_field_is_rejected():
    """extra='forbid': a typo'd setting fails fast instead of being silently dropped
    and surfacing later as an unexplained connection error."""
    with pytest.raises(ValidationError):
        Settings(not_a_real_setting="x")


@pytest.mark.parametrize("field", ["rate_attempts", "db_attempts", "max_deliveries"])
def test_non_positive_attempts_are_rejected(field: str):
    """A value of 0 here would make retry_async's guard fire, or the DLQ cutoff fire
    on the very first delivery — both configuration mistakes, not runtime outcomes."""
    with pytest.raises(ValidationError):
        Settings(**{field: 0})
