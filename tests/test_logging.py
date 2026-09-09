"""Regression tests for the JSON formatter fixes made in step 2's follow-up commit
(977cc23).

Not exhaustive — just enough that a future refactor letting extra={...} shadow a fixed
key again, or reverting to local-time timestamps, fails here immediately.
"""

import json
import logging

from app.core.logging import _JSONFormatter


def _record(**extra: object) -> logging.LogRecord:
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="processed %s",
        args=("evt-1",),
        exc_info=None,
    )
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_extra_cannot_override_fixed_keys():
    """extra={"level": ...} must not be able to shadow the record's real level (the
    bug this replaced: extras were applied last and won)."""
    payload = json.loads(_JSONFormatter().format(_record(level="OVERRIDE", timestamp="OVERRIDE")))

    assert payload["level"] == "INFO"
    assert payload["timestamp"] != "OVERRIDE"


def test_timestamp_is_utc_with_milliseconds():
    """Was local time at second granularity; worker and API lines need to correlate
    during a burst regardless of container timezone."""
    payload = json.loads(_JSONFormatter().format(_record()))

    assert payload["timestamp"].endswith("+00:00")
    assert "." in payload["timestamp"]
