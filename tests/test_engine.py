"""wait_until_ready — the worker's startup gate, waiting for both Postgres and the schema.

Needs a real Postgres; see the `postgres_engine` fixture in conftest.py.
"""

import pytest

from app.core.errors import DatabaseUnavailable
from app.db.engine import init_models, wait_until_ready


async def test_succeeds_once_the_table_exists(postgres_engine):
    await init_models(postgres_engine)

    # Must not raise — the whole point of this test is that a table probe, not a bare
    # SELECT 1, is what wait_until_ready actually runs.
    await wait_until_ready(postgres_engine, attempts=1, base_delay=0)


async def test_raises_when_the_table_does_not_exist_yet(postgres_engine):
    # postgres_engine is connected but init_models was never called, so the table this
    # probes for genuinely doesn't exist — the exact case the docstring promises to wait out.
    with pytest.raises(DatabaseUnavailable):
        await wait_until_ready(postgres_engine, attempts=1, base_delay=0)
