"""Maps asyncpg/SQLAlchemy exceptions onto the app's Transient/Permanent taxonomy.

Classifying by Python exception type doesn't work on this driver: SQLAlchemy's asyncpg dialect
translates most server-side failures — an admin shutdown and a string too long for its column
alike — into the same generic `sqlalchemy.exc.DBAPIError`. It has no `OperationalError` mapping
at all. What actually tells connection trouble apart from bad data is the Postgres SQLSTATE: a
standard 5-character error code whose first two characters group it into a class (e.g. `08` is
"connection exception", `57` is "operator intervention" — admin shutdown, still starting up).
"""

import asyncpg
from sqlalchemy.exc import SQLAlchemyError

from app.core.errors import DatabaseUnavailable

DB_EXCEPTIONS = (OSError, SQLAlchemyError, asyncpg.exceptions.PostgresError)
"""What every DB call site should catch. Broad on purpose — `classify_db_error` narrows it.

Three shapes, all real: a raw OSError from the socket layer before any driver code runs; a
SQLAlchemy-wrapped exception for a failure during a query; and a raw asyncpg exception for a
failure during the very first connect, which happens before SQLAlchemy gets a chance to wrap
anything (its wrapping check is `isinstance(e, dialect.loaded_dbapi.Error)`, and a raw asyncpg
exception, whose hierarchy predates that check, never satisfies it).
"""

_TRANSIENT_SQLSTATE_CLASSES = frozenset(
    {
        "08",  # connection exception
        "40",  # transaction rollback: deadlock, serialization failure
        "53",  # insufficient resources: too many connections, out of memory
        "57",  # operator intervention: admin shutdown, still starting up, query canceled
    }
)
"""Everything else — 22 data exception, 23 integrity constraint violation, 42 syntax/access
rule violation, and so on — means the event or the schema is wrong. No amount of retrying
fixes that, so those SQLSTATE classes are deliberately not in this set."""


def classify_db_error(exc: Exception) -> DatabaseUnavailable | None:
    """Return a DatabaseUnavailable wrapping `exc` if it looks retryable, else None — the
    caller should let the original exception propagate unclassified.

    A caught raw asyncpg exception carries `.sqlstate` directly; a SQLAlchemy-wrapped one
    carries it on `.orig` (the original driver exception `sqlalchemy.exc.DBAPIError` wraps).
    """
    if isinstance(exc, OSError):
        return DatabaseUnavailable(str(exc))

    sqlstate = getattr(exc, "sqlstate", None) or getattr(
        getattr(exc, "orig", None), "sqlstate", None
    )
    if sqlstate is not None and sqlstate[:2] in _TRANSIENT_SQLSTATE_CLASSES:
        return DatabaseUnavailable(str(exc))

    return None
