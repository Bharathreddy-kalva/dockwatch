"""Async Postgres connection pool for the API.

psycopg (not SQLAlchemy Core/ORM), matching every other write/read path in
this codebase — SQLAlchemy is only ever used by Alembic for migrations.
Async, not the sync psycopg the rest of the app uses: the API layer is the
one place actually running under an async framework, so it's the one place
that benefits from psycopg's async mode instead of blocking the event loop.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from dockwatch.common.config import settings

_dsn = settings.database_url.replace("postgresql+psycopg://", "postgresql://")

pool = AsyncConnectionPool(_dsn, open=False)


async def get_db() -> AsyncIterator[AsyncConnection]:
    async with pool.connection() as conn:
        yield conn
