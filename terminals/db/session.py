"""Async SQLAlchemy engine and session factory."""

import os
from pathlib import Path

from terminals.config import settings

engine = None
async_session = None

_db_url = settings.database_url
if _db_url:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    # Ensure the directory for the SQLite file exists.
    if _db_url.startswith("sqlite"):
        _db_path = _db_url.split("///", 1)[-1]
        if _db_path:
            os.makedirs(os.path.dirname(_db_path) or ".", exist_ok=True)

    connect_args = {}
    if _db_url.startswith("sqlite"):
        # Wait for locks instead of failing fast — required once multiple
        # worker processes share the database file.
        connect_args["timeout"] = 30

    engine = create_async_engine(_db_url, echo=False, connect_args=connect_args)
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    if _db_url.startswith("sqlite"):
        from sqlalchemy import event

        @event.listens_for(engine.sync_engine, "connect")
        def _set_sqlite_pragmas(dbapi_connection, connection_record):
            # WAL allows concurrent reader/writer processes (multi-worker);
            # synchronous=NORMAL is the standard WAL pairing — skips the
            # per-commit fsync while staying crash-safe.
            # (The 30s busy timeout comes from connect_args above.)
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.close()


def _init_lock_path() -> Path:
    """Lock file location for serializing startup migrations.

    Lives next to the SQLite file (or in the data dir) rather than the
    world-writable temp dir, so other users on a shared host can't squat it.
    """
    if settings.database_url.startswith("sqlite"):
        db_path = settings.database_url.split("///", 1)[-1]
        if db_path:
            return Path(db_path + ".init.lock")
    data_dir = Path(settings.data_dir)
    return data_dir / ".db-init.lock"


def init_db():
    """Run Alembic migrations (sync — safe to call from any context).

    Guarded by a cross-process file lock: with multiple uvicorn workers,
    every worker calls this at startup and concurrent migrations would race.
    Workers always run on the same host, so an fcntl lock is sufficient.
    """
    if os.name != "posix":  # no flock on this platform
        _run_migrations()
        return

    import fcntl

    lock_path = _init_lock_path()
    os.makedirs(lock_path.parent, exist_ok=True)
    with open(lock_path, "a") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            _run_migrations()
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def _run_migrations():
    from alembic import command
    from alembic.config import Config

    alembic_cfg = Config()
    ini_path = Path(__file__).resolve().parent.parent / "alembic.ini"
    if ini_path.exists():
        alembic_cfg = Config(str(ini_path))

    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    alembic_cfg.set_main_option("script_location", str(migrations_dir))

    # Use sync URL for alembic (env.py uses sync engine).
    sync_url = settings.database_url.replace(
        "sqlite+aiosqlite", "sqlite"
    ).replace(
        "postgresql+asyncpg", "postgresql"
    )
    alembic_cfg.set_main_option("sqlalchemy.url", sync_url)
    command.upgrade(alembic_cfg, "head")


async def close_db():
    """Dispose of the engine's connection pool."""
    if engine is not None:
        await engine.dispose()
