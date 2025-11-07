from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from .settings import settings


def _as_asyncpg_dsn(db_url: str) -> str:
    """Normalize a Postgres DSN so async SQLAlchemy can use asyncpg."""
    normalized = db_url
    if normalized.startswith("postgres://"):
        normalized = normalized.replace("postgres://", "postgresql://", 1)
    if normalized.startswith("postgresql://"):
        normalized = normalized.replace("postgresql://", "postgresql+asyncpg://", 1)
    return normalized


# Create async engine
async_database_url = _as_asyncpg_dsn(settings.DATABASE_URL)
engine = create_async_engine(
    async_database_url,
    echo=False,
    future=True,
)

# Create async session maker
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session() as session:
        yield session


async def close_db() -> None:
    await engine.dispose()
