import asyncio
import logging
import sys
from pathlib import Path

from sqlalchemy import text
from sqlmodel import SQLModel

sys.path.insert(0, str(Path(__file__).parent.parent))
import app.models  # noqa: F401
from app.core.db import close_db, engine

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("reset_database")


async def drop_database() -> None:
    """Remove SQLModel-managed structures and Alembic version tracking."""
    async with engine.begin() as conn:
        logger.info("Dropping SQLModel metadata (tables, enums, constraints)…")
        await conn.run_sync(SQLModel.metadata.drop_all)

        logger.info("Dropping Alembic version table if it exists…")
        await conn.execute(text("DROP TABLE IF EXISTS alembic_version CASCADE"))


async def main() -> None:
    try:
        await drop_database()
        logger.info(
            "Database metadata dropped successfully. Alembic version table removed."
        )
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
