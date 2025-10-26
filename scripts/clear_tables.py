import asyncio
import logging
from typing import List
from pathlib import Path
import sys

from sqlalchemy import text
from sqlmodel import SQLModel

sys.path.insert(0, str(Path(__file__).parent.parent))
from app.core.db import engine, close_db
from app.models import *

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("clear_tables")


def _collect_table_names() -> List[str]:
    tables = [table.name for table in SQLModel.metadata.sorted_tables]
    if not tables:
        logger.warning("No tables found in metadata. Did you import your models?")
    return tables


async def truncate_tables(table_names: List[str]) -> None:
    if not table_names:
        return

    truncate_sql = "TRUNCATE TABLE {} RESTART IDENTITY CASCADE".format(
        ", ".join(f'"{name}"' for name in table_names)
    )

    async with engine.begin() as conn:
        logger.info("Clearing data from tables: %s", ", ".join(table_names))
        await conn.execute(text(truncate_sql))
        logger.info("All tables truncated and identity columns reset.")


async def main() -> None:
    tables = _collect_table_names()
    try:
        await truncate_tables(tables)
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
