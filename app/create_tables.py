import asyncio
import sys
from pathlib import Path

from sqlalchemy import text

# Allow running as: `python app/create_tables.py`
if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.database import Base, engine
from app import models  # noqa: F401 - ensures models are imported before create_all

async def create_tables() -> None:
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        await conn.run_sync(Base.metadata.create_all)


if __name__ == "__main__":
    try:
        asyncio.run(create_tables())
        print("Tables created successfully.")
    except Exception as exc:
        print(
            "Database connection failed. Set DATABASE_URL or POSTGRES_* env vars "
            "to match your running Postgres container."
        )
        print(f"Details: {exc.__class__.__name__}: {exc}")
