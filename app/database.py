from pathlib import Path
from dotenv import load_dotenv
import os

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
)
from sqlalchemy.orm import declarative_base
from typing import AsyncGenerator

# -- Load .env from project root --
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# ---- Build Database URL ----
DATABASE_URL = (
    f"postgresql+asyncpg://"
    f"{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@"
    f"localhost:5433/"
    f"{os.getenv('POSTGRES_DB')}"
)
# ---- Async Engine ----
engine = create_async_engine(DATABASE_URL, echo=True)

# ---- Session Factory ----
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# ---- Base for Models ----
Base = declarative_base()

# ---- Dependency Helper ----
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session