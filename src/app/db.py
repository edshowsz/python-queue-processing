import os
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/fastapi_queue",
)


class Base(DeclarativeBase):
    pass


engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    pool_size=200,
    max_overflow=100,
    pool_pre_ping=True,
    pool_recycle=3600,
)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session


async def init_db() -> None:
    from . import models  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
