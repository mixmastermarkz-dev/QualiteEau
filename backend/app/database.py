"""
Connexion à PostgreSQL via SQLAlchemy 2 (async).
"""

import os

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import settings

_testing = os.getenv("TESTING") == "1"

if _testing:
    # NullPool : pas de réutilisation de connexions entre les tests.
    # asyncpg lie chaque connexion à l'event-loop qui l'a créée ;
    # avec un pool standard, les connexions peuvent devenir stale entre tests.
    from sqlalchemy.pool import NullPool
    engine = create_async_engine(
        settings.database_url,
        echo=False,
        poolclass=NullPool,
    )
else:
    engine = create_async_engine(
        settings.database_url,
        echo=settings.is_development,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    """Dépendance FastAPI : fournit une session DB par requête."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
