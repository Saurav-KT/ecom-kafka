import contextlib
from typing import AsyncIterator
from sqlalchemy import text

from sqlalchemy.schema import CreateSchema
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlmodel import SQLModel

class DatabaseSessionManager:
    SCHEMA_NAME = "task_management"

    def __init__(self):
        self._engine: AsyncEngine | None = None
        self._sessionmaker: async_sessionmaker | None = None

    # expire_on_commit - don't expire objects after transaction commit
    # async_sessionmaker: a factory for new AsyncSession objects.

    def init(self, host: str):
        self._engine = create_async_engine(host, echo=True)
        self._sessionmaker = async_sessionmaker(expire_on_commit=False, bind=self._engine)

    async def close(self):
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")
        await self._engine.dispose()
        self._engine = None
        self._sessionmaker = None

    @contextlib.asynccontextmanager
    async def db_connect(self) -> AsyncIterator[AsyncConnection]:
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")

        async with self._engine.begin() as connection:
            try:
                yield connection
            except Exception:
                await connection.rollback()
                raise

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        if self._sessionmaker is None:
            raise Exception("DatabaseSessionManager is not initialized")

        session = self._sessionmaker()
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    # async def create_all(self):
    #     async with self.db_connect() as connection:
    #         await connection.run_sync(SQLModel.metadata.create_all)

    async def create_schema(self):
        async with self.db_connect() as connection:
            await connection.execute(text(f"DROP SCHEMA IF EXISTS {self.SCHEMA_NAME} CASCADE"))
            await connection.run_sync(lambda conn: conn.execute(CreateSchema(self.SCHEMA_NAME, if_not_exists=True)))


    async def init_db(self):

        await self.create_schema()
        async with self.db_connect() as connection:
            print("Dropping all tables...")
            await connection.run_sync(SQLModel.metadata.drop_all)
            print("Creating all tables...")
            await connection.run_sync(SQLModel.metadata.create_all)
            print("Database initialization complete.")

sessionmanager = DatabaseSessionManager()

async def get_db():
    async with sessionmanager.session() as session:
        yield session
