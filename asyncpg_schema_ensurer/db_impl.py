from database_schema_ensurer import (
    Database as _Database,
    MigrationRecord as _MigrationRecord,
)
from typing import (
    Callable as _Callable,
    Awaitable as _Awaitable,
    Optional as _Optional,
    TypeVar as _TypeVar,
)
import asyncpg as _asyncpg


_T = _TypeVar("_T")


class Database(_Database):
    def __init__(self, awaiter: _Callable[[_Awaitable[_T]], _T], table_name: str, connection: _asyncpg.Connection) -> None:
        self._connection = connection
        self._table_name = table_name
        self._awaiter = awaiter
        super().__init__()

    def get_max_migration_version(self) -> _Optional[int]:
        async def task():
            try:
                row = await self._connection.fetch(
                    f"SELECT version FROM {self._table_name}"
                    "ORDER BY version DESC LIMIT 1"
                )
            except _asyncpg.UndefinedTableError:
                return None
            return row[0]["version"]
        return self._awaiter(task())

    def get_migration(self, version: int) -> _MigrationRecord:
        async def task():
            row = await self._connection.fetch(
                f"SELECT down_sql, version FROM {self._table_name}"
                "WHERE VERSION = $1",
                version,
            )
            return _MigrationRecord(
                down_sql=row[0]["down_sql"],
                version=row[0]["version"],
            )
        return self._awaiter(task())

    def add_migration(self, migration: _MigrationRecord):
        async def task():
            await self._connection.execute(
                f"INSERT INTO {self._table_name} (down_sql, version) VALUES ($1, $2)",
                migration.down_sql, migration.version,
            )
        return self._awaiter(task())

    def delete_migration(self, version: int):
        async def task():
            await self._connection.execute(
                f"DELETE FROM {self._table_name} WHERE version = $1",
                version,
            )
        return self._awaiter(task())

    def execute_sql(self, sql: str):
        async def task():
            await self._connection.execute(sql)
        return self._awaiter(task())
