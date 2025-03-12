# pyre-strict
import asyncio
import logging
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import asyncpg
import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor, execute_values

from neptunedb.db_config import DBConfig

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- Synchronous DBReader ---
@dataclass
class SyncDBHandler:
    config: DBConfig

    def connect(self) -> psycopg2.extensions.connection:
        try:
            return psycopg2.connect(**self.config.__dict__)
        except psycopg2.DatabaseError as e:
            logger.error(f"Database connection failed: {e}")
            raise

    @contextmanager
    def get_cursor(self):
        with self.connect() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                yield cursor
                conn.commit()

    def fetch(self, query: str) -> List[Dict[str, Any]]:
        with self.get_cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    def fetchdf(self, query: str) -> pd.DataFrame:
        rows = self.fetch(query)
        return pd.DataFrame(rows)

    def execute(self, query: Union[str, List[str]]) -> None:
        with self.get_cursor() as cursor:
            if isinstance(query, str):
                cursor.execute(query)
            else:
                for q in query:
                    cursor.execute(q)

    def push(
        self, data: Iterator[Tuple], table_name: str, columns: List[str]
    ) -> None:
        query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES %s"
        with self.get_cursor() as cursor:
            execute_values(cursor, query, data)

    def copy_from_csv(self, df: pd.DataFrame, table_name: str) -> None:
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_file:
            df.to_csv(tmp_file.name, index=False, header=False)
            tmp_file.close()

            with self.connect() as conn:
                with conn.cursor() as cursor, open(tmp_file.name, "r") as file:
                    cursor.copy_from(file, table_name, sep=",")
                    conn.commit()

            os.remove(tmp_file.name)


# --- Asynchronous DBReader ---


@dataclass
class AsyncDBHandler:
    config: DBConfig
    conn: Optional[asyncpg.Connection] = None

    async def connect(self) -> asyncpg.Connection:
        if self.conn is None:
            try:
                self.conn = await asyncpg.connect(**self.config.__dict__)
                logger.info("Connected to PostgreSQL asynchronously.")
            except Exception as e:
                logger.error(f"Async connection failed: {e}")
                raise
        return self.conn

    async def close(self):
        if self.conn is not None:
            await self.conn.close()
            self.conn = None

    async def fetch(self, query: str, *args) -> List[asyncpg.Record]:
        conn = await self.connect()
        try:
            return await conn.fetch(query, *args)
        except Exception as e:
            logger.error(f"Async fetch failed: {e}")
            raise

    async def execute(self, query: Union[str, List[str]], *args) -> None:
        """Executes one of multiple SQL queries asynchronously

        Parameters
        ----------
        query : Union[str, List[str]]
            _description_
        """
        conn = await self.connect()
        try:
            if isinstance(query, str):
                await conn.execute(query, *args)
            else:
                async with conn.transaction():
                    for i, q in enumerate(query):
                        try:
                            await conn.execute(q)
                        except Exception as sub_e:
                            logger.error(
                                f"Query {i + 1}/{len(query)} failed: {sub_e} | Query: {q}"  # noqa
                            )
                            raise sub_e
        except Exception as e:
            logger.error(f"Async execute batch failed: {e}")
            raise

    async def push(
        self, data: Iterator[Tuple], table_name: str, columns: List[str]
    ) -> None:
        """Inserts bulk data asynchronously into a PostgreSQL table

        Parameters
        ----------
        data : Iterator[Tuple]
            The data to insert, as an iterator of tuples
        table_name : str
            The name of target table
        columns : List[str]
            The column names corresponding to the data
        """
        conn = await self.connect()
        try:
            await conn.copy_records_to_table(
                table_name, records=data, columns=columns
            )
        except Exception as e:
            logger.error(f"Async data push failed: {e}")
            raise

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


async def main():
    config = DBConfig.from_env()

    async with AsyncDBHandler(config) as reader:
        rows = await reader.fetch(
            "SELECT * FROM pg_catalog.pg_tables LIMIT 5;"
        )
        for row in rows:
            print(dict(row))


if __name__ == "__main__":
    # used for debugging purposes
    asyncio.run(main())
