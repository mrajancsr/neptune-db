import asyncio

import asyncpg
import pytest

from neptunedb.dbreader import AsyncDBReader, DBConfig

# Define test database config
TEST_DB_CONFIG = DBConfig(
    user="test_user",
    password="test_password",
    database="test_database",
    host="localhost",
)


@pytest.fixture(scope="session")
async def db_reader():
    """Creates an instance of AsyncDBReader for testing."""
    async with AsyncDBReader(TEST_DB_CONFIG) as db:
        yield db


@pytest.mark.asyncio
async def test_connect_and_close(db_reader):
    """Test database connection and closing."""
    assert db_reader.conn is not None
    await db_reader.close()
    assert db_reader.conn is None


@pytest.mark.asyncio
async def test_execute_single_query(db_reader):
    """Test executing a single SQL statement."""
    await db_reader.execute(
        "CREATE TABLE IF NOT EXISTS test (id SERIAL PRIMARY KEY);"
    )


@pytest.mark.asyncio
async def test_execute_multiple_queries(db_reader):
    """Test executing multiple queries inside a transaction."""
    queries = [
        "INSERT INTO test DEFAULT VALUES;",
        "INSERT INTO test DEFAULT VALUES;",
    ]
    await db_reader.execute(queries)

    result = await db_reader.fetch("SELECT COUNT(*) FROM test;")
    assert result[0]["count"] >= 2


@pytest.mark.asyncio
async def test_fetch_query(db_reader):
    """Test fetching data from the database."""
    result = await db_reader.fetch("SELECT * FROM test;")
    assert len(result) > 0


@pytest.mark.asyncio
async def test_execute_invalid_query(db_reader):
    """Test executing an invalid query to trigger error handling."""
    with pytest.raises(Exception):
        await db_reader.execute("INVALID SQL STATEMENT")


@pytest.mark.asyncio
async def test_push_bulk_data(db_reader):
    """Test inserting bulk data using push()."""
    data = [(1, "test1"), (2, "test2")]
    await db_reader.execute(
        "CREATE TABLE IF NOT EXISTS test_bulk (id INT, name TEXT);"
    )
    await db_reader.push(data, "test_bulk", ["id", "name"])

    result = await db_reader.fetch("SELECT COUNT(*) FROM test_bulk;")
    assert result[0]["count"] == 2
