import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pytest
from A_postgres_config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB

@pytest.fixture(scope="module")
def test_database():
    # Set up a test database
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database='postgres'
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # Create a test database
    test_database_name = 'test_db_nypd_calls_for_service_data'
    cursor.execute(f"CREATE DATABASE {test_database_name}")

    # Connect to the test database
    conn.close()
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=test_database_name
    )
    cursor = conn.cursor()

    yield conn

    # Drop the test database
    cursor.execute(f"DROP DATABASE {test_database_name}")
    cursor.close()
    conn.close()


@pytest.fixture(scope="module")
def test_schema(test_database):
    # Set up a test schema
    conn = test_database
    cursor = conn.cursor()

    test_schema_name = 'test_sch_nypd_calls_tables'
    cursor.execute(f"CREATE SCHEMA {test_schema_name}")

    yield test_schema_name

    # Drop the test schema
    cursor.execute(f"DROP SCHEMA {test_schema_name} CASCADE")
    cursor.close()


def test_database_creation(test_database):
    # Check if the test database was created
    cursor = test_database.cursor()
    check_db_query = f"SELECT 1 FROM pg_database WHERE datname = '{test_database.dsn.split('/')[-1]}'"
    cursor.execute(check_db_query)
    database_exists = cursor.fetchone()
    assert database_exists is not None


def test_schema_creation(test_database, test_schema):
    # Check if the test schema was created
    cursor = test_database.cursor()
    check_schema_query = f"SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = '{test_schema}')"
    cursor.execute(check_schema_query)
    schema_exists = cursor.fetchone()[0]
    assert schema_exists


def test_calls_table_creation(test_database, test_schema):
    # Check if the calls table was created
    cursor = test_database.cursor()
    table_name = 'tb_call_data'
    check_table_query = f"SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_schema = '{test_schema}' AND table_name = '{table_name}')"
    cursor.execute(check_table_query)
    table_exists = cursor.fetchone()[0]
    assert table_exists


def test_weather_table_creation(test_database, test_schema):
    # Check if the weather table was created
    cursor = test_database.cursor()
    weather_table_name = 'tb_weather_data'
    check_weather_table_query = f"SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_schema = '{test_schema}' AND table_name = '{weather_table_name}')"
    cursor.execute(check_weather_table_query)
    weather_table_exists = cursor.fetchone()[0]
    assert weather_table_exists
