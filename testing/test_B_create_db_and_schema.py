import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pytest
from A_postgres_config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
import B_create_db_and_schema as script

# This function creates a schema on an existing connection.
def create_schema(conn, schema_name):
    cursor = conn.cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    conn.commit()
    cursor.close()

@pytest.fixture(scope="module")
def test_database():
    # Connect to Postgres Server
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
    cursor.execute(f"DROP DATABASE IF EXISTS {test_database_name}")  # Drop the test database if it exists
    try:
        cursor.execute(f"CREATE DATABASE {test_database_name}")
        print("Test Database created.")
    except psycopg2.Error as e:
        print(f"Error creating Test Database: {e}")

    cursor.close()
    conn.close()

    yield test_database_name

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database='postgres'
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {test_database_name}")
    cursor.close()
    conn.close()


@pytest.fixture(scope="module")
def test_schema(test_database):
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=test_database
    )
    cursor = conn.cursor()

    # Call your function that creates the schema
    test_schema_name = 'test_sch_nypd_calls_tables'
    create_schema(conn, test_schema_name)

    cursor.close()
    conn.close()

    yield test_schema_name


def test_database_creation(test_database):
    # Check if the test database was created
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=test_database
    )
    cursor = conn.cursor()
    check_db_query = f"SELECT 1 FROM pg_database WHERE datname = '{test_database}'"
    cursor.execute(check_db_query)
    database_exists = cursor.fetchone()
    cursor.close()
    conn.close()

    assert database_exists is not None


def test_schema_creation(test_database, test_schema):
    # Check if the test schema was created
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=test_database
    )
    cursor = conn.cursor()
    check_schema_query = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{test_schema}'"
    cursor.execute(check_schema_query)
    schema_exists = cursor.fetchone()
    cursor.close()
    conn.close()

    assert schema_exists is not None
