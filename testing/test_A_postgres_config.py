from A_postgres_config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB


# This test simply verifies that each of the variables imported from A_postgres_config.py is a string.
# In a real-world scenario, you might also want to test that these values meet certain conditions
# (e.g., the host is a valid IP address or hostname, the port is a valid port number, etc.).

def test_postgres_config():
    assert isinstance(POSTGRES_USER, str)
    assert isinstance(POSTGRES_PASSWORD, str)
    assert isinstance(POSTGRES_HOST, str)
    assert isinstance(POSTGRES_PORT, str)
    assert isinstance(POSTGRES_DB, str)
