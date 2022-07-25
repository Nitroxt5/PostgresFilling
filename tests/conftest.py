import psycopg2
from pytest import fixture

from tests.credentials import user, password, host, port, test_db


@fixture(scope="session")
def db_conn():
    with psycopg2.connect(user=user, password=password, host=host, port=port, database=test_db) as conn:
        yield conn
