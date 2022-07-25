from pytest import raises

from PostgresFilling.utils import commit


def test_no_connection_commit_wrapper(db_conn):
    @commit
    def func(conn):
        pass

    with raises(TypeError):
        func(1)
