from datetime import date
from pytest import raises

from PostgresFilling.db.operations import add_like_to_the_latest_post, insert_post, clear_posts_table, get_latest_post_id, count_rows
from PostgresFilling.post.post import Post
from PostgresFilling.utils import commit


def test_no_connection_commit_wrapper():
    @commit()
    def func(conn):
        pass

    with raises(TypeError):
        func(1)


def test_bad_times_to_repeat_commit_wrapper(db_conn):
    with raises(ValueError):
        @commit(times_to_repeat=0)
        def func(conn):
            pass


def test_insert_post(db_conn):
    insert_post(db_conn, Post("adv", "vadv", "ffbsd", date(2000, 5, 30), 100))
    insert_post(db_conn, Post("dfb", "adsda", "bfdg", date(2012, 4, 12), 50))

    rows = count_rows(db_conn)
    clear_posts_table(db_conn)

    assert rows == 2


def test_add_like_to_the_latest_post(db_conn):
    insert_post(db_conn, Post("adv", "vadv", "ffbsd", date(2000, 5, 30), 100))
    insert_post(db_conn, Post("dfb", "adsda", "bfdg", date(2012, 4, 12), 50))

    with db_conn.cursor() as cursor:
        add_like_to_the_latest_post(db_conn, 0, 0)
        post_id = get_latest_post_id(db_conn)
        cursor.execute(f"SELECT likes FROM posts WHERE id = {post_id}")
        likes = cursor.fetchone()[0]
    clear_posts_table(db_conn)

    assert likes == 51
