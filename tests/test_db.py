from datetime import date
from pytest import raises

from PostgresFilling.db.operations import add_like_to_the_latest_post, insert_post, clear_posts_table, get_latest_post_id, count_rows
from PostgresFilling.post.post import Post
from PostgresFilling.utils import commit


def test_no_connection_commit_wrapper(db_conn):
    @commit
    def func(conn):
        pass

    with raises(TypeError):
        func(1)


def test_clear_table(db_conn):
    insert_post(db_conn, Post("adv", "vadv", "ffbsd", date(2000, 5, 30), 100))
    insert_post(db_conn, Post("dfb", "adsda", "bfdg", date(2012, 4, 12), 50))

    clear_posts_table(db_conn)
    rows = count_rows(db_conn)

    assert rows == 0


def test_add_like_to_the_latest_post(db_conn):
    insert_post(db_conn, Post("adv", "vadv", "ffbsd", date(2000, 5, 30), 100))
    insert_post(db_conn, Post("dfb", "adsda", "bfdg", date(2012, 4, 12), 50))

    add_like_to_the_latest_post(db_conn)
    post_id = get_latest_post_id(db_conn)
    cursor = db_conn.cursor()
    cursor.execute(f"SELECT likes FROM posts WHERE id = {post_id}")
    likes = cursor.fetchone()[0]
    clear_posts_table(db_conn)

    assert likes == 51
