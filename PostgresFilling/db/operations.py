from psycopg2.extensions import connection as Connection, cursor as Cursor
from PostgresFilling.post.post import Post
from PostgresFilling.utils import commit, time_counter, BadFetch


_EVERY_FIELD_EXCEPT_ID = "name, author, description, created_at, likes"


@time_counter
def get_posts(conn: Connection):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM posts ORDER BY id")
        result = cursor.fetchall()
    return result


def get_latest_post_id(conn: Connection):
    with conn.cursor() as cursor:
        cursor.execute("SELECT MAX(id) FROM posts")
        result = cursor.fetchone()[0]
    return result


def count_rows(conn: Connection):
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(1) FROM posts")
        result = cursor.fetchone()[0]
    return result


@commit()
def insert_post(conn: Connection, post: Post):
    with conn.cursor() as cursor:
        cursor.execute(f"INSERT INTO posts ({_EVERY_FIELD_EXCEPT_ID}) VALUES (%s, %s, %s, %s, %s)", post.to_tuple())


@commit(times_to_repeat=10, repeat_in=0.1)
def add_like_to_the_latest_post(conn: Connection, cursor: Cursor, thread_num: int, call_num: int):
    cursor.execute("SELECT * FROM posts ORDER BY id DESC LIMIT 1 FOR UPDATE")
    post_id = cursor.fetchone()
    if post_id is None or post_id[0] is None:
        raise BadFetch(thread_num, call_num)
    cursor.execute("SELECT likes FROM posts WHERE id = (%s)", (post_id[0],))
    likes = cursor.fetchone()
    if likes is None or likes[0] is None:
        raise BadFetch(thread_num, call_num)
    cursor.execute("UPDATE posts SET likes = (%s) WHERE id = (%s)", (likes[0] + 1, post_id[0]))
    # cursor.execute(f"UPDATE posts SET likes = likes + 1 WHERE id = {post_id}")


@commit()
def clear_posts_table(conn: Connection):
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM posts")
