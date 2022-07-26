from PostgresFilling.post.post import Post
from PostgresFilling.utils import commit, time_counter


_EVERY_FIELD_EXCEPT_ID = "name, author, description, created_at, likes"


@time_counter
def get_posts(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM posts ORDER BY id")
    return cursor.fetchall()


def get_latest_post_id(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(id) FROM posts")
    return cursor.fetchone()[0]


def count_rows(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(1) FROM posts")
    return cursor.fetchone()[0]


@commit
def insert_post(conn, post: Post):
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO posts ({_EVERY_FIELD_EXCEPT_ID}) VALUES {post.to_tuple()}")


@commit
def add_like_to_the_latest_post(conn):
    cursor = conn.cursor()
    post_id = get_latest_post_id(conn)
    # cursor.execute(f"UPDATE posts SET likes = likes + 1 WHERE id = {post_id}")
    cursor.execute(f"SELECT likes FROM posts WHERE id = {post_id} FOR UPDATE")
    likes = cursor.fetchone()[0]
    cursor.execute(f"UPDATE posts SET likes = {likes + 1} WHERE id = {post_id}")


@commit
def clear_posts_table(conn):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM posts")
