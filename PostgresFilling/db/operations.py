from PostgresFilling.post.post import Post
from PostgresFilling.utils import commit, time_counter


_EVERY_FIELD_EXCEPT_ID = "name, author, description, created_at, likes"


@time_counter
def get_posts(conn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM posts ORDER BY id")
    return cursor.fetchall()


@commit
def insert_post(conn, post: Post):
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO posts ({_EVERY_FIELD_EXCEPT_ID}) VALUES {post.to_tuple()}")


@commit
def clear_posts_table(conn):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM posts")
