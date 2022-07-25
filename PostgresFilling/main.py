from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
import psycopg2.extensions

from .credentials import user, password, host, port, prod_db
from db.operations import get_posts, insert_post, clear_posts_table
from post.post_generator import generate_post
from utils import time_counter


def main():
    post_count = 100
    thread_count = 4
    with psycopg2.connect(user=user, password=password, host=host, port=port, database=prod_db) as conn:
        # conn.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = [executor.submit(fill_posts_table, conn, post_count // thread_count) for _ in range(thread_count)]
            for _ in as_completed(futures):
                print("job done!")

        posts = get_posts(conn)
        print(*posts, sep="\n")

        clear_posts_table(conn)


@time_counter
def fill_posts_table(conn, post_count: int):
    for i in range(post_count):
        insert_post(conn, generate_post())


if __name__ == "__main__":
    main()
