from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import psycopg2
import psycopg2.extensions
from time import sleep

from credentials import user, password, host, port, prod_db
from db.operations import get_posts, insert_post, clear_posts_table, add_like_to_the_latest_post
from post.post_generator import generate_post
from post.post import Post
from utils import time_counter


def main():
    post_per_thread_count = 20
    likes_per_thread_count = 30
    thread_count = 8
    with psycopg2.connect(user=user, password=password, host=host, port=port, database=prod_db) as conn:
        # conn.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

        # with ThreadPoolExecutor(max_workers=thread_count) as executor:
        #     futures = [executor.submit(fill_posts_table, conn, post_per_thread_count) for _ in range(thread_count)]
        #     for _ in as_completed(futures):
        #         print("job done!")
        insert_post(conn, Post("Test", "Max", "abcde", date(2000, 1, 1), 0))

        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = []
            for i in range(thread_count):
                futures.append(executor.submit(like_latest_post, conn, likes_per_thread_count, i))
                # sleep(0.01)
            for _ in as_completed(futures):
                print("job done!")
        # like_latest_post(conn, conn.cursor(), likes_per_thread_count)

        posts = get_posts(conn)
        print(*posts, sep="\n")

        clear_posts_table(conn)


@time_counter
def fill_posts_table(conn, post_count: int):
    for i in range(post_count):
        insert_post(conn, generate_post())


@time_counter
def like_latest_post(conn, likes_count: int, thread_num: int):
    for i in range(likes_count):
        add_like_to_the_latest_post(conn, thread_num, i)


if __name__ == "__main__":
    main()
