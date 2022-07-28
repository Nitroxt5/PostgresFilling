from time import perf_counter, sleep
import psycopg2.extensions


class BadFetch(Exception):
    def __init__(self, thread_num: int, call_num: int):
        self._thread_num = thread_num
        self._call_num = call_num

    def __str__(self):
        return f"Bad fetch in thread {self._thread_num}; call number = {self._call_num}"


def commit(times_to_repeat=1, repeat_in=0):
    """This wrapper tries to execute given operation. On success commits it.
    On failure rollbacks and tries again given number of times. Every try starts with pause of given number of seconds"""
    if times_to_repeat < 1:
        raise ValueError("times_to_repeat must be > 0")

    def commit_wrapper_1(operation):

        def commit_wrapper_2(conn, *args, **kwargs):
            if type(conn) != psycopg2.extensions.connection:
                raise TypeError("Connection must be first positional arg!")
            result = None
            isFailed = False
            for i in range(times_to_repeat, 0, -1):
                try:
                    result = operation(conn, *args, **kwargs)
                except (psycopg2.Error, BadFetch) as error:
                    print(f"{error} in {operation}")
                    conn.rollback()
                    sleep(repeat_in)
                    if i == 1:
                        isFailed = True
                else:
                    break
            if isFailed:
                print("Data lost")
                return
            conn.commit()
            return result

        return commit_wrapper_2

    return commit_wrapper_1


def time_counter(func):
    """Marks the time, required to execute given function"""
    def time_counter_wrapper(*args, **kwargs):
        start = perf_counter()
        result = func(*args, **kwargs)
        time_taken = perf_counter() - start
        print(f"Function {func} ended in {time_taken} s")
        return result

    return time_counter_wrapper
