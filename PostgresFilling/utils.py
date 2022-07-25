from time import perf_counter
import psycopg2.extensions


def commit(operation):
    def commit_wrapper(conn, *args, **kwargs):
        if not isinstance(conn, psycopg2.extensions.connection):
            raise TypeError
        result = operation(conn, *args, **kwargs)
        conn.commit()
        return result
    return commit_wrapper


def time_counter(func):
    def time_counter_wrapper(*args, **kwargs):
        start = perf_counter()
        result = func(*args, **kwargs)
        time_taken = perf_counter() - start
        print(f"Function {func} ended in {time_taken} s")
        return result
    return time_counter_wrapper
