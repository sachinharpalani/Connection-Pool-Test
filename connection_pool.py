import threading
import time
import sys
import mysql.connector
import queue 


POOL_SIZE = 10
pool = queue.Queue(maxsize=POOL_SIZE)

def timeit(func):
    def wrapper(*args, **kwargs):
        st = time.time()
        result = func(*args, **kwargs)
        et = time.time()
        print(f"{func.__name__} took {et-st}sec")
        return result
    return wrapper

def get_connection():
    return mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="",
        password=""
    )

def run_query():
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT SLEEP(0.1)")
    cursor.fetchall()

@timeit
def bechmark_without_pool(n):
    threads = []
    for _ in range(n):
        thread = threading.Thread(target=run_query)
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
def initialize_pool():
    for _ in range(POOL_SIZE):
        connection = get_connection()
        pool.put(connection)

def run_query_pool():
    connection = pool.get()
    cursor = connection.cursor()
    cursor.execute("SELECT SLEEP(0.1)")
    cursor.fetchall()
    pool.put(connection)

@timeit
def benchmark_with_pool(n):
    threads = []
    for _ in range(n):
        thread = threading.Thread(target=run_query_pool)
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    n = int(sys.argv[1])
    bechmark_without_pool(n)
    initialize_pool()
    benchmark_with_pool(n)

# OUTPUT
# ➜  ~ python connection_pool.py 160
# mysql.connector.errors.DatabaseError: 1040 (HY000): Too many connections
# bechmark_without_pool took 0.33902907371520996sec
# ➜  ~ python connection_pool.py 500 
# benchmark_with_pool took 8.526005029678345sec
# ➜  ~ python connection_pool.py 1000
# benchmark_with_pool took 16.96301817893982sec