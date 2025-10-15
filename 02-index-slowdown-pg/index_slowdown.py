DB_NAME = "testdb"
DB_USER = "test"
DB_PASSWORD = "test"
DB_PORT = 6432
DB_BATCH = 10000

import psycopg2
import random
import time
import os

# Wait for DB to be ready (simple, not production-safe)
import socket
import time as t



def benchmark_search(cur):
    total = 0
    for _ in range(100):
        v = random.randint(1, 1000000)
        start = time.time()
        cur.execute("SELECT * FROM test WHERE value = %s;", (v,))
        cur.fetchall()
        total += time.time() - start
    return total / 100

def main():
    default_host = "localhost" if os.getenv("CODESPACES") == "true" else "127.0.0.1"
    DB_HOST = os.getenv("DB_HOST", default_host)
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cur = conn.cursor()

    # With index
    cur.execute("DROP TABLE IF EXISTS test;")
    cur.execute("CREATE TABLE test (id SERIAL PRIMARY KEY, value INTEGER);")
    cur.execute("CREATE INDEX idx_value ON test(value);")
    conn.commit()

    print("Inserting 1M rows WITH index...")
    start = time.time()
    for i in range(0, 1_000_000, DB_BATCH):
        cur.executemany(
            "INSERT INTO test (value) VALUES (%s);",
            [(random.randint(1, 1000000),) for _ in range(DB_BATCH)]
        )
        conn.commit()
        if (i+DB_BATCH) % 100000 == 0:
            print(f"Inserted {i+DB_BATCH} rows...")
    with_index_time = time.time() - start
    print(f"Time to insert 1M rows WITH index: {with_index_time:.2f} sec")

    # Without index
    cur.execute("DROP TABLE IF EXISTS test;")
    cur.execute("CREATE TABLE test (id SERIAL PRIMARY KEY, value INTEGER);")
    conn.commit()

    print("Inserting 1M rows WITHOUT index...")
    start = time.time()
    for i in range(0, 1_000_000, DB_BATCH):
        cur.executemany(
            "INSERT INTO test (value) VALUES (%s);",
            [(random.randint(1, 1000000),) for _ in range(DB_BATCH)]
        )
        conn.commit()
        if (i+DB_BATCH) % 100000 == 0:
            print(f"Inserted {i+DB_BATCH} rows...")
    without_index_time = time.time() - start
    print(f"Time to insert 1M rows WITHOUT index: {without_index_time:.2f} sec")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main() 