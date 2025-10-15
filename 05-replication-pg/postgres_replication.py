import os
import time
import psycopg2

default_host = "localhost" if os.getenv("CODESPACES") == "true" else "127.0.0.1"
DB_HOST = os.getenv("ES_HOST", default_host)

MASTER_HOST = DB_HOST
REPLICA_HOST = DB_HOST
MASTER_PORT = "5432"
REPLICA_PORT = "5433"
DB = "my_database"
USER = "postgres"
#PASSWORD = "password"
PASSWORD = ""
TIMEOUT = 60

def wait_conn(host, port):
    start = time.time()
    while True:
        try:
            conn = psycopg2.connect(host=host, port=port, dbname=DB, user=USER, password=PASSWORD)
            conn.close()
            return
        except Exception:
            if time.time() - start > 120:
                raise
            time.sleep(1)

wait_conn(MASTER_HOST, MASTER_PORT)
wait_conn(REPLICA_HOST, REPLICA_PORT)

m = psycopg2.connect(host=MASTER_HOST, port=MASTER_PORT, dbname=DB, user=USER, password=PASSWORD)
r = psycopg2.connect(host=REPLICA_HOST, port=REPLICA_PORT, dbname=DB, user=USER, password=PASSWORD)
m.autocommit = True
r.autocommit = True

mc = m.cursor()
rc = r.cursor()

mc.execute("select pg_is_in_recovery()")
print("master pg_is_in_recovery() =", mc.fetchone()[0])

rc.execute("select pg_is_in_recovery()")
print("replica pg_is_in_recovery() =", rc.fetchone()[0])

mc.execute("create table if not exists public.replica_check(token text)")
mc.execute("insert into public.replica_check(token) values ('test_token')")
print("inserted token on master: test_token")


start = time.time()
found = False
while time.time() - start < TIMEOUT:
    rc.execute("select to_regclass('public.replica_check') is not null")
    print(f"start execute with end till timeout {TIMEOUT - (time.time() - start)} seconds")
    if rc.fetchone()[0]:
        rc.execute("select count(*) from public.replica_check where token='test_token'")
        if rc.fetchone()[0] >= 1:
            print(f"replica sees token after {time.time() - start} seconds")
            found = True
            break
        time.sleep(1)

if not found:
    print("timeout: token not on replica")


print("== master: pg_stat_replication ==")
mc.execute("select client_addr,state,sync_state,sent_lsn,write_lsn,flush_lsn,replay_lsn from pg_stat_replication")
for row in mc.fetchall():
    print(row)

print("== replica: LSN/lag ==")
rc.execute("select pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn(), pg_last_xact_replay_timestamp(), now()-pg_last_xact_replay_timestamp()")
print(rc.fetchone())

mc.close()
rc.close()
m.close()
r.close()