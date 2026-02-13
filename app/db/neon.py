import psycopg2
from psycopg2.extras import RealDictCursor
from app.core.config import settings

_conn = None

def get_conn():
    global _conn
    if _conn is None or _conn.closed != 0:
        _conn = psycopg2.connect(settings.DATABASE_URL, cursor_factory=RealDictCursor)
        _conn.autocommit = True
    return _conn

def query_one(sql: str, params=None):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        row = cur.fetchone()
    return row

def query_all(sql: str, params=None):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
    return rows

def exec_sql(sql: str, params=None):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
