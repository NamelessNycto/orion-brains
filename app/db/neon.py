# app/db/neon.py

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import OperationalError, InterfaceError
from app.core.config import settings

_conn = None


def _connect():
    return psycopg2.connect(
        settings.DATABASE_URL,
        cursor_factory=RealDictCursor,
        sslmode="require",
        connect_timeout=10,
        application_name="orion-brain",
    )


def get_conn():
    global _conn

    if _conn is None or getattr(_conn, "closed", 1) != 0:
        _conn = _connect()
        _conn.autocommit = True
        return _conn

    try:
        with _conn.cursor() as cur:
            cur.execute("SELECT 1;")
    except (OperationalError, InterfaceError):
        try:
            _conn.close()
        except Exception:
            pass
        _conn = _connect()
        _conn.autocommit = True

    return _conn


def query_one(sql: str, params=None):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchone()


def query_all(sql: str, params=None):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def exec_sql(sql: str, params=None):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
