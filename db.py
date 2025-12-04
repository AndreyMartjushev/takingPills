import logging
from contextlib import contextmanager
from typing import Any, Iterable, Optional

import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool

_pool: Optional[SimpleConnectionPool] = None
_db_config: Optional[dict[str, Any]] = None
_minconn = 1
_maxconn = 5


def init_db_pool(db_config: dict[str, Any], minconn: int = 1, maxconn: int = 5) -> None:
    """
    Инициализирует connection pool. Повторные вызовы безопасны.
    """
    global _pool, _db_config, _minconn, _maxconn
    _db_config = db_config
    _minconn = minconn
    _maxconn = maxconn

    if _pool is not None:
        return

    _pool = SimpleConnectionPool(minconn, maxconn, **db_config)


def close_db_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None


def _ensure_pool() -> SimpleConnectionPool:
    global _pool
    if _pool is None:
        if _db_config is None:
            raise RuntimeError("DB pool is not initialized")
        _pool = SimpleConnectionPool(_minconn, _maxconn, **_db_config)
    return _pool


def _put_connection(conn, *, close: bool = False):
    pool = _ensure_pool()
    if close:
        pool.putconn(conn, close=True)
    else:
        pool.putconn(conn)


@contextmanager
def get_cursor():
    """
    Утилита для ручного управления курсором, если понадобится.
    """
    conn = _ensure_pool().getconn()
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            yield cursor
    finally:
        _put_connection(conn)


def db_query(
    query: str,
    params: Optional[Iterable[Any]] = None,
    *,
    fetchone: bool = False,
    fetchall: bool = False,
):
    """
    Выполняет запрос с автоповтором при отвалившемся соединении.
    """
    params = params or ()

    for attempt in range(2):
        conn = None
        try:
            pool = _ensure_pool()
            conn = pool.getconn()
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                if fetchone:
                    return cursor.fetchone()
                if fetchall:
                    return cursor.fetchall()
                return None
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as exc:
            logging.warning("DB connection error, resetting pool: %s", exc)
            if conn is not None:
                _put_connection(conn, close=True)
                conn = None
            close_db_pool()
            if attempt == 0:
                continue
            raise
        finally:
            if conn is not None:
                _put_connection(conn)

