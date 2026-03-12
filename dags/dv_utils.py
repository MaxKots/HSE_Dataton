import hashlib
import logging
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_default"
STG_SCHEMA = "cdl_ceh"
DV_SCHEMA = "ceh"
RECORD_SOURCE = "cdl_ceh"
GHOST_DATE = "9999-12-31"


def get_conn():
     return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


def execute_sql(sql: str, params=None, fetch: bool = False):
    """Выполнить SQL, опционально вернуть результат."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        if fetch:
            result = cur.fetchall()
        else:
            result = cur.rowcount
        conn.commit()
        return result
    except Exception as e:
        conn.rollback()
        logger.error(f"SQL error: {e}")
        raise
    finally:
        conn.close()


def execute_sql_returning_count(sql: str, params=None) -> int:
    """Выполнить INSERT и вернуть количество вставленных строк."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        count = cur.rowcount
        conn.commit()
        logger.info(f"Affected rows: {count}")
        return count
    except Exception as e:
        conn.rollback()
        logger.error(f"SQL error: {e}")
        raise
    finally:
        conn.close()
