import logging
from contextlib import contextmanager

import tornado.util
import psycopg2
from psycopg2.pool import PoolError
from asyncio import sleep

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Anton Khromov"
__email__ = "akhromov@ot.ru"
__status__ = "Production"


class PGConnector:
    """
    Base Postgres connector class with no specific methods
    """

    def __init__(self, conn_pool):
        self.pool = conn_pool
        self.logger = logging.getLogger('osr')

    @contextmanager
    def transaction(self, name="transaction", **kwargs):
        options = {
            "isolation_level": kwargs.get("isolation_level", None),
            "readonly": kwargs.get("readonly", None),
            "deferrable": kwargs.get("deferrable", None),
        }
        while True:
            try:
                conn = self.pool.getconn()
            except PoolError:
                sleep(0.1)
            else:
                break

        try:
            conn.set_session(**options)
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Transaction {name} error: {e}")
            raise RuntimeError(f"Transaction {name} failed")
        finally:
            conn.reset()
            self.pool.putconn(conn)

    def row_to_obj(self, row, cur):
        """Convert a SQL row to an object supporting dict and attribute access."""
        obj = tornado.util.ObjectDict()
        for val, desc in zip(row, cur.description):
            obj[desc.name] = val
        return obj

    def execute_query(self, query, conn=None, params=None, with_commit=False,
                      with_fetch=True, as_obj=False, fetchall=False):
        fetch_result = None

        if not conn:
            with_transaction = False
            while True:
                try:
                    conn = self.pool.getconn()
                except PoolError:
                    sleep(0.1)
                else:
                    break
        else:
            with_transaction = True

        cur = conn.cursor()

        try:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            if with_fetch:
                if fetchall:
                    fetch_result = cur.fetchall()
                    if as_obj:
                        fetch_result = [self.row_to_obj(row, cur) for row in fetch_result]
                else:
                    fetch_result = cur.fetchone()
                    if as_obj and fetch_result:
                        fetch_result = self.row_to_obj(fetch_result, cur)
            if with_commit:
                conn.commit()
        except psycopg2.OperationalError as err:
            self.logger.error(f'SQL Error: {err}')
        else:
            return fetch_result
        finally:
            cur.close()
            if not with_transaction:
                self.pool.putconn(conn)
