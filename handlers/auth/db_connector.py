import logging

import tornado.util

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Andrey Starchenkov"
__email__ = "akhromov@ot.ru"
__status__ = "Development"


class PostgresConnector:
    def __init__(self, conn_pool):
        self.pool = conn_pool
        self.logger = logging.getLogger('osr')

    def row_to_obj(self, row, cur):
        """Convert a SQL row to an object supporting dict and attribute access."""
        obj = tornado.util.ObjectDict()
        for val, desc in zip(row, cur.description):
            obj[desc.name] = val
        return obj

    def execute_query(self, query, params=None, with_commit=False,
                      with_fetch=True, as_obj=False, fetchall=False):
        fetch = None
        conn = self.pool.getconn()
        cur = conn.cursor()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        if with_fetch:
            if fetchall:
                fetch = cur.fetchall()
                if as_obj:
                    fetch = [self.row_to_obj(row, cur) for row in fetch]
            else:
                fetch = cur.fetchone()
                if as_obj:
                    fetch = self.row_to_obj(fetch, cur)
        if with_commit:
            conn.commit()
        self.pool.putconn(conn)
        return fetch

    def check_user_exists(self, username):
        query_str = "SELECT * FROM users WHERE username = %s;"
        user_data = self.execute_query(query_str, (username,), as_obj=True)
        return True if user_data else False

    def add_user(self, *, role, username, password):
        query_str = "WITH role as (SELECT id FROM roles WHERE role_name = %s) " \
                    "INSERT INTO users (username, password, role_id) " \
                    "VALUES (%s, %s, (SELECT id FROM role)) RETURNING id;"
        user_id = self.execute_query(query_str, (role, username, password), with_commit=True, as_obj=True)
        return user_id

    def get_user_data(self, username):
        query_str = "SELECT users.*, roles.role_name, roles.rights, sessions.token FROM users " \
                    "LEFT OUTER JOIN roles ON users.role_id = roles.id LEFT OUTER JOIN sessions " \
                    "ON sessions.user_id = users.id WHERE users.username = %s;"
        user_data = self.execute_query(query_str, (username,), as_obj=True, fetchall=True)
        return user_data

    def add_session(self, user_id, token):
        query_str = "INSERT INTO sessions (user_id, token) VALUES(%s, %s);"
        self.execute_query(query_str, (user_id, token), with_commit=True, with_fetch=False)
