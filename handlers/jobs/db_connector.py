import logging

from psycopg2.pool import ThreadedConnectionPool

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.3"
__maintainer__ = "Andrey Starchenkov"
__email__ = "akhromov@ot.ru"
__status__ = "Development"


# TODO: Realize Postgres connections pool using for each new connection

class PostgresConnector:
    def __init__(self, *, db_conf, min_pool, max_pool):
        self.pool = ThreadedConnectionPool(min_pool, max_pool, **db_conf)
        self.logger = logging.getLogger('osr')

    def execute_query(self, query, params=None, with_commit=False, with_fetch=True):
        fetch = None
        conn = self.pool.getconn()
        cur = conn.cursor()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        if with_fetch:
            fetch = cur.fetchone()
        if with_commit:
            conn.commit()
        self.pool.putconn(conn)
        return fetch

    def check_cache(self, *, original_spl, tws, twf, field_extraction, preview):
        cache_id = creating_date = None
        query_str = "SELECT id, extract(epoch from creating_date) FROM cachesdl WHERE expiring_date >= " \
                    "CURRENT_TIMESTAMP AND original_spl=%s AND tws=%s AND twf=%s AND field_extraction=%s AND preview=%s;"
        stm_tuple = (original_spl, tws, twf, field_extraction, preview)
        self.logger.info(query_str % stm_tuple)

        cache_data = self.execute_query(query_str, stm_tuple)
        if cache_data:
            cache_id, creating_date = cache_data
        return cache_id, creating_date

    def check_running(self, *, original_spl, tws, twf, field_extraction, preview):
        job_id = creating_date = None
        query_str = "SELECT id, extract(epoch from creating_date) FROM splqueries WHERE status = 'running' " \
                    "AND original_spl=%s AND tws=%s AND twf=%s AND field_extraction=%s AND preview=%s;"
        stm_tuple = (original_spl, tws, twf, field_extraction, preview)
        self.logger.info(query_str % stm_tuple)

        job_data = self.execute_query(query_str, stm_tuple)
        if job_data:
            job_id, creating_date = job_data
        return job_id, creating_date

    def check_user_role(self, username):
        query_str = "SELECT indexes FROM RoleModel WHERE username = %s;"
        self.logger.debug(query_str % username)

        indexes = self.execute_query(query_str, (username,))
        if indexes:
            return indexes[0]

    def add_job(self, *, search, subsearches, tws, twf, cache_ttl, username, field_extraction, preview):
        query_str = "INSERT INTO splqueries (original_spl, service_spl, subsearches, tws, twf, cache_ttl, username, " \
                    "field_extraction, preview) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id, " \
                    "extract(epoch from creating_date);"
        stm_tuple = (search[0], search[1], subsearches, tws, twf, cache_ttl, username, field_extraction, preview)
        self.logger.info(query_str % stm_tuple)

        job_id, creating_date = self.execute_query(query_str, stm_tuple, with_commit=True)
        return job_id, creating_date

    def add_sid(self, *, sid, remote_ip, original_spl):
        query_str = "INSERT INTO SplunkSIDs (sid, src_ip, spl) VALUES (%s, %s, %s);"
        stm_tuple = (sid, remote_ip, original_spl)
        self.logger.info(query_str % stm_tuple)
        self.execute_query(query_str, stm_tuple, with_commit=True, with_fetch=False)

    def add_external_job(self, *, original_spl, service_spl, tws, twf, cache_ttl, username, status):
        query_str = "INSERT INTO splqueries (original_spl, service_spl, tws, twf, cache_ttl, username, status) " \
                    "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id, extract(epoch from creating_date);"
        stm_tuple = (original_spl, service_spl, tws, twf, cache_ttl, username, status)
        self.logger.debug(query_str % stm_tuple)

        cache_id, creating_date = self.execute_query(query_str, stm_tuple, with_commit=True)
        return cache_id, creating_date

    def add_to_cache(self, *, original_spl, tws, twf, cache_id, expiring_date):
        query_str = "INSERT INTO CachesDL (original_spl, tws, twf, id, expiring_date) " \
                    "VALUES(%s, %s, %s, %s, to_timestamp(extract(epoch from now()) + %s));"
        stm_tuple = (original_spl, tws, twf, cache_id, expiring_date)
        self.logger.debug(query_str % stm_tuple)
        self.execute_query(query_str, stm_tuple, with_commit=True, with_fetch=False)

    def get_datamodel(self, datamodel_name):
        query_str = "SELECT search FROM DataModels WHERE name = %s;"
        return self.execute_query(query_str, (datamodel_name,))

    def get_spl(self, sid, src_ip):
        query_str = "SELECT spl FROM SplunkSIDs WHERE sid=%s AND src_ip=%s;"
        return self.execute_query(query_str, (sid, src_ip,))

    def check_dispatcher_status(self):
        query_str = "SELECT (extract(epoch from CURRENT_TIMESTAMP) - extract(epoch from lastcheck)) as delta " \
                    "from ticks ORDER BY lastcheck DESC LIMIT 1;"

        time_delta = self.execute_query(query_str)
        if time_delta:
            return time_delta[0]

    def check_job_status(self, *, original_spl, tws, twf, field_extraction, preview):
        query_str = "SELECT splqueries.id, splqueries.status, cachesdl.expiring_date, splqueries.msg " \
                    "FROM splqueries LEFT JOIN cachesdl ON splqueries.id = cachesdl.id " \
                    "WHERE splqueries.original_spl=%s AND splqueries.tws=%s AND splqueries.twf=%s " \
                    "AND splqueries.field_extraction=%s AND splqueries.preview=%s ORDER BY splqueries.id DESC LIMIT 1;"
        stm_tuple = (original_spl, tws, twf, field_extraction, preview)
        self.logger.info(query_str % stm_tuple)

        job_data = self.execute_query(query_str, stm_tuple)
        if job_data:
            cid, status, expiring_date, msg = job_data
            return cid, status, expiring_date, msg

    def clear_data_models(self):
        query_str = "DELETE FROM DataModels;"
        self.execute_query(query_str, with_commit=True, with_fetch=False)

    def add_data_model(self, *, name, search):
        query_str = "INSERT INTO DataModels (name, search) VALUES (%s, %s);"
        stm_tuple = (name, search,)
        self.logger.debug(query_str % stm_tuple)
        self.execute_query(query_str, stm_tuple, with_commit=True, with_fetch=False)

    def clear_roles(self):
        query_str = "DELETE FROM RoleModel;"
        self.execute_query(query_str, with_commit=True, with_fetch=False)

    def add_roles(self, *, username, roles, indexes):
        query_str = "INSERT INTO RoleModel (username, roles, indexes) VALUES (%s, %s, %s);"
        self.execute_query(query_str, (username, roles, indexes,), with_commit=True, with_fetch=False)
