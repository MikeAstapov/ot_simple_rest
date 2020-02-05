import logging

import psycopg2

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.9.1"
__maintainer__ = "Andrey Starchenkov"
__email__ = "akhromov@ot.ru"
__status__ = "Development"


class PostgresConnector:
    def __init__(self, db_conf):
        self.logger = logging.getLogger('osr')
        self.db_conf = db_conf
        self.conn = psycopg2.connect(**self.db_conf)
        self.cur = self.conn.cursor()

    def check_cache_statement(self, *, original_spl, tws, twf, field_extraction, preview):
        query_str = 'SELECT id, extract(epoch from creating_date) FROM cachesdl WHERE expiring_date >= ' \
                    'CURRENT_TIMESTAMP AND original_spl=%s AND tws=%s AND twf=%s AND field_extraction=%s AND preview=%s;'
        stm_tuple = (original_spl, tws, twf, field_extraction, preview)
        self.logger.info(query_str % stm_tuple)
        self.cur.execute(query_str, stm_tuple)
        return self.cur.fetchone()

    def check_running_statement(self, *, original_spl, tws, twf, field_extraction, preview):
        query_str = 'SELECT id, extract(epoch from creating_date) FROM splqueries ' \
                    'WHERE status = "running" AND original_spl=%s AND tws=%s AND twf=%s AND field_extraction=%s ' \
                    'AND preview=%s;'
        stm_tuple = (original_spl, tws, twf, field_extraction, preview)
        self.logger.info(query_str % stm_tuple)
        self.cur.execute(query_str, stm_tuple)
        return self.cur.fetchone()

    def check_user_role_stm(self, username):
        query_str = 'SELECT indexes FROM RoleModel WHERE username = %s;'
        self.logger.debug(query_str % (username,))
        self.cur.execute(query_str, (username,))
        return self.cur.fetchone()

    def make_job_statement(self, *, search, subsearches, tws, twf, cache_ttl, username, field_extraction, preview):
        query_str = 'INSERT INTO splqueries ' \
                    '(original_spl, service_spl, subsearches, tws, twf, cache_ttl, username, field_extraction, preview) ' \
                    'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id, extract(epoch from creating_date);'
        stm_tuple = (search[0], search[1], subsearches, tws, twf, cache_ttl, username,
                     field_extraction, preview)
        self.logger.info(query_str % stm_tuple)
        self.cur.execute(query_str, stm_tuple)
        result = self.cur.fetchone()
        self.conn.commit()
        return result

    def add_sid_statement(self, *, sid, remote_ip, original_spl):
        query_str = 'INSERT INTO SplunkSIDs (sid, src_ip, spl) VALUES (%s,%s,%s);'
        stm_tuple = (sid, remote_ip, original_spl)
        self.logger.info(query_str % stm_tuple)
        self.cur.execute(query_str, stm_tuple)
        self.conn.commit()

    def make_external_job_statement(self, *, original_spl, service_spl, tws, twf, cache_ttl, username, status):
        query_str = 'INSERT INTO splqueries ' \
                    '(original_spl, service_spl, tws, twf, cache_ttl, username, status) ' \
                    'VALUES (%s,%s,%s,%s,%s,%s,%s) ' \
                    'RETURNING id, extract(epoch from creating_date);'
        stm_tuple = (original_spl, service_spl, tws, twf, cache_ttl, username, status)
        self.logger.debug(query_str % stm_tuple)
        self.cur.execute(query_str, stm_tuple)
        cache_id, creating_date = self.cur.fetchone()
        self.conn.commit()
        return cache_id, creating_date

    def save_to_cache_statement(self, *, original_spl, tws, twf, cache_id, expiring_date):
        save_to_cache_statement = 'INSERT INTO CachesDL (original_spl, tws, twf, id, expiring_date) ' \
                                  'VALUES(%s, %s, %s, %s, to_timestamp(extract(epoch from now()) + %s));'
        stm_tuple = (original_spl, tws, twf, cache_id, expiring_date)
        self.logger.debug(save_to_cache_statement % stm_tuple)
        self.cur.execute(save_to_cache_statement, stm_tuple)
        self.conn.commit()

    def get_datamodel_stm(self, datamodel_name):
        query_str = 'SELECT search FROM DataModels WHERE name = "%s";'
        self.cur.execute(query_str % (datamodel_name,))
        return self.cur.fetchone()

    def get_spl_stm(self, sid, src_ip):
        query_str = 'SELECT spl FROM SplunkSIDs WHERE sid=%s AND src_ip="%s";'
        self.cur.execute(query_str % (sid, src_ip))
        return self.cur.fetchone()

    def check_dispatcher_status(self):
        query_str = 'SELECT (extract(epoch from CURRENT_TIMESTAMP) - extract(epoch from lastcheck)) as delta ' \
                    'from ticks ORDER BY lastcheck DESC LIMIT 1;'
        self.cur.execute(query_str)
        return self.cur.fetchone()

    def check_job_status(self, *, original_spl, tws, twf, field_extraction, preview):
        query_str = 'SELECT splqueries.id, splqueries.status, cachesdl.expiring_date, splqueries.msg ' \
                    'FROM splqueries ' \
                    'LEFT JOIN cachesdl ON splqueries.id = cachesdl.id WHERE splqueries.original_spl=%s AND ' \
                    'splqueries.tws=%s AND splqueries.twf=%s AND splqueries.field_extraction=%s ' \
                    'AND splqueries.preview=%s ORDER BY splqueries.id DESC LIMIT 1 '
        stm_tuple = (original_spl, tws, twf, field_extraction, preview)
        self.logger.info(query_str % stm_tuple)
        self.cur.execute(query_str, stm_tuple)
        return self.cur.fetchone()
