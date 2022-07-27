from tools.pg_connector import PGConnector
from utils.hashes import hash512


__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.4"
__maintainer__ = "Andrey Starchenkov"
__email__ = "akhromov@ot.ru"
__status__ = "Production"


class PostgresConnector(PGConnector):
    def __init(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def check_cache(self, *, original_otl, tws, twf, field_extraction, preview):
        cache_id = creating_date = None
        original_otl = hash512(original_otl)
        query_str = "SELECT id, extract(epoch from creating_date) FROM cachesdl WHERE original_otl=%s AND tws=%s AND twf=%s AND field_extraction=%s AND preview=%s;"
        stm_tuple = (original_otl, tws, twf, field_extraction, preview)
        self.logger.info(query_str % stm_tuple)

        cache_data = self.execute_query(query_str, params=stm_tuple)
        if cache_data:
            cache_id, creating_date = cache_data
        return cache_id, creating_date

    def check_running(self, *, original_otl, tws, twf, field_extraction, preview):
        job_id = creating_date = None
        query_str = "SELECT id, extract(epoch from creating_date) FROM OTLQueries WHERE status IN ('running', 'new') " \
                    "AND original_otl=%s AND tws=%s AND twf=%s AND field_extraction=%s AND preview=%s;"
        stm_tuple = (original_otl, tws, twf, field_extraction, preview)
        self.logger.info(query_str % stm_tuple)

        job_data = self.execute_query(query_str, params=stm_tuple)
        if job_data:
            job_id, creating_date = job_data
        return job_id, creating_date

    def check_user_role(self, username):
        query_str = "SELECT indexes FROM RoleModel WHERE username = %s;"
        self.logger.debug(query_str % username)

        indexes = self.execute_query(query_str, params=(username,), fetchall=True)
        if indexes:
            return indexes[0]

    def add_job(self, *, search, subsearches, tws, twf, cache_ttl, username, field_extraction, preview):
        job_id = creating_date = None
        query_str = "INSERT INTO OTLQueries (original_otl, service_otl, subsearches, tws, twf, cache_ttl, username, " \
                    "field_extraction, preview) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id, " \
                    "extract(epoch from creating_date);"
        stm_tuple = (search[0], search[1], subsearches, tws, twf, cache_ttl, username, field_extraction, preview)
        self.logger.info(query_str % stm_tuple)

        job_data = self.execute_query(query_str, params=stm_tuple, with_commit=True)
        if job_data:
            job_id, creating_date = job_data
        return job_id, creating_date

    def add_sid(self, *, sid, remote_ip, original_otl):
        query_str = "INSERT INTO GUISIDs (sid, src_ip, otl) VALUES (%s, %s, %s);"
        stm_tuple = (sid, remote_ip, original_otl)
        self.logger.info(query_str % stm_tuple)
        self.execute_query(query_str, params=stm_tuple, with_commit=True, with_fetch=False)

    def add_external_job(self, *, original_otl, service_otl, tws, twf, cache_ttl, username, status):
        cache_id = creating_date = None
        query_str = "INSERT INTO OTLQueries (original_otl, service_otl, tws, twf, cache_ttl, username, status) " \
                    "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id, extract(epoch from creating_date);"
        stm_tuple = (original_otl, service_otl, tws, twf, cache_ttl, username, status)
        self.logger.debug(query_str % stm_tuple)

        cache_data = self.execute_query(query_str, params=stm_tuple, with_commit=True)
        if cache_data:
            cache_id, creating_date = cache_data
        return cache_id, creating_date

    def add_to_cache(self, *, original_otl, tws, twf, cache_id, expiring_date):
        original_otl = hash512(original_otl)
        query_str = "INSERT INTO CachesDL (original_otl, tws, twf, id, expiring_date) " \
                    "VALUES(%s, %s, %s, %s, to_timestamp(extract(epoch from now()) + %s));"
        stm_tuple = (original_otl, tws, twf, cache_id, expiring_date)
        self.logger.debug(query_str % stm_tuple)
        self.execute_query(query_str, params=stm_tuple, with_commit=True, with_fetch=False)

    def get_datamodel(self, datamodel_name):
        query_str = "SELECT search FROM DataModels WHERE name = %s;"
        return self.execute_query(query_str, params=(datamodel_name,), fetchall=True)

    def get_otl(self, sid, src_ip):
        query_str = "SELECT otl FROM GUISIDs WHERE sid=%s AND src_ip=%s;"
        return self.execute_query(query_str, params=(sid, src_ip,), fetchall=True)

    def check_dispatcher_status(self):
        query_str = "SELECT (extract(epoch from CURRENT_TIMESTAMP) - extract(epoch from lastcheck)) as delta " \
                    "from ticks ORDER BY lastcheck DESC LIMIT 1;"

        time_delta = self.execute_query(query_str)
        if time_delta:
            return time_delta[0]

    def check_job_status(self, *, original_otl, tws, twf, field_extraction, preview):
        query_str = "SELECT OTLQueries.id, OTLQueries.status, cachesdl.expiring_date, OTLQueries.msg " \
                    "FROM OTLQueries LEFT JOIN cachesdl ON OTLQueries.id = cachesdl.id " \
                    "WHERE OTLQueries.original_otl=%s AND OTLQueries.tws=%s AND OTLQueries.twf=%s " \
                    "AND OTLQueries.field_extraction=%s AND OTLQueries.preview=%s ORDER BY OTLQueries.id DESC LIMIT 1;"
        stm_tuple = (original_otl, tws, twf, field_extraction, preview)
        self.logger.info(query_str % stm_tuple)

        job_data = self.execute_query(query_str, params=stm_tuple)
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
        self.execute_query(query_str, params=stm_tuple, with_commit=True, with_fetch=False)

    def clear_roles(self):
        query_str = "DELETE FROM RoleModel;"
        self.execute_query(query_str, with_commit=True, with_fetch=False)

    def add_roles(self, *, username, roles, indexes):
        query_str = "INSERT INTO RoleModel (username, roles, indexes) VALUES (%s, %s, %s);"
        self.execute_query(query_str, params=(username, roles, indexes,), with_commit=True, with_fetch=False)

    def get_running_jobs_num(self):
        query_str = "SELECT COUNT(*) FROM otlqueries WHERE status = 'running';"
        return self.execute_query(query_str)[0]
