import re
import logging

import tornado.web
import psycopg2


class MakeJob(tornado.web.RequestHandler):

    def initialize(self, db_conf):
        self.db_conf = db_conf
        self.logger = logging.getLogger('osr')
        self.logger.info('Initialized')

    def post(self):
        response = self.make_job()
        self.write(response)

    @staticmethod
    def validate():
        # TODO
        return True

    def check_cache(self, cache_ttl, original_spl, tws, twf, cur):
        cache_id = creating_date = None
        self.logger.debug('cache_ttl: %s' % cache_ttl)
        if cache_ttl:
            check_cache_statement = 'SELECT id, extract(epoch from creating_date) FROM cachesdl WHERE expiring_date >= \
            CURRENT_TIMESTAMP AND original_spl=%s AND tws=%s AND twf=%s;'
            self.logger.info(check_cache_statement % (original_spl, tws, twf))
            cur.execute(check_cache_statement, (original_spl, tws, twf))
            fetch = cur.fetchone()
            if fetch:
                cache_id, creating_date = fetch
        self.logger.debug('cache_id: %s, creating_date: %s' % (cache_id, creating_date))
        return cache_id, creating_date

    def check_running(self, original_spl, tws, twf, cur):
        check_running_statement = "SELECT id, extract(epoch from creating_date) FROM splqueries \
        WHERE status = 'running' AND original_spl=%s AND tws=%s AND twf=%s;"
        self.logger.info(check_running_statement % (original_spl, tws, twf))
        cur.execute(check_running_statement, (original_spl, tws, twf))
        fetch = cur.fetchone()

        if fetch:
            job_id, creating_date = fetch
        else:
            job_id = creating_date = None

        self.logger.debug('job_id: %s, creating_date: %s' % (job_id, creating_date))
        return job_id, creating_date

    def user_have_right(self, username, indexes, cur):
        check_user_role_stm = "SELECT indexes FROM RoleModel WHERE username = %s;"
        self.logger.debug(check_user_role_stm % (username,))
        cur.execute(check_user_role_stm, (username,))
        fetch = cur.fetchone()
        access_flag = False
        if fetch:
            _indexes = fetch[0]
            if '*' in _indexes:
                access_flag = True
            else:
                index_count = 0
                for index in indexes:
                    if index in _indexes:
                        index_count += 1
                if index_count == len(indexes):
                    access_flag = True
        self.logger.debug('User has a right: %s' % access_flag)
        return access_flag

    def make_job(self):
        request = self.request.body_arguments
        self.logger.debug(request)
        original_spl = request["original_spl"][0].decode()

        username = request["username"][0].decode()
        splitted_spl = original_spl.split('|')
        indexes = re.findall(r"index=(\S+)", splitted_spl[2])

        conn = psycopg2.connect(**self.db_conf)
        cur = conn.cursor()

        if self.user_have_right(username, indexes, cur):

            cache_ttl = int(request['cache_ttl'][0])
            tws = int(float(request['tws'][0]))
            twf = int(float(request['twf'][0]))
            cache_id, creating_date = self.check_cache(cache_ttl, original_spl, tws, twf, cur)

            if cache_id is None:
                self.logger.debug('No cache')

                fields = re.findall(r"\\| ?fields (\S+)+", original_spl)
                filters = list(filter(
                    lambda x: 'index' not in x and 'AND' not in x and x, splitted_spl[2].strip().split(' ')[1:]
                ))
                calculation = splitted_spl[4 if fields else 3:-1]

                if self.validate():

                    job_id, creating_date = self.check_running(original_spl, tws, twf, cur)
                    self.logger.debug('Running job_id: %s, creating_date: %s' % (job_id, creating_date))
                    if job_id is None:
                        make_job_statement = 'INSERT INTO splqueries \
                        (original_spl, indexes, fields, filters, tws, twf, calculation, cache_ttl) \
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id, extract(epoch from creating_date);'
                        self.logger.info(make_job_statement % (original_spl, indexes, fields, filters, tws, twf,
                                                               calculation, cache_ttl))
                        cur.execute(make_job_statement, (
                            original_spl, indexes, fields, filters, tws, twf, calculation, cache_ttl
                        ))
                        job_id, creating_date = cur.fetchone()
                        conn.commit()

                    response = {"_time": creating_date, "status": "success", "job_id": job_id}
                else:
                    response = {"status": "fail", "error": "Validation failed"}
            else:
                response = {"_time": creating_date, "status": "success", "job_id": cache_id}

        else:
            response = {"status": "fail", "error": "User has no access to index"}

        self.logger.debug('Response: %s' % response)
        return response
