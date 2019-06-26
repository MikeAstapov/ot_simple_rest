import logging

from hashlib import sha256

import tornado.web
import psycopg2

from utils.cachewriter import CacheWriter

class SaveOtRest(tornado.web.RequestHandler):

    def initialize(self, db_conf, mem_conf):
        self.db_conf = db_conf
        self.mem_conf = mem_conf
        self.logger = logging.getLogger('osr')
        self.logger.debug('Initialized')

    def post(self):
        response = self.save_to_cache()
        self.write(response)

    @staticmethod
    def validate():
        # TODO
        return True

    def save_to_cache(self):
        request = self.request.body_arguments
        endpoint = request['endpoint'][0]
        data = request['endpoint'][0].decode()
        self.logger.debug('Endpoint: %s.' % endpoint.decode())
        original_spl = '| otrest endpoint="%s"' % endpoint
        service_spl = '| otrest subsearch=%s' % sha256(endpoint).hexdigest()

        if self.validate():
            conn = psycopg2.connect(**self.db_conf)
            cur = conn.cursor()

            make_external_job_statement = 'INSERT INTO splqueries \
                            (original_spl, service_spl, tws, twf, cache_ttl, username) \
                            VALUES (%s,%s,%s,%s,%s,%s) RETURNING id, extract(epoch from creating_date;'
            stm_tuple = (original_spl, service_spl, 0, 0, 60, '_ot_simple_rest')
            self.logger.debug(make_external_job_statement % stm_tuple)
            cache_id, creating_date = cur.execute(make_external_job_statement, stm_tuple)
            save_to_cache_statement = 'INSERT INTO CachesDL (original_spl, tws, twf, id, expiring_date) ' \
                                      'VALUES(%s, %s, %s, %s, to_timestamp(extract(epoch from now()) + %s))'
            stm_tuple = (original_spl, 0, 0, cache_id, 60)
            self.logger.debug(save_to_cache_statement % stm_tuple)
            cur.execute(save_to_cache_statement, stm_tuple)

            CacheWriter(data, cache_id, self.mem_conf).write()

            response = {"_time": creating_date, "status": "success", "job_id": cache_id}

        else:
            response = {"status": "fail", "error": "Validation failed"}

        return response







