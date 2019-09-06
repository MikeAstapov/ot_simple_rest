import logging

from hashlib import sha256

import tornado.web
import psycopg2
from tornado.ioloop import IOLoop

from utils.cachewriter import CacheWriter

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.2.1"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


class SaveOtRest(tornado.web.RequestHandler):
    """
    Saves results of OT.Simple Splunk app's otrest command to separate cache file as a result of normal search query.
    """

    logger = logging.getLogger('osr')

    def initialize(self, db_conf, mem_conf):
        """
        Gets config and init logger.

        :param db_conf: DB config.
        :param mem_conf: RAM cache config.
        :return:
        """
        self.db_conf = db_conf
        self.mem_conf = mem_conf

    def write_error(self, status_code: int, **kwargs) -> None:
        """Override to implement custom error pages.

        ``write_error`` may call `write`, `render`, `set_header`, etc
        to produce output as usual.

        If this error was caused by an uncaught exception (including
        HTTPError), an ``exc_info`` triple will be available as
        ``kwargs["exc_info"]``.  Note that this exception may not be
        the "current" exception for purposes of methods like
        ``sys.exc_info()`` or ``traceback.format_exc``.
        """
        if "exc_info" in kwargs:
            error = str(kwargs["exc_info"][1])
            error_msg = {"status": "rest_error", "server_error": self._reason, "status_code": status_code,
                         "error": error}
            self.logger.debug('Error_msg: %s' % error_msg)
            self.finish(error_msg)

    async def post(self):
        """
        It writes response to remote side.
        :return:
        """
        future = IOLoop.current().run_in_executor(None, self.save_to_cache)
        await future

    @staticmethod
    def validate():
        # TODO
        return True

    def save_to_cache(self):
        """
        Registers new external Job and it's cache, then saves it in RAM cache.
        :return:
        """
        request = self.request.body_arguments
        original_spl = request['original_spl'][0].decode()
        sha_spl = 'otrest%s' % original_spl.split('otrest')[1]
        data = request['data'][0].decode()
        self.logger.debug('Original SPL: %s.' % original_spl)
        self.logger.debug('Data: %s.' % data)
        # Parses params for DB tables.
        service_spl = '| otrest subsearch=subsearch_%s' % sha256(sha_spl.encode()).hexdigest()

        if self.validate():
            conn = psycopg2.connect(**self.db_conf)
            cur = conn.cursor()
            # Registers new Job.
            make_external_job_statement = 'INSERT INTO splqueries ' \
                                          '(original_spl, service_spl, tws, twf, cache_ttl, username, status) ' \
                                          'VALUES (%s,%s,%s,%s,%s,%s,%s) ' \
                                          'RETURNING id, extract(epoch from creating_date);'
            stm_tuple = (original_spl, service_spl, 0, 0, 60, '_ot_simple_rest', 'external')
            self.logger.debug(make_external_job_statement % stm_tuple)
            cur.execute(make_external_job_statement, stm_tuple)
            cache_id, creating_date = cur.fetchone()
            # Writes data to RAM cache.
            CacheWriter(data, cache_id, self.mem_conf).write()
            # Registers cache in Dispatcher's DB.
            save_to_cache_statement = 'INSERT INTO CachesDL (original_spl, tws, twf, id, expiring_date) ' \
                                      'VALUES(%s, %s, %s, %s, to_timestamp(extract(epoch from now()) + %s));'
            stm_tuple = (original_spl, 0, 0, cache_id, 60)
            self.logger.debug(save_to_cache_statement % stm_tuple)
            cur.execute(save_to_cache_statement, stm_tuple)
            conn.commit()

            response = {"_time": creating_date, "status": "success", "job_id": cache_id}

        else:
            response = {"status": "fail", "error": "Validation failed"}

        self.write(response)
