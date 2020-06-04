import logging

from hashlib import sha256

import tornado.web
from tornado.ioloop import IOLoop

from utils.cachewriter import CacheWriter
from handlers.jobs.db_connector import PostgresConnector

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = ["Anton Khromov"]
__license__ = ""
__version__ = "0.2.3"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Production"


class SaveOtRest(tornado.web.RequestHandler):
    """
    Saves results of OT.Simple Splunk app's otrest command to separate cache file as a result of normal search query.
    """

    logger = logging.getLogger('osr')

    def initialize(self, db_conn_pool, mem_conf):
        """
        Gets config and init logger.

        :param db_conn: DB connector object.
        :param mem_conf: RAM cache config.
        :return:
        """
        self.db = PostgresConnector(db_conn_pool)
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
            self.logger.debug(f'Error_msg: {error_msg}')
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

    def check_cache(self, cache_ttl, original_otl, tws, twf, field_extraction, preview):
        """
        It checks if the same query Job is already finished and it's cache is ready to be downloaded. This way it will
        return it's id for OT.Simple Splunk app JobLoader to download it's cache.

        :param original_otl: Original SPL query.
        :type original_otl: String.
        :param cache_ttl: Time To Life of cache.
        :param tws: Time Window Start.
        :type tws: Integer.
        :param twf: Time Window Finish.
        :type twf: Integer.
        :param field_extraction: Field Extraction mode.
        :type field_extraction: Boolean.
        :param preview: Preview mode.
        :type preview: Boolean.
        :return: Job cache's id and date of creating.
        """
        cache_id = creating_date = None
        self.logger.debug(f'cache_ttl: {cache_ttl}')
        if cache_ttl:
            cache_id, creating_date = self.db.check_cache(original_otl=original_otl, tws=tws, twf=twf,
                                                          field_extraction=field_extraction, preview=preview)
        self.logger.debug(f'cache_id: {cache_id}, creating_date: {creating_date}')
        return cache_id, creating_date

    def save_to_cache(self):
        """
        Registers new external Job and it's cache, then saves it in RAM cache.
        :return:
        """
        request = self.request.body_arguments
        original_otl = request['original_otl'][0].decode()
        cache_ttl = request['cache_ttl'][0].decode()

        self.logger.debug('Original SPL: %s.' % original_otl)

        if self.validate():
            # Check for cache.
            cache_id, creating_date = self.check_cache(cache_ttl, original_otl, 0, 0, False, False)

            if cache_id is None:

                sha_spl = 'otrest%s' % original_otl.split('otrest')[1]
                data = request['data'][0].decode()
                self.logger.debug('Data: %s.' % data)
                service_otl = '| otrest subsearch=subsearch_%s' % sha256(sha_spl.encode()).hexdigest()

                # Registers new Job.
                cache_id, creating_date = self.db.add_external_job(original_otl=original_otl,
                                                                   service_otl=service_otl,
                                                                   tws=0, twf=0, cache_ttl=cache_ttl,
                                                                   username='_ot_simple_rest',
                                                                   status='external')
                # Writes data to RAM cache.
                CacheWriter(data, cache_id, self.mem_conf).write()
                # Registers cache in Dispatcher's DB.
                self.db.add_to_cache(original_otl=original_otl, tws=0, twf=0,
                                     cache_id=cache_id, expiring_date=60)

                response = {"_time": creating_date, "status": "success", "job_id": cache_id}

            else:

                response = {"_time": creating_date, "status": "success", "job_id": cache_id}

        else:
            response = {"status": "fail", "error": "Validation failed"}

        self.write(response)
