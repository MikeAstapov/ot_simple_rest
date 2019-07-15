import logging
import os
import re

import tornado.web
import psycopg2
from tornado.ioloop import IOLoop

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.1.2"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


class LoadJob(tornado.web.RequestHandler):
    """
    This handler helps OT.Simple Splunk app JobLoader to check Job's status and then to download results from ramcache.

    1. Remove OT.Simple Splunk app service data from SPL query.
    2. Get Job's status based on (original_spl, tws, twf) parameters.
    3. Check Job's status and return it to OT.Simple Splunk app if it is not still ready.
    4. Load results of Job from cache for transcending.
    5. Return Job's status or results.
    """

    logger = logging.getLogger('osr')

    def initialize(self, db_conf, mem_conf):
        """
        Gets config and init logger.

        :param db_conf: DB config.
        :type db_conf: Dictionary.
        :param mem_conf: RAM cache configuration.
        :type mem_conf: Dictionary.
        :return:
        """

        self.db_conf = db_conf
        self.mem_conf = mem_conf

    async def get(self):
        """
        It writes response to remote side.

        :return:
        """

        future = IOLoop.current().run_in_executor(None, self.load_job)
        await future

    def load_job(self):
        """
        It checks for Job's status then downloads the result.

        :return:
        """

        request = self.request.arguments
        self.logger.debug(request)
        # Step 1. Remove OT.Simple Splunk app service data from SPL query.
        original_spl = request["original_spl"][0].decode()
        original_spl = re.sub(r"\|\s*ot\s+(ttl=\d+)?\s*\|", "", original_spl)
        original_spl = re.sub(r"\|\s*simple.*", "", original_spl)
        # Get time window.
        tws = int(float(request['tws'][0]))
        twf = int(float(request['twf'][0]))

        conn = psycopg2.connect(**self.db_conf)
        cur = conn.cursor()

        # Step 2. Get Job's status based on (original_spl, tws, twf) parameters.
        check_job_status = 'SELECT splqueries.id, splqueries.status, cachesdl.expiring_date FROM splqueries ' \
                           'LEFT JOIN cachesdl ON splqueries.id = cachesdl.id WHERE splqueries.original_spl=%s AND ' \
                           'splqueries.tws=%s AND splqueries.twf=%s ORDER BY splqueries.id DESC LIMIT 1 '

        self.logger.info(check_job_status % (original_spl, tws, twf))
        cur.execute(check_job_status, (original_spl, tws, twf))
        fetch = cur.fetchone()
        self.logger.info(fetch)

        # Check if such Job presents.
        if fetch:
            cid, status, expiring_date = fetch

            # Step 3. Check Job's status and return it to OT.Simple Splunk app if it is not still ready.
            if status == 'finished' and expiring_date:

                # Step 4. Load results of Job from cache for transcending.
                events = self.load_from_memcache(cid)
                self.logger.info('Cache is %s loaded.' % cid)
                response = {'status': 'success', 'events': events}
            elif status == 'finished' and not expiring_date:
                response = {'status': 'nocache'}
            elif status == 'running':
                response = {'status': 'running'}
            elif status == 'new':
                response = {'status': 'new'}
            elif status == 'fail':
                response = {'status': 'fail', 'error': 'Job is failed'}
            else:
                self.logger.warning('Unknown status of job: %s' % status)
                response = {'status': 'fail', 'error': 'Unknown status: %s' % status}
        else:
            # Return missed job error.
            response = {'status': 'fail', 'error': 'Job is not found'}

        # Step 5. Write Job's status or results.
        self.write(response)

    def load_from_memcache(self, cid):
        """
        It loads result's cache from ramcache and then returns it to response writer.
        :param cid: Cache's id.
        :type cid: Integer.
        :return: List of cache table lines.
        """
        self.logger.debug('Started loading cache %s.' % cid)
        events = {}
        path_to_cache_dir = '%s/search_%s.cache/' % (self.mem_conf['path'], cid)
        self.logger.debug('Path to cache %s.' % path_to_cache_dir)
        file_names = os.listdir(path_to_cache_dir)
        for file_name in file_names:
            self.logger.debug('Reading part: %s' % file_name)
            if file_name[-4:] == '.csv':
                events[file_name] = open(path_to_cache_dir + file_name).read()
        return events

