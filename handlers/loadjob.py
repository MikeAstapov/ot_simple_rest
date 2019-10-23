import json
import logging
import os
import re

import tornado.web
import psycopg2
from tornado.ioloop import IOLoop

from utils import backlasher

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.8.0"
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

    def initialize(self, db_conf, mem_conf, disp_conf):
        """
        Gets config and init logger.

        :param db_conf: DB config.
        :type db_conf: Dictionary.
        :param mem_conf: RAM cache configuration.
        :type mem_conf: Dictionary.
        :param disp_conf: SuperDispatcher configuration.
        :type disp_conf: Dictionary.

        :return:
        """

        self.db_conf = db_conf
        self.mem_conf = mem_conf
        self.tracker_max_interval = float(disp_conf['tracker_max_interval'])

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

    async def get(self):
        """
        It writes response to remote side.

        :return:
        """

        future = IOLoop.current().run_in_executor(None, self.load_job)
        await future

    def check_dispatcher_status(self, cur):
        check_disp_status = """SELECT (extract(epoch from CURRENT_TIMESTAMP) - extract(epoch from lastcheck)) as delta 
        from ticks ORDER BY lastcheck DESC LIMIT 1;"""
        cur.execute(check_disp_status)
        fetch = cur.fetchone()
        self.logger.debug("Dispatcher last check: %s." % fetch)
        if fetch:
            delta = fetch[0]
            if delta <= self.tracker_max_interval:
                return True
        return False

    def load_job(self):
        """
        It checks for Job's status then downloads the result.

        :return:
        """

        conn = psycopg2.connect(**self.db_conf)
        cur = conn.cursor()

        dispatcher_status = self.check_dispatcher_status(cur)
        if dispatcher_status:
            request = self.request.arguments
            self.logger.debug(request)
            # Step 1. Remove OT.Simple Splunk app service data from SPL query.
            original_spl = request["original_spl"][0].decode()
            cache_ttl = re.findall(r"\|\s*ot[^|]*ttl\s*=\s*(\d+)", original_spl)
            field_extraction = re.findall(r"\|\s*ot[^|]*field_extraction\s*=\s*(\S+)", original_spl)
            preview = re.findall(r"\|\s*ot[^|]*preview\s*=\s*(\S+)", original_spl)
            original_spl = re.sub(r"\|\s*ot\s[^|]*\|", "", original_spl)
            original_spl = re.sub(r"\|\s*simple[^\"]*", "", original_spl)
            original_spl = original_spl.replace("oteval", "eval")
            original_spl = original_spl.strip()

            # Get time window.
            tws = int(float(request['tws'][0]))
            twf = int(float(request['twf'][0]))

            # Get Field Extraction mode.
            field_extraction = field_extraction[0] if field_extraction else False

            # Get preview mode.
            preview = preview[0] if preview else False

            # Update time window to discrete value.
            tws, twf = backlasher.discretize(tws, twf, int(cache_ttl[0]) if cache_ttl else int(request['cache_ttl'][0]))
            self.logger.debug("Discrete time window: [%s,%s]." % (tws, twf))

            # Step 2. Get Job's status based on (original_spl, tws, twf) parameters.
            check_job_status = 'SELECT splqueries.id, splqueries.status, cachesdl.expiring_date, splqueries.msg ' \
                               'FROM splqueries ' \
                               'LEFT JOIN cachesdl ON splqueries.id = cachesdl.id WHERE splqueries.original_spl=%s AND ' \
                               'splqueries.tws=%s AND splqueries.twf=%s AND splqueries.field_extraction=%s ' \
                               'AND splqueries.preview=%s ORDER BY splqueries.id DESC LIMIT 1 '

            stm_tuple = (original_spl, tws, twf, field_extraction, preview)
            self.logger.info(check_job_status % stm_tuple)
            cur.execute(check_job_status, stm_tuple)
            fetch = cur.fetchone()
            self.logger.info(fetch)

            # Check if such Job presents.
            if fetch:
                cid, status, expiring_date, msg = fetch
                # Step 3. Check Job's status and return it to OT.Simple Splunk app if it is not still ready.
                if status == 'finished' and expiring_date:
                    # Step 4. Load results of Job from cache for transcending.
                    self.load_and_send_from_memcache(cid)
                    self.logger.info('Cache is %s loaded.' % cid)
                elif status == 'finished' and not expiring_date:
                    response = {'status': 'nocache'}
                    self.write(response)
                elif status == 'running':
                    response = {'status': status}
                    self.write(response)
                elif status == 'new':
                    response = {'status': status}
                    self.write(response)
                elif status in ['failed', 'canceled']:
                    response = {'status': status, 'error': msg}
                    self.write(response)
                else:
                    self.logger.warning('Unknown status of job: %s' % status)
                    response = {'status': 'failed', 'error': 'Unknown error: %s' % status}
                    self.write(response)
            else:
                # Return missed job error.
                response = {'status': 'notfound', 'error': 'Job is not found'}
                self.write(response)
        else:
            msg = 'SuperDispatcher is offline. Please check Spark Cluster.'
            self.logger.warning(msg)
            response = {'status': 'failed', 'error': msg}
            self.write(response)

    def load_and_send_from_memcache(self, cid):
        """
        It loads result's cache from ramcache and then writes batches.
        :param cid: Cache's id.
        :type cid: Integer.
        :return: List of cache table lines.
        """

        self.logger.debug('Started loading cache %s.' % cid)
        path_to_cache_dir = '%s/search_%s.cache/' % (self.mem_conf['path'], cid)
        self.logger.debug('Path to cache %s.' % path_to_cache_dir)
        file_names = [file_name for file_name in os.listdir(path_to_cache_dir) if file_name[-5:] == '.json']
        with open(path_to_cache_dir + "_SCHEMA") as fr:
            df_schema = fr.read()
        self.write('{"status": "success", "schema": "%s", "events": {' % df_schema.strip())
        length = len(file_names)
        for i in range(length):
            file_name = file_names[i]
            self.logger.debug('Reading part: %s' % file_name)
            self.write('"%s": ' % file_name)
            with open(path_to_cache_dir + file_name) as fr:
                body = fr.read()
            self.write(json.dumps(body))
            if i != length - 1:
                self.write(", ")
        self.write('}}')
