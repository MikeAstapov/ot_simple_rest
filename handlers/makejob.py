import re
import logging

import tornado.web
import psycopg2
from tornado.ioloop import IOLoop

from parsers.spl_resolver.Resolver import Resolver

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.3.0"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


class MakeJob(tornado.web.RequestHandler):
    """
    This handler is the beginning of a long way of each SPL/OTL search query in OT.Simple Platform.
    The algorithm of search query's becoming to Dispatcher's Job is next:

    1. Remove OT.Simple Splunk app service data from SPL query.
    2. Get Role Model information about query and user.
    3. Get service OTL form of query from original SPL.
    4. Check for Role Model Access to requested indexes.
    5. Make searches queue based on subsearches of main query.
    6. Check if the same (original_spl, tws, twf) query Job is already calculated and has ready cache.
    7. Check if the same query Job is already be running.
    8. Register new Job in Dispatcher DB.
    """

    logger = logging.getLogger('osr')

    def initialize(self, db_conf):
        """
        Gets config and init logger.
        :param db_conf: DB config.
        :type db_conf: Dictionary.
        :return:
        """

        self.db_conf = db_conf

    async def post(self):
        """
        It writes response to remote side.
        :return:
        """

        future = IOLoop.current().run_in_executor(None, self.make_job)
        await future

    @staticmethod
    def validate():
        # TODO
        return True

    def check_cache(self, cache_ttl, original_spl, tws, twf, cur):
        """
        It checks if the sqme query Job is already finished and it's cache is ready to be downloaded. This way it will
        return it's id for OT.Simple Splunk app JobLoader to download it's cache.

        """
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
        """
        It checks if the same query Job is already running. This way it will return id of running job and will not
        register a new one.

        :param original_spl: Original SPL query.
        :type original_spl: String.
        :param tws: Time Window Start.
        :type tws: Integer.
        :param twf: Time Window Finish.
        :type twf: Integer.
        :param cur: Cursor to Postgres DB.
        :return: Job's id and date of creating.
        """
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
        """
        It checks Role Model if user has access to requested indexes.

        :param username: User from query meta.
        :type username: String.
        :param indexes: Requested indexes parsed from SPL query.
        :type indexes: List.
        :param cur: Cursor to Postgres DB.
        :return: Boolean access flag and resolved indexes.
        """
        check_user_role_stm = "SELECT indexes FROM RoleModel WHERE username = %s;"
        self.logger.debug(check_user_role_stm % (username,))
        cur.execute(check_user_role_stm, (username,))
        fetch = cur.fetchone()
        access_flag = False
        _indexes = []
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
        return access_flag, _indexes

    def make_job(self):
        """
        It checks for the same query Jobs and returns id for loading results to OT.Simple Splunk app.

        :return:
        """
        request = self.request.body_arguments
        self.logger.debug('Request: %s' % request)

        # Step 1. Remove OT.Simple Splunk app service data from SPL query.
        original_spl = request['original_spl'][0].decode()
        self.logger.debug("Original spl: %s" % original_spl)
        original_spl = re.sub(r"\|\s*ot\s+(ttl=\d+)?\s*\|", "", original_spl)
        original_spl = re.sub(r"\|\s*simple.*", "", original_spl)
        self.logger.debug('Fixed original_spl: %s' % original_spl)

        # Step 2. Get Role Model information about query and user.
        username = request['username'][0].decode()
        indexes = re.findall(r"index=(\S+)", original_spl)

        tws = int(float(request['tws'][0]))
        twf = int(float(request['twf'][0]))
        sid = request['sid'][0].decode()

        conn = psycopg2.connect(**self.db_conf)
        cur = conn.cursor()

        # Step 3. Get service OTL form of query from original SPL.
        resolver = Resolver(indexes, tws, twf, cur, sid)
        resolved_spl = resolver.resolve(original_spl)
        self.logger.debug("Resolved_spl: %s" % resolved_spl)

        # Step 4. Check for Role Model Access to requested indexes.
        access_flag, indexes = self.user_have_right(username, indexes, cur)

        if access_flag:

            # Get cache lifetime.
            cache_ttl = int(request['cache_ttl'][0])

            # Step 5. Make searches queue based on subsearches of main query.
            searches = []
            for search in resolved_spl['subsearches'].values():
                if 'otrest' in search[0]:
                    continue
                searches.append(search)

            # Append main search query to the end.
            searches.append(resolved_spl['search'])
            self.logger.debug("Searches: %s" % searches)
            response = {"status": "fail", "error": "No any searches were resolved"}
            for search in searches:

                # Step 6. Check if the same query Job is already calculated and has ready cache.
                cache_id, creating_date = self.check_cache(cache_ttl, search[0], tws, twf, cur)

                if cache_id is None:
                    self.logger.debug('No cache')

                    # Check for validation.
                    if self.validate():

                        # Step 7. Check if the same query Job is already be running.
                        job_id, creating_date = self.check_running(original_spl, tws, twf, cur)
                        self.logger.debug('Running job_id: %s, creating_date: %s' % (job_id, creating_date))
                        if job_id is None:

                            # Form the list of subsearches for each search.
                            subsearches = []
                            if 'subsearch=' in search[1]:
                                _subsearches = re.findall(r'subsearch=(\S+)', search[1])
                                for each in _subsearches:
                                    subsearches.append(resolved_spl['subsearches'][each][0])

                            # Step 8. Register new Job in Dispatcher DB.
                            self.logger.debug('Search: %s. Subsearches: %s.' % (search[1], subsearches))
                            make_job_statement = 'INSERT INTO splqueries \
                            (original_spl, service_spl, subsearches, tws, twf, cache_ttl, username) \
                            VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id, extract(epoch from creating_date);'

                            stm_tuple = (search[0], search[1], subsearches, tws, twf, cache_ttl, username)
                            self.logger.info(make_job_statement % stm_tuple)
                            cur.execute(make_job_statement, stm_tuple)
                            job_id, creating_date = cur.fetchone()

                            # Add SID to DB if search is not subsearch.
                            if search == searches[-1]:
                                add_sid_statement = 'INSERT INTO SplunkSIDs (sid, src_ip, spl) VALUES (%s,%s,%s);'
                                stm_tuple = (sid, self.request.remote_ip, original_spl)
                                self.logger.info(add_sid_statement % stm_tuple)
                                cur.execute(add_sid_statement, stm_tuple)

                            conn.commit()

                        # Return id of new Job.
                        response = {"_time": creating_date, "status": "success", "job_id": job_id}

                    else:
                        # Return validation error.
                        response = {"status": "fail", "error": "Validation failed"}

                else:
                    # Return id of the same already calculated Job with ready cache. Ot.Simple Splunk app JobLoader will
                    # request it to download.
                    response = {"_time": creating_date, "status": "success", "job_id": cache_id}

        else:
            # Return Role Model Access error.
            response = {"status": "fail", "error": "User has no access to index"}

        self.logger.debug('Response: %s' % response)
        self.write(response)
