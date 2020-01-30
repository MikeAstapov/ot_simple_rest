import logging
import re
import os
import json
import asyncio

import psycopg2

from utils import backlasher
from parsers.spl_resolver.Resolver import Resolver


class JobLoader:
    """
    This class contains all of methods for check job status
    and get jobs result from cache.
    """

    logger = logging.getLogger('osr')

    def __init__(self, request, db_conf, mem_conf, tracker_max_interval):
        self.request = request
        self.db_conf = db_conf
        self.mem_conf = mem_conf
        self.tracker_max_interval = tracker_max_interval

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

    def start(self):
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
                    response = ''.join(list(self.load_and_send_from_memcache(cid)))
                    self.logger.info('Cache is %s loaded.' % cid)
                elif status == 'finished' and not expiring_date:
                    response = {'status': 'nocache'}
                elif status == 'running':
                    response = {'status': status}
                elif status == 'new':
                    response = {'status': status}
                elif status in ['failed', 'canceled']:
                    response = {'status': status, 'error': msg}
                else:
                    self.logger.warning('Unknown status of job: %s' % status)
                    response = {'status': 'failed', 'error': 'Unknown error: %s' % status}
            else:
                # Return missed job error.
                response = {'status': 'notfound', 'error': 'Job is not found'}
        else:
            msg = 'SuperDispatcher is offline. Please check Spark Cluster.'
            self.logger.warning(msg)
            response = {'status': 'failed', 'error': msg}
        return response

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
        yield '{"status": "success", "schema": "%s", "events": {' % df_schema.strip()
        length = len(file_names)
        for i in range(length):
            file_name = file_names[i]
            self.logger.debug('Reading part: %s' % file_name)
            yield '"%s": ' % file_name
            with open(path_to_cache_dir + file_name) as fr:
                body = fr.read()
            yield json.dumps(body)
            if i != length - 1:
                yield ", "
        yield '}}'


class JobMaker:
    """
    This class contains all of methods for create job in OT_Dispatcher.
    """

    logger = logging.getLogger('osr')

    def __init__(self, request, db_conf, resolver_conf, check_index_access):
        self.request = request
        self.db_conf = db_conf
        self.resolver_conf = resolver_conf
        self.check_index_access = check_index_access

    @staticmethod
    def validate():
        # TODO
        return True

    def user_has_right(self, username, indexes, cur):
        """
        It checks Role Model if user has access to requested indexes.

        :param username: User from query meta.
        :type username: String.
        :param indexes: Requested indexes parsed from SPL query.
        :type indexes: List.
        :param cur: Cursor to Postgres DB.
        :return: Boolean access flag and resolved indexes.
        """
        if not self.check_index_access:
            return True, indexes

        accessed_indexes = []
        if indexes:
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
                    for index in indexes:
                        index = index.replace('"', '').replace('\\', '')
                        for _index in _indexes:
                            indexes_from_rm = re.findall(index.replace("*", ".*"), _index)
                            self.logger.debug("Indexes from rm: %s. Left index: %s. Right index: %s." % (
                                indexes_from_rm, index, _index
                            ))
                            for ifrm in indexes_from_rm:
                                accessed_indexes.append(ifrm)
                if accessed_indexes:
                    access_flag = True
            self.logger.debug('User has a right: %s' % access_flag)
        else:
            access_flag = True
        return access_flag, accessed_indexes

    def check_cache(self, cache_ttl, original_spl, tws, twf, cur, field_extraction, preview):
        """
        It checks if the same query Job is already finished and it's cache is ready to be downloaded. This way it will
        return it's id for OT.Simple Splunk app JobLoader to download it's cache.
        :param original_spl: Original SPL query.
        :type original_spl: String.
        :param cache_ttl: Time To Life of cache.
        :param tws: Time Window Start.
        :type tws: Integer.
        :param twf: Time Window Finish.
        :type twf: Integer.
        :param cur: Cursor to Postgres DB.
        :param field_extraction: Field Extraction mode.
        :type field_extraction: Boolean.
        :param preview: Preview mode.
        :type preview: Boolean.
        :return: Job cache's id and date of creating.
        """
        cache_id = creating_date = None
        self.logger.debug('cache_ttl: %s' % cache_ttl)
        if cache_ttl:
            check_cache_statement = 'SELECT id, extract(epoch from creating_date) FROM cachesdl WHERE expiring_date >= \
            CURRENT_TIMESTAMP AND original_spl=%s AND tws=%s AND twf=%s AND field_extraction=%s AND preview=%s;'
            stm_tuple = (original_spl, tws, twf, field_extraction, preview)
            self.logger.info(check_cache_statement % stm_tuple)
            cur.execute(check_cache_statement, stm_tuple)
            fetch = cur.fetchone()
            if fetch:
                cache_id, creating_date = fetch
        self.logger.debug('cache_id: %s, creating_date: %s' % (cache_id, creating_date))
        return cache_id, creating_date

    def check_running(self, original_spl, tws, twf, cur, field_extraction, preview):
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
        :param field_extraction: Field Extraction mode.
        :type field_extraction: Boolean.
        :param preview: Preview mode.
        :type preview: Boolean.
        :return: Job's id and date of creating.
        """
        check_running_statement = "SELECT id, extract(epoch from creating_date) FROM splqueries \
        WHERE status = 'running' AND original_spl=%s AND tws=%s AND twf=%s AND field_extraction=%s AND preview=%s;"
        stm_tuple = (original_spl, tws, twf, field_extraction, preview)
        self.logger.info(check_running_statement % stm_tuple)
        cur.execute(check_running_statement, stm_tuple)
        fetch = cur.fetchone()

        if fetch:
            job_id, creating_date = fetch
        else:
            job_id = creating_date = None

        self.logger.debug('job_id: %s, creating_date: %s' % (job_id, creating_date))
        return job_id, creating_date

    async def start(self):
        """
        It checks for the same query Jobs and returns id for loading results to OT.Simple Splunk app.

        :return:
        """
        """
        It checks for the same query Jobs and returns id for loading results to OT.Simple Splunk app.
        :return:
        """
        request = self.request.body_arguments
        self.logger.debug('Request: %s' % request)

        # Step 1. Remove OT.Simple Splunk app service data from SPL query.
        original_spl = request['original_spl'][0].decode()
        self.logger.debug("Original spl: %s" % original_spl)
        original_spl = re.sub(r"\|\s*ot\s[^|]*\|", "", original_spl)
        original_spl = re.sub(r"\|\s*simple", "", original_spl)
        original_spl = original_spl.replace("oteval", "eval")
        original_spl = original_spl.strip()
        self.logger.debug('Fixed original_spl: %s' % original_spl)

        # Step 2. Get Role Model information about query and user.
        username = request['username'][0].decode()
        indexes = re.findall(r"index=(\S+)", original_spl)

        # Get search time window.
        tws = int(float(request['tws'][0]))
        twf = int(float(request['twf'][0]))

        # Get cache lifetime.
        cache_ttl = int(request['cache_ttl'][0])

        # Get field extraction mode.
        field_extraction = request['field_extraction'][0]
        field_extraction = True if field_extraction == b'True' else False

        # Get preview mode.
        preview = request['preview'][0]
        preview = True if preview == b'True' else False

        # Update time window to discrete value.
        tws, twf = backlasher.discretize(tws, twf, cache_ttl if cache_ttl else 0)
        self.logger.debug("Discrete time window: [%s,%s]." % (tws, twf))

        sid = request['sid'][0].decode()

        conn = psycopg2.connect(**self.db_conf)
        cur = conn.cursor()

        # Step 4. Check for Role Model Access to requested indexes.
        access_flag, indexes = self.user_has_right(username, indexes, cur)
        if access_flag:
            self.logger.debug("User has access. Indexes: %s." % indexes)
            resolver = Resolver(indexes, tws, twf, cur, sid, self.request.remote_ip,
                                self.resolver_conf.get('no_subsearch_commands'))
            resolved_spl = resolver.resolve(original_spl)
            self.logger.debug("Resolved_spl: %s" % resolved_spl)

            # Step 5. Make searches queue based on subsearches of main query.
            searches = []
            for search in resolved_spl['subsearches'].values():
                if ('otrest' or 'otloadjob') in search[0]:
                    continue
                searches.append(search)

            # Append main search query to the end.
            searches.append(resolved_spl['search'])
            self.logger.debug("Searches: %s" % searches)
            response = {"status": "fail", "error": "No any searches were resolved"}
            for search in searches:

                # Step 6. Check if the same query Job is already calculated and has ready cache.
                cache_id, creating_date = self.check_cache(cache_ttl, search[0], tws, twf, cur, field_extraction,
                                                           preview)

                if cache_id is None:
                    self.logger.debug('No cache')

                    # Check for validation.
                    if self.validate():

                        # Step 7. Check if the same query Job is already be running.
                        job_id, creating_date = self.check_running(search[0], tws, twf, cur, field_extraction,
                                                                   preview)
                        self.logger.debug('Running job_id: %s, creating_date: %s' % (job_id, creating_date))
                        if job_id is None:

                            # Form the list of subsearches for each search.
                            subsearches = []
                            if 'subsearch=' in search[1]:
                                _subsearches = re.findall(r'subsearch=([\w\d]+)', search[1])
                                for each in _subsearches:
                                    subsearches.append(resolved_spl['subsearches'][each][0])

                            # Step 8. Register new Job in Dispatcher DB.
                            self.logger.debug('Search: %s. Subsearches: %s.' % (search[1], subsearches))
                            make_job_statement = 'INSERT INTO splqueries \
                            (original_spl, service_spl, subsearches, tws, twf, cache_ttl, username, field_extraction,' \
                                                 ' preview) \
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id, extract(epoch from creating_date);'

                            stm_tuple = (search[0], search[1], subsearches, tws, twf, cache_ttl, username,
                                         field_extraction, preview)
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
            self.logger.debug("User has no access.")
            response = {"status": "fail", "error": "User has no access to index"}

        self.logger.debug('Response: %s' % response)
        await asyncio.sleep(0.001)
        # return response
