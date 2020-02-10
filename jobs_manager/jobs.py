import logging
import re
import os
import json
import asyncio

from utils import backlasher
from parsers.spl_resolver.Resolver import Resolver

from handlers.jobs.db_connector import PostgresConnector


# TODO: Put the code that works with the database in a separate module.


class Job:
    """
    This class contains all of methods for check job status
    and get jobs result from cache.
    """

    logger = logging.getLogger('osr')

    def __init__(self, request, db_conf, mem_conf, resolver_conf,
                 check_index_access, tracker_max_interval):
        self.request = request
        self.mem_conf = mem_conf
        self.resolver_conf = resolver_conf
        self.tracker_max_interval = tracker_max_interval
        self.check_index_access = check_index_access

        self.db = PostgresConnector(db_conf)
        self.status = {'status': 'created'}

    def check_dispatcher_status(self):
        delta = self.db.check_dispatcher_status()
        self.logger.debug("Dispatcher last check: %s." % delta)
        if delta <= self.tracker_max_interval:
            return True
        return False

    def load_and_send_from_memcache(self, cid):
        """
        It loads result's cache from ramcache and then writes batches.
        :param cid: Cache's id.
        :type cid: Integer.
        :return: List of cache table lines.
        """
        self.logger.debug(f'Started loading cache {cid}.')
        _path = self.mem_conf['path']
        path_to_cache_dir = f'{_path}/search_{cid}.cache/'
        self.logger.debug(f'Path to cache {path_to_cache_dir}.')
        file_names = [file_name for file_name in os.listdir(path_to_cache_dir) if file_name[-5:] == '.json']
        with open(path_to_cache_dir + "_SCHEMA") as fr:
            df_schema = fr.read()
        yield '{"status": "success", "schema": "%s", "events": {' % df_schema.strip()
        length = len(file_names)
        for i in range(length):
            file_name = file_names[i]
            self.logger.debug(f'Reading part: {file_name}')
            yield f'"{file_name}": '
            with open(path_to_cache_dir + file_name) as fr:
                body = fr.read()
            yield json.dumps(body)
            if i != length - 1:
                yield ", "
        yield '}}'

    @staticmethod
    def validate():
        # TODO: Implement in a future release
        return True

    def user_has_right(self, username, indexes):
        """
        It checks Role Model if user has access to requested indexes.

        :param username: User from query meta.
        :type username: String.
        :param indexes: Requested indexes parsed from SPL query.
        :type indexes: List.
        :return: Boolean access flag and resolved indexes.
        """
        if not self.check_index_access or not indexes:
            return True, indexes

        accessed_indexes = []

        user_indexes = self.db.check_user_role(username)
        access_flag = False
        if user_indexes:
            if '*' in user_indexes:
                access_flag = True
            else:
                for index in indexes:
                    index = index.replace('"', '').replace('\\', '')
                    for _index in user_indexes:
                        indexes_from_rm = re.findall(index.replace("*", ".*"), _index)
                        self.logger.debug("Indexes from rm: %s. Left index: %s. Right index: %s." % (
                            indexes_from_rm, index, _index
                        ))
                        for ifrm in indexes_from_rm:
                            accessed_indexes.append(ifrm)
            if accessed_indexes:
                access_flag = True
        self.logger.debug(f'User has a right: {access_flag}')

        return access_flag, accessed_indexes

    def check_cache(self, cache_ttl, original_spl, tws, twf, field_extraction, preview):
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
        :param field_extraction: Field Extraction mode.
        :type field_extraction: Boolean.
        :param preview: Preview mode.
        :type preview: Boolean.
        :return: Job cache's id and date of creating.
        """
        cache_id = creating_date = None
        self.logger.debug('cache_ttl: %s' % cache_ttl)
        if cache_ttl:
            cache_id, creating_date = self.db.check_cache(original_spl=original_spl, tws=tws, twf=twf,
                                                          field_extraction=field_extraction, preview=preview)

        self.logger.debug('cache_id: %s, creating_date: %s' % (cache_id, creating_date))
        return cache_id, creating_date

    def check_running(self, original_spl, tws, twf, field_extraction, preview):
        """
        It checks if the same query Job is already running. This way it will return id of running job and will not
        register a new one.
        :param original_spl: Original SPL query.
        :type original_spl: String.
        :param tws: Time Window Start.
        :type tws: Integer.
        :param twf: Time Window Finish.
        :type twf: Integer.
        :param field_extraction: Field Extraction mode.
        :type field_extraction: Boolean.
        :param preview: Preview mode.
        :type preview: Boolean.
        :return: Job's id and date of creating.
        """
        job_id, creating_date = self.db.check_running(original_spl=original_spl, tws=tws, twf=twf,
                                                      field_extraction=field_extraction, preview=preview)

        self.logger.debug('job_id: %s, creating_date: %s' % (job_id, creating_date))
        return job_id, creating_date

    def get_request_params(self):
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

        # Get Field Extraction mode.
        field_extraction = field_extraction[0] if field_extraction else False

        # Get preview mode.
        preview = preview[0] if preview else False

        # Get time window.
        tws = int(float(request['tws'][0]))
        twf = int(float(request['twf'][0]))

        # Update time window to discrete value.
        tws, twf = backlasher.discretize(tws, twf, int(cache_ttl[0]) if cache_ttl else int(request['cache_ttl'][0]))

        return {'original_spl': original_spl, 'field_extraction': field_extraction,
                'preview': preview, 'tws': tws, 'twf': twf}

    async def start_make(self):
        """
        It checks for the same query Jobs and returns id for loading results to OT.Simple Splunk app.
        :return:
        """
        request = self.request.body_arguments
        self.logger.debug(f'Request: {request}')

        # Get cache lifetime.
        cache_ttl = int(request['cache_ttl'][0])

        params = self.get_request_params()
        original_spl = params['original_spl']
        tws, twf = params['tws'], params['twf']
        field_extraction = params['field_extraction']
        preview = params['preview']

        username = request['username'][0].decode()
        indexes = re.findall(r"index=(\S+)", params['original_spl'])

        self.logger.debug(f"Discrete time window: [{tws},{twf}].")

        sid = request['sid'][0].decode()

        # Step 4. Check for Role Model Access to requested indexes.
        access_flag, indexes = self.user_has_right(username, indexes)
        if access_flag:
            self.logger.debug("User has access. Indexes: %s." % indexes)
            resolver = Resolver(indexes, tws, twf, self.db, sid, self.request.remote_ip,
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
                cache_id, creating_date = self.check_cache(cache_ttl, search[0], tws, twf, field_extraction, preview)

                if cache_id is None:
                    self.logger.debug('No cache')

                    # Check for validation.
                    if self.validate():

                        # Step 7. Check if the same query Job is already be running.
                        job_id, creating_date = self.check_running(search[0], tws, twf, field_extraction, preview)
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
                            job_id, creating_date = self.db.add_job(search=search, subsearches=subsearches,
                                                                    tws=tws, twf=twf, cache_ttl=cache_ttl,
                                                                    username=username,
                                                                    field_extraction=field_extraction,
                                                                    preview=preview)

                            # Add SID to DB if search is not subsearch.
                            if search == searches[-1]:
                                self.db.add_sid(sid=sid, remote_ip=self.request.remote_ip,
                                                original_spl=original_spl)

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

    def start_load(self):
        """
        It checks for Job's status then downloads the result.

        :return:
        """
        dispatcher_status = self.check_dispatcher_status()
        if not dispatcher_status:
            msg = 'SuperDispatcher is offline. Please check Spark Cluster.'
            self.logger.warning(msg)
            self.status = {'status': 'failed', 'error': msg}
            return

        params = self.get_request_params()
        original_spl = params['original_spl']
        tws, twf = params['tws'], params['twf']
        field_extraction = params['field_extraction']
        preview = params['preview']

        self.logger.debug(f"Discrete time window: [{tws},{twf}].")

        # Step 2. Get Job's status based on (original_spl, tws, twf) parameters.

        job_status_data = self.db.check_job_status(original_spl=original_spl, tws=tws, twf=twf,
                                                   field_extraction=field_extraction, preview=preview)
        self.logger.info(job_status_data)

        # Check if such Job presents.
        if job_status_data:
            cid, status, expiring_date, msg = job_status_data
            # Step 3. Check Job's status and return it to OT.Simple Splunk app if it is not still ready.
            if status == 'finished' and expiring_date:
                # Step 4. Load results of Job from cache for transcending.
                response = ''.join(list(self.load_and_send_from_memcache(cid)))
                self.logger.info(f'Cache is {cid} loaded.')
            elif status == 'finished' and not expiring_date:
                response = {'status': 'nocache'}
            elif status in ['new', 'running']:
                response = {'status': status}
            elif status in ['failed', 'canceled']:
                response = {'status': status, 'error': msg}
            else:
                self.logger.warning(f'Unknown status of job: {status}')
                response = {'status': 'failed', 'error': f'Unknown error: {status}'}
        else:
            # Return missed job error.
            response = {'status': 'notfound', 'error': 'Job is not found'}
        self.status = response
