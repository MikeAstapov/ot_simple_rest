import logging
import re
import os
import json
import asyncio

from utils import backlasher
from parsers.spl_resolver.Resolver import Resolver

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Anton Khromov"
__email__ = "akhromov@ot.ru"
__status__ = "Production"


class Job:
    """
    This class contains all of methods for check job status
    and get jobs result from cache.
    """

    logger = logging.getLogger('osr_hid')

    def __init__(self, *, id, request, db_conn, mem_conf, resolver_conf,
                 tracker_max_interval, indexes=None):
        self.handler_id = id
        self.request = request
        self.indexes = indexes
        self.db = db_conn
        self.mem_conf = mem_conf
        self.resolver_conf = resolver_conf
        self.tracker_max_interval = tracker_max_interval

        self.status = {'status': 'created'}

    def check_dispatcher_status(self):
        delta = self.db.check_dispatcher_status()
        self.logger.debug(f"Dispatcher last check: {delta}.", extra={'hid': self.handler_id})
        if delta and delta <= self.tracker_max_interval:
            return True
        return False

    def load_and_send_from_memcache(self, cid):
        """
        It loads result's cache from ramcache and then writes batches.
        :param cid: Cache's id.
        :type cid: Integer.
        :return: List of cache table lines.
        """
        self.logger.debug(f'Started loading cache {cid}.', extra={'hid': self.handler_id})
        _path = self.mem_conf['path']
        path_to_cache_dir = os.path.join(_path, f'search_{cid}.cache/data/')
        self.logger.debug(f'Path to cache {path_to_cache_dir}.', extra={'hid': self.handler_id})
        file_names = [file_name for file_name in os.listdir(path_to_cache_dir) if file_name[-5:] == '.json']
        with open(path_to_cache_dir + "_SCHEMA") as fr:
            df_schema = fr.read()
        yield '{"status": "success", "schema": "%s", "events": {' % df_schema.strip()
        length = len(file_names)
        for i in range(length):
            file_name = file_names[i]
            self.logger.debug(f'Reading part: {file_name}', extra={'hid': self.handler_id})
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
        self.logger.debug(f'cache_ttl: {cache_ttl}', extra={'hid': self.handler_id})
        if cache_ttl:
            cache_id, creating_date = self.db.check_cache(original_spl=original_spl, tws=tws, twf=twf,
                                                          field_extraction=field_extraction, preview=preview)

        self.logger.debug(f'cache_id: {cache_id}, creating_date: {creating_date}', extra={'hid': self.handler_id})
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

        self.logger.debug(f'job_id: {job_id}, creating_date: {creating_date}', extra={'hid': self.handler_id})
        return job_id, creating_date

    def get_request_params(self):
        request = self.request.arguments
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
        self.logger.debug(f'Request: {request}', extra={'hid': self.handler_id})

        # Get cache lifetime.
        cache_ttl = int(request['cache_ttl'][0])

        params = self.get_request_params()
        original_spl = params['original_spl']
        tws, twf = params['tws'], params['twf']
        field_extraction = params['field_extraction']
        preview = params['preview']

        username = request['username'][0].decode()

        self.logger.debug(f"Discrete time window: [{tws},{twf}].", extra={'hid': self.handler_id})

        sid = request['sid'][0].decode()

        resolver = Resolver(self.indexes, tws, twf, self.db, sid, self.request.remote_ip,
                            self.resolver_conf.get('no_subsearch_commands'))
        resolved_spl = resolver.resolve(original_spl)
        self.logger.debug(f"Resolved_spl: {resolved_spl}", extra={'hid': self.handler_id})

        # Make searches queue based on subsearches of main query.
        searches = []
        for search in resolved_spl['subsearches'].values():
            if ('otrest' or 'otloadjob') in search[0]:
                continue
            searches.append(search)

        # Append main search query to the end.
        searches.append(resolved_spl['search'])
        self.logger.debug(f"Searches: {searches}", extra={'hid': self.handler_id})
        response = {"status": "fail", "error": "No any searches were resolved"}
        for search in searches:

            # Check if the same query Job is already calculated and has ready cache.
            cache_id, creating_date = self.check_cache(cache_ttl, search[0], tws, twf, field_extraction, preview)

            if cache_id is None:
                self.logger.debug(f'No cache', extra={'hid': self.handler_id})

                # Check for validation.
                if self.validate():

                    # Check if the same query Job is already be running.
                    job_id, creating_date = self.check_running(search[0], tws, twf, field_extraction, preview)
                    self.logger.debug(f'Running job_id: {job_id}, creating_date: {creating_date}',
                                      extra={'hid': self.handler_id})
                    if job_id is None:

                        # Form the list of subsearches for each search.
                        subsearches = []
                        if 'subsearch=' in search[1]:
                            _subsearches = re.findall(r'subsearch=([\w\d]+)', search[1])
                            for each in _subsearches:
                                subsearches.append(resolved_spl['subsearches'][each][0])

                        # Register new Job in Dispatcher DB.
                        self.logger.debug(f'Search: {search[1]}. Subsearches: {subsearches}.',
                                          extra={'hid': self.handler_id})
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

        self.logger.debug(f'Response: {response}', extra={'hid': self.handler_id})
        self.status = response
        await asyncio.sleep(0.001)

    def start_check(self, with_load=False):
        """
        It checks for Job's status then downloads the result.

        :return:
        """
        dispatcher_status = self.check_dispatcher_status()
        if not dispatcher_status:
            msg = 'SuperDispatcher is offline. Please check Spark Cluster.'
            self.logger.warning(msg, extra={'hid': self.handler_id})
            self.status = {'status': 'failed', 'error': msg}
            return

        params = self.get_request_params()
        original_spl = params['original_spl']
        tws, twf = params['tws'], params['twf']
        field_extraction = params['field_extraction']
        preview = params['preview']

        self.logger.debug(f"Discrete time window: [{tws},{twf}].", extra={'hid': self.handler_id})

        # Step 2. Get Job's status based on (original_spl, tws, twf) parameters.

        job_status_data = self.db.check_job_status(original_spl=original_spl, tws=tws, twf=twf,
                                                   field_extraction=field_extraction, preview=preview)
        self.logger.info(job_status_data, extra={'hid': self.handler_id})

        # Check if such Job presents.
        if job_status_data:
            cid, status, expiring_date, msg = job_status_data
            # Step 3. Check Job's status and return it to OT.Simple Splunk app if it is not still ready.
            if status == 'finished' and expiring_date:
                if with_load:
                    # Step 4. Load results of Job from cache for transcending.
                    response = ''.join(list(self.load_and_send_from_memcache(cid)))
                    self.logger.info(f'Cache cid={cid} was loaded.', extra={'hid': self.handler_id})
                else:
                    self.logger.info(f'Cache for task_id={cid} was found.', extra={'hid': self.handler_id})
                    response = {'status': 'success', 'cid': cid}
            elif status == 'finished' and not expiring_date:
                response = {'status': 'nocache', 'error': 'No cache for this job'}
            elif status in ['new', 'running']:
                response = {'status': status}
            elif status in ['failed', 'canceled']:
                response = {'status': status, 'error': msg}
            else:
                self.logger.warning(f'Unknown status of job: {status}', extra={'hid': self.handler_id})
                response = {'status': 'failed', 'error': f'Unknown error: {status}'}
        else:
            # Return missed job error.
            response = {'status': 'notfound', 'error': 'Job is not found'}
        self.status = response
