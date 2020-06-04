import logging
import asyncio
from datetime import datetime

from jobs_manager.jobs import Job
from handlers.jobs.db_connector import PostgresConnector

logger = logging.getLogger('osr')

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.2"
__maintainer__ = "Andrey Starchenkov"
__email__ = "akhromov@ot.ru"
__status__ = "Production"


class JobsManager:
    """
    A JobsManager instance gives an access to manipulations with OT_Dispatcher jobs.

    There is two methods for create auxiliary jobs:
    - make_job: creates job and queue it in asyncio.Queue for scheduled executing.
                This action creates JOB in OT_Dispatcher.
    - check_job: creates job and start it immediately.
                This action checks status JOB in OT_Dispatcher.
    - load_job: creates job and start it immediately.
                This action checks status JOB in OT_Dispatcher and load results of finished job.
    Also we have a _start_monitoring method to identify and run jobs from jobs queue.

    Job from the queue will be started later, when _start_monitoring detect it.
    If jobs queue is empty, monitoring is waiting for new jobs.
    """

    def __init__(self, db_conn_pool, mem_conf, disp_conf,
                 resolver_conf):
        self.db_conn = PostgresConnector(db_conn_pool)
        self.mem_conf = mem_conf
        self.disp_conf = disp_conf
        self.r_conf = resolver_conf
        self.tracker_max_interval = float(disp_conf['tracker_max_interval'])

        self._enable = False
        self.jobs_queue = asyncio.Queue(maxsize=100)

        logger.info('Jobs manager started')

    async def make_job(self, *, hid, request, indexes):
        """
        Creates Job instance with needed params and queue it for create job.

        :param hid:         handler identifier
        :param request:     request object from handler
        :param indexes:     list of accessed indexes
        :return:            None
        """
        try:
            parent_job = Job(id=hid,
                             request=request,
                             indexes=indexes,
                             db_conn=self.db_conn,
                             mem_conf=self.mem_conf,
                             resolver_conf=self.r_conf,
                             tracker_max_interval=self.tracker_max_interval)
            parent_job.resolve()
            resolved_data = parent_job.resolved_data

            for search in resolved_data['searches']:
                if search == resolved_data['searches'][-1]:
                    parent_job.search = search
                    await self.jobs_queue.put(parent_job)
                else:
                    job = Job(id=hid,
                              request=request,
                              indexes=indexes,
                              db_conn=self.db_conn,
                              mem_conf=self.mem_conf,
                              resolver_conf=self.r_conf,
                              tracker_max_interval=self.tracker_max_interval)
                    job.resolved_data = resolved_data
                    job.search = search
                    await self.jobs_queue.put(job)
        except Exception as err:
            response = {"status": "fail", "timestamp": str(datetime.now()), "error": str(err)}
        else:
            response = {"status": "success", "timestamp": str(datetime.now())}
            logger.debug('MakeJob was queued')
        return response

    def check_job(self, *, hid, request, with_load=False):
        """
        Creates Job instance with needed params and start it for check job.

        :param hid:         handler identifier
        :param request:     request object from handler
        :param with_load:   sign of need load results after finish
        :return:            results of checking job
        """
        job = Job(id=hid,
                  request=request,
                  db_conn=self.db_conn,
                  mem_conf=self.mem_conf,
                  resolver_conf=self.r_conf,
                  tracker_max_interval=self.tracker_max_interval)
        logger.debug('CheckJob was created')
        job.start_check(with_load)
        return job.status

    def load_job(self, *, hid, request):
        """
        Creates Job instance with needed params and start it for check job and load results.

        :param hid:         handler identifier
        :param request:     request object from handler
        :return:            results of loading job
        """
        return self.check_job(hid=hid, request=request, with_load=True)

    async def _start_monitoring(self):
        """
        Runs endless loop with jobs check and execute code in it.

        :return:        None
        """
        logger.info('Watchdog was started')
        loop = asyncio.get_event_loop()
        while self._enable:
            if not self.jobs_queue.empty():
                job = await self.jobs_queue.get()
                logging.debug('Got a job from queue')
                loop.create_task(job.start_make())
            else:
                await asyncio.sleep(0.05)
        logger.info('Manager was stopped')

    def start(self):
        """
        Starts manager work.

        :return:        None
        """
        self._enable = True
        asyncio.ensure_future(self._start_monitoring())

    def stop(self):
        """
        Stops manager work.

        :return:        None
        """
        self._enable = False
