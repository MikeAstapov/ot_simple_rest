import logging
import asyncio
from datetime import datetime

from jobs_manager.jobs import Job

logger = logging.getLogger('osr')


class JobsManager:
    """
    A JobsManager instance gives an access to manipulations with OT_Dispatcher jobs.

    There is two methods for create auxiliary jobs:
    - make_job: creates job and queue it in asyncio.Queue for scheduled executing.
                This action creates JOB in OT_Dispatcher.
    - load_job: creates job and start it immediately.
                This action checks status JOB in OT_Dispatcher.
    Also we have a _start_monitoring method to identify and run jobs from jobs queue.

    Job from the queue will be started later, when _start_monitoring detect it.
    If jobs queue is empty, monitoring is waiting for new jobs.
    """
    def __init__(self, db_conf, mem_conf, disp_conf,
                 resolver_conf, user_conf):
        self.db_conf = db_conf
        self.mem_conf = mem_conf
        self.disp_conf = disp_conf
        self.r_conf = resolver_conf
        self.check_index = False if user_conf['check_index_access'] == 'False' else True
        self.tracker_max_interval = float(disp_conf['tracker_max_interval'])

        self._enable = False
        self.jobs_queue = asyncio.Queue(maxsize=100)

        logger.info('Jobs manager started')

    async def make_job(self, request):
        """
        Creates JobMaker instance with needed params and queue it.

        :param request:     request object from handler
        :return:            None
        """
        try:
            job = Job(request=request,
                      db_conf=self.db_conf,
                      mem_conf=self.mem_conf,
                      resolver_conf=self.r_conf,
                      tracker_max_interval=self.tracker_max_interval,
                      check_index_access=self.check_index)
            await self.jobs_queue.put(job)
        except Exception as err:
            response = {"status": "fail", "timestamp": str(datetime.now()), "error": str(err)}
        else:
            response = {"status": "success", "timestamp": str(datetime.now())}
            logger.debug('Make job was queued')
        return response

    def load_job(self, request):
        """
        Creates JobLoader instance with needed params and start it.

        :param request:     request object from handler
        :return:            results of loading job
        """
        job = Job(request=request,
                  db_conf=self.db_conf,
                  mem_conf=self.mem_conf,
                  resolver_conf=self.r_conf,
                  tracker_max_interval=self.tracker_max_interval,
                  check_index_access=self.check_index)
        logger.debug('Load job was created')
        job.start_load()
        return job.status

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
