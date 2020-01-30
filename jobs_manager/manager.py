import logging
import asyncio

from jobs_manager.jobs import JobLoader, JobMaker

logger = logging.getLogger('osr')


class JobsManager:
    """
    A JobsManager instance gives an access to manipulations with jobs.

    There is two job types:
    - make_job: creates job and queue it in asyncio.Queue
    - load_job: creates job and start it immediately

    Also we have a _start_monitoring method to identify and run jobs from jobs queue.

    Job from the queue will be started later, when _start_monitoring detect it.
    If jobs queue is empty, monitoring is waiting for new jobs.
    """
    def __init__(self, db_conf, mem_conf, disp_conf,
                 resolver_conf, check_index_access):
        self.db_conf = db_conf
        self.mem_conf = mem_conf
        self.disp_conf = disp_conf
        self.r_conf = resolver_conf
        self.check_index = check_index_access
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
        job = JobMaker(request=request,
                       db_conf=self.db_conf,
                       resolver_conf=self.r_conf,
                       check_index_access=self.check_index)
        await self.jobs_queue.put(job)
        logger.debug('Make job was queued')

    def load_job(self, request):
        """
        Creates JobLoader instance with needed params and start it.

        :param request:     request object from handler
        :return:            results of loading job
        """
        job = JobLoader(request=request,
                        db_conf=self.db_conf,
                        mem_conf=self.mem_conf,
                        tracker_max_interval=self.tracker_max_interval)
        logger.debug('Load job was created')
        return job.start()

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
                loop.create_task(job.start())
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
