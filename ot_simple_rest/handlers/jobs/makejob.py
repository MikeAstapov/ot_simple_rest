import logging

import tornado.web

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = ["Anton Khromov"]
__license__ = ""
__version__ = "0.10.2"
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

    def initialize(self, manager):
        """
        Gets config and init logger.

        :param manager: Jobs manager object
        :return:
        """
        self.jobs_manager = manager
        self.logger = logging.getLogger('osr')

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
        response = await self.jobs_manager.make_job(self.request)
        self.logger.debug(f'MakeJob RESPONSE: {response}')
        self.write(response)
