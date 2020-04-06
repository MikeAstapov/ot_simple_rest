import logging
import uuid

import tornado.web


__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.2.1"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


class CheckJob(tornado.web.RequestHandler):
    """
    This handler helps OT.Simple Splunk app JobLoader to check Job's status and then (optionally)
    to download results from ramcache.

    1. Remove OT.Simple Splunk app service data from SPL query.
    2. Get Job's status based on (original_spl, tws, twf) parameters.
    3. Check Job's status and return it to OT.Simple Splunk app if it is not still ready.
    4. Load results of Job from cache for transcending.
    5. Return Job's status or results.
    """

    logger = logging.getLogger('osr')

    def initialize(self, manager):
        """
        Gets config and init logger.

        :param manager: Jobs manager object.

        :return:
        """
        self.handler_id = str(uuid.uuid4())
        self.jobs_manager = manager
        self.logger = logging.getLogger('osr_hid')

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
            self.logger.debug(f'Error_msg: {error_msg}', extra={'hid': self.handler_id})
            self.finish(error_msg)

    async def get(self):
        """
        It writes response to remote side.

        :return:
        """
        response = self.jobs_manager.check_job(hid=self.handler_id,
                                               request=self.request)
        self.logger.debug(f'CheckJob RESPONSE: {response}', extra={'hid': self.handler_id})
        self.write(response)
