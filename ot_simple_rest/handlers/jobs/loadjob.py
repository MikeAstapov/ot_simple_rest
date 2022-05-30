import logging
import uuid

import tornado.web

__author__ = "Andrey Starchenkov, Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = ["Anton Khromov"]
__license__ = ""
__version__ = "0.9.2"
__maintainer__ = "Andrey Starchenkov"
__email__ = "akhromov@ot.ru"
__status__ = "Production"


class LoadJob(tornado.web.RequestHandler):
    """
    This handler helps OT.Simple OTP app JobLoader to check Job's status and then to download results from ramcache.

    1. Remove OT.Simple OTP app service data from OTP query.
    2. Get Job's status based on (original_otl, tws, twf) parameters.
    3. Check Job's status and return it to OT.Simple OTP app if it is not still ready.
    4. Load results of Job from cache for transcending.
    5. Return Job's status or results.
    """

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
            self.logger.debug(f'Error_msg: {error_msg}')
            self.finish(error_msg)

    async def get(self):
        """
        It writes response to remote side.

        :return:
        """
        try:
            response, strnum = self.jobs_manager.load_job(hid=self.handler_id,
                                                request=self.request)
        except Exception as e:
            error = {'status': 'error', 'msg': str(e)}
            self.logger.error(f"LoadJob RESPONSE: {error}", extra={'hid': self.handler_id})
            return self.write(error)
        self.logger.debug(f'LoadJob RESPONSE: {response}', extra={'hid': self.handler_id})
        self.write(response)



