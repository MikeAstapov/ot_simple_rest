import json
import logging
import tornado.web
from tornado.ioloop import IOLoop

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.2.0"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


class PingPong(tornado.web.RequestHandler):

    response = json.dumps({'response': 'pong'})
    logger = logging.getLogger('osr')

    async def post(self):
        """
        It writes response to remote side.
        :return:
        """

        self.logger.debug('Ping Post.')
        future = IOLoop.current().run_in_executor(None, self._write)
        await future

    async def get(self):
        """
        It writes response to remote side.

        :return:
        """

        self.logger.debug('Ping Get.')
        future = IOLoop.current().run_in_executor(None, self._write)
        await future

    def _write(self):
        self.write(self.response)

