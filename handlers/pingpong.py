import json
import logging
import tornado.web

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


class PingPong(tornado.web.RequestHandler):

    response = json.dumps({'response': 'pong'})
    logger = logging.getLogger('osr')

    def post(self):
        """
        It writes response to remote side.
        :return:
        """

        self.logger.debug('Ping Post.')
        self.write(self.response)

    def get(self):
        """
        It writes response to remote side.

        :return:
        """

        self.logger.debug('Ping Get.')
        self.write(self.response)
