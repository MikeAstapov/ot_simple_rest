import json
import logging

import tornado.web
from tornado.ioloop import IOLoop

from handlers.jobs.db_connector import PostgresConnector

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = ["Nikolay Ryabykh", "Anton Khromov"]
__license__ = ""
__version__ = "0.2.4"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


class MakeRoleModel(tornado.web.RequestHandler):
    """
    This handler saves Role Model gotten from Splunk's API to OT.Simple Dispatcher's one.
    """

    logger = logging.getLogger('osr')

    def initialize(self, db_conn_pool):
        """
        Gets configs.

        :param db_conn_pool: DB connections pool object.
        :return:
        """
        self.db = PostgresConnector(db_conn_pool)

    async def post(self):
        """
        It writes response to remote side.

        :return:
        """
        future = IOLoop.current().run_in_executor(None, self.make_role_model)
        await future

    def make_role_model(self):
        """
        Clears old role model and saves new one.
        :return:
        """
        request = self.request.body_arguments
        role_model = request["role_model"][0].decode()
        role_model = json.loads(role_model)

        self.db.clear_roles()

        for rm in role_model:
            self.logger.debug(f"RM: {rm}.")
            username = rm['username']
            roles = rm['roles'].split('\n')
            indexes = rm['indexes'].split('\n')
            roles = roles if type(roles) is list else [roles]
            indexes = indexes if type(indexes) is list else [indexes]
            self.db.add_roles(username=username, roles=roles, indexes=indexes)

        response = {"status": "ok"}
        self.write(response)
