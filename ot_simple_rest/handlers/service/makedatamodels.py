import json
import logging

import tornado.web
from tornado.ioloop import IOLoop

from handlers.jobs.db_connector import PostgresConnector

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = ["Nikolay Ryabykh", "Anton Khromov"]
__license__ = ""
__version__ = "0.0.3"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Production"


class MakeDataModels(tornado.web.RequestHandler):
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
        Starts async updating DataModel in Dispatcher DB.

        :return:
        """
        future = IOLoop.current().run_in_executor(None, self.make_data_models)
        await future

    def make_data_models(self):
        """
        Clears old DataModel and saves new one.

        :return:
        """
        request = self.request.body_arguments
        data_models = request["data_models"][0].decode()
        data_models = json.loads(data_models)

        self.db.clear_data_models()

        for name in data_models:
            self.db.add_data_model(name=name, search=data_models[name])

        response = {"status": "ok"}
        self.write(response)
