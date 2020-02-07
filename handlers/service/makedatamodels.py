import json
import logging

import tornado.web
import psycopg2
from tornado.ioloop import IOLoop

from handlers.jobs.db_connector import PostgresConnector

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = ["Nikolay Ryabykh", "Anton Khromov"]
__license__ = ""
__version__ = "0.0.2"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


class MakeDataModels(tornado.web.RequestHandler):
    """
    This handler saves Role Model gotten from Splunk's API to OT.Simple Dispatcher's one.
    """

    logger = logging.getLogger('osr')

    def initialize(self, db_conf):
        """
        Gets configs.

        :param db_conf: Postgres config.
        :return:
        """
        self.db = PostgresConnector(db_conf)

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
