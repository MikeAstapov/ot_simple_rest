import json

import jwt
import tornado.web

from handlers.eva.db_connector import PostgresConnector

SECRET_KEY = '8b62abb2-bbf6-4e0e-a7c1-2e4734bebbd9'

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Anton Khromov"
__email__ = "akhromov@ot.ru"
__status__ = "Production"


class BaseHandler(tornado.web.RequestHandler):

    def initialize(self, db_conn_pool):
        """
        Gets config and init logger.

        :param db_conn_pool: Postgres DB connection pool object.
        :return:
        """
        self.db = PostgresConnector(db_conn_pool)
        self.permissions = None
        self.data = None
        self.token = None

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Credentials', True)
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', '*')

    def options(self, *args, **kwargs):
        self.set_status(204)
        self.finish()

    def decode_token(self, token):
        return jwt.decode(token, SECRET_KEY, algorithms='HS256')

    def generate_token(self, payload):
        return jwt.encode(payload, SECRET_KEY, algorithm='HS256')

    async def prepare(self):
        self.data = json.loads(self.request.body) if self.request.body else dict()
        client_token = self.get_cookie('eva_token')
        if client_token:
            self.token = client_token
            try:
                token_data = self.decode_token(client_token)
                user_id = token_data['user_id']
                self.permissions = self.db.get_permissions_data(user_id=user_id,
                                                                names_only=True)
            except (jwt.ExpiredSignatureError, jwt.DecodeError):
                pass
            else:
                self.current_user = user_id

        if not self.current_user:
            raise tornado.web.HTTPError(401, "unauthorized")
