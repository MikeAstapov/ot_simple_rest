import json
import tornado.web
import psycopg2

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.1.1"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


class MakeRoleModel(tornado.web.RequestHandler):
    """
    This handler saves Role Model gotten from Splunk's API to OT.Simple Dispatcher's one.
    """

    def initialize(self, db_conf):
        """
        Gets configs.

        :param db_conf: Postgres config.
        :return:
        """
        self.db_conf = db_conf

    def post(self):
        """
        It writes response to remote side.

        :return:
        """
        response = self.make_role_model()
        self.write(response)

    def make_role_model(self):
        """
        Clears old role model and saves new one.
        :return:
        """
        request = self.request.body_arguments
        role_model = request["role_model"][0].decode()
        role_model = json.loads(role_model)

        conn = psycopg2.connect(**self.db_conf)
        cur = conn.cursor()

        clear_role_stm = "DELETE FROM RoleModel"
        cur.execute(clear_role_stm)
        conn.commit()

        for rm in role_model:
            role_stm = "INSERT INTO RoleModel (username, roles, indexes) VALUES (%s, %s, %s)"
            cur.execute(role_stm, (rm['username'], rm['roles'].split('\n'), rm['indexes'].split('\n')))

        conn.commit()

        return {"status": "ok"}