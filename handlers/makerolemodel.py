import json
import tornado.web
import psycopg2


class MakeRoleModel(tornado.web.RequestHandler):

    def initialize(self, db_conf):
        self.db_conf = db_conf

    def post(self):
        response = self.make_role_model()
        self.write(response)

    def make_role_model(self):
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