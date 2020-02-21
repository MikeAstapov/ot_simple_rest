import json

import requests


class MakeRoleModelTester:
    """
    Test suite for /makerolemodel OT_REST endpoint.
    Test cases:
    - create role model
    """

    def __init__(self, db, conf):
        self.db = db
        self.conf = conf

    def create_model(self):
        role_model = [{"username": "rest_test", "roles": "rest_test_role", "indexes": "index1\nindex2\nindex3"},
                      {"username": "tester", "roles": "tester", "indexes": "pprbappcore_business"}]
        resp = requests.post(f'http://{self.conf["host"]}:{self.conf["port"]}/makerolemodel',
                             data={"role_model": json.dumps(role_model)}).json()
        if resp['status'] != 'ok':
            raise ValueError('Role model not created')

        self.db.cur.execute("""SELECT * FROM rolemodel where username='rest_test';""")
        created_model = self.db.cur.fetchone()

        self.db.cur.execute("""DELETE FROM rolemodel WHERE username='rest_test';""")
        self.db.conn.commit()
        return created_model == ('rest_test', ['rest_test_role'], ['index1', 'index2', 'index3'])
