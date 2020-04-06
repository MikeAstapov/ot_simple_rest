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
        resp = requests.post(f'http://{self.conf["host"]}:{self.conf["port"]}/api/makerolemodel',
                             data={"role_model": json.dumps(role_model)}).json()
        if resp['status'] != 'ok':
            raise ValueError('Role model not created')

        created_model = self.db.execute_query("""SELECT * FROM rolemodel where username='rest_test';""")

        self.db.execute_query("""DELETE FROM rolemodel WHERE username='rest_test';""", with_commit=True, with_fetch=False)
        return created_model == ('rest_test', ['rest_test_role'], ['index1', 'index2', 'index3'])
