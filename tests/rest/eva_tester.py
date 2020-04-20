import requests


class EvaTester:
    """
    Test suite for EVA OT_REST endpoints.
    Test cases:
    - authorisation
    - create/get/delete/edit user
    - get users list
    - create/get/delete/edit role
    - get roles list
    """

    def __init__(self, api_conf, db):
        self.config = api_conf
        self.db = db
        self.cookies = None

    def _cleanup(self):
        del_user_query = """DELETE FROM "user" WHERE name != 'admin';"""
        del_role_query = "DELETE FROM role where name != 'admin';"
        del_group_query = 'DELETE FROM "group";'
        del_session_query = "DELETE FROM session;"
        for query in [del_user_query, del_role_query, del_group_query, del_session_query]:
            self.db.execute_query(query, with_commit=True, with_fetch=False)

    def auth(self):
        data = {'username': 'admin', 'password': '12345678'}
        resp = requests.post(f'http://{self.config["host"]}:{self.config["port"]}/api/auth/login', json=data)
        resp.raise_for_status()
        self.cookies = resp.cookies

    def check_cookies(self):
        if not self.cookies:
            self.auth()

    def send_request(self, *, endpoint, method='GET', data=None):
        methods = {'GET': requests.get,
                   'POST': requests.post,
                   'PUT': requests.put,
                   'DELETE': requests.delete}
        self.check_cookies()
        _method = methods.get(method)
        if not _method:
            raise TypeError(f'unknown type of http method: {method}')

        resp = _method(f'http://{self.config["host"]}:{self.config["port"]}{endpoint}',
                       cookies=self.cookies, json=data)
        resp.raise_for_status()
        return resp.json()

    def test__auth(self):
        self.check_cookies()
        return 'eva_token' in self.cookies

    def test__create_user(self):
        try:
            data = {'name': 'test_user', 'password': '12345678'}
            self.send_request(method='POST', endpoint='/api/user', data=data)
            user_data = self.db.execute_query('SELECT name FROM "user" WHERE name=%s;',
                                              params=(data['name'],), as_obj=True)
        finally:
            self._cleanup()
        return user_data is not None

    def test__delete_user(self):
        try:
            data = {'name': 'test_user', 'password': '12345678'}
            self.send_request(method='POST', endpoint='/api/user', data=data)
            users_before = self.db.execute_query('SELECT id FROM "user" WHERE name=%s;',
                                                 params=(data['name'],), as_obj=True, fetchall=True)
            self.send_request(method='DELETE', endpoint=f'/api/user?id={users_before[0].id}')
            users_after = self.db.execute_query("""SELECT id FROM "user" where name != 'admin';""",
                                                as_obj=True, fetchall=True)
        finally:
            self._cleanup()
        return users_after == []

    def test__edit_user(self):
        try:
            data = {'name': 'test_user', 'password': '12345678'}
            self.send_request(method='POST', endpoint='/api/user', data=data)
            user_from_db = self.db.execute_query('SELECT id FROM "user" WHERE name=%s;',
                                                 params=(data['name'],), as_obj=True)
            edited_data = {'id': user_from_db.id, 'name': 'edited_user'}
            self.send_request(method='PUT', endpoint=f'/api/user', data=edited_data)
            edited_user = self.db.execute_query("""SELECT id, name FROM "user" WHERE name != 'admin';""",
                                                as_obj=True)
        finally:
            self._cleanup()
        return edited_user.name == edited_data['name']

    def test__get_user(self):
        data = {'name': 'test_user', 'password': '12345678'}
        try:
            self.send_request(method='POST', endpoint='/api/user', data=data)
            user_from_db = self.db.execute_query('SELECT id FROM "user" WHERE name=%s;',
                                                 params=(data['name'],), as_obj=True)
            user_from_api = self.send_request(method='GET', endpoint=f'/api/user?id={user_from_db.id}')
        finally:
            self._cleanup()
        return user_from_api['data']['name'] == data['name']

    def test__get_users_list(self):
        data = [{'name': 'admin', 'password': '12345678'}, {'name': 'user_2', 'password': 'password2'}]
        try:
            self.send_request(method='POST', endpoint='/api/user', data=data[1])
            users_from_api = self.send_request(method='GET', endpoint=f'/api/users')
        finally:
            self._cleanup()
        return users_from_api['data'][0]['name'] == data[0]['name'] and \
               users_from_api['data'][1]['name'] == data[1]['name']

    def test__create_role(self):
        try:
            data = {'name': 'test_role'}
            self.send_request(method='POST', endpoint='/api/role', data=data)
            user_data = self.db.execute_query('SELECT name FROM role WHERE name=%s;',
                                              params=(data['name'],), as_obj=True)
        finally:
            self._cleanup()
        return user_data is not None

    def test__delete_role(self):
        try:
            data = {'name': 'test_role'}
            self.send_request(method='POST', endpoint='/api/role', data=data)
            roles_before = self.db.execute_query('SELECT id FROM role WHERE name=%s;',
                                                 params=(data['name'],), as_obj=True, fetchall=True)
            self.send_request(method='DELETE', endpoint=f'/api/role?id={roles_before[0].id}')
            roles_after = self.db.execute_query("SELECT id FROM role where name != 'admin';",
                                                as_obj=True, fetchall=True)
        finally:
            self._cleanup()
        return roles_after == []

    def test__edit_role(self):
        try:
            data = {'name': 'test_role'}
            self.send_request(method='POST', endpoint='/api/role', data=data)
            role_from_db = self.db.execute_query('SELECT id FROM role WHERE name=%s;',
                                                 params=(data['name'],), as_obj=True)
            edited_data = {'id': role_from_db.id, 'name': 'edited_role'}
            self.send_request(method='PUT', endpoint=f'/api/role', data=edited_data)
            edited_role = self.db.execute_query("SELECT id, name FROM role WHERE name != 'admin';",
                                                as_obj=True)
        finally:
            self._cleanup()
        return edited_role.name == edited_data['name']

    def test__get_role(self):
        data = {'name': 'test_role'}
        try:
            self.send_request(method='POST', endpoint='/api/role', data=data)
            role_from_db = self.db.execute_query('SELECT id FROM role WHERE name=%s;',
                                                 params=(data['name'],), as_obj=True)
            role_from_api = self.send_request(method='GET', endpoint=f'/api/role?id={role_from_db.id}')
        finally:
            self._cleanup()
        return role_from_api['data']['name'] == data['name']

    def test__get_roles_list(self):
        data = [{'name': 'admin'}, {'name': 'role_2'}]
        try:
            self.send_request(method='POST', endpoint='/api/role', data=data[1])
            roles_from_api = self.send_request(method='GET', endpoint='/api/roles')
        finally:
            self._cleanup()
        return roles_from_api['data'][0]['name'] == data[0]['name'] and \
               roles_from_api['data'][1]['name'] == data[1]['name']

    def test__create_group(self):
        try:
            data = {'name': 'test_group', 'color': '#332211'}
            self.send_request(method='POST', endpoint='/api/group', data=data)
            group_data = self.db.execute_query('SELECT name FROM "group" WHERE name=%s;',
                                               params=(data['name'],), as_obj=True)
        finally:
            self._cleanup()
        return group_data is not None

    def test__delete_group(self):
        try:
            data = {'name': 'test_group', 'color': '#332211'}
            self.send_request(method='POST', endpoint='/api/group', data=data)
            groups_before = self.db.execute_query('SELECT id FROM "group" WHERE name=%s;',
                                                  params=(data['name'],), as_obj=True, fetchall=True)
            self.send_request(method='DELETE', endpoint=f'/api/group?id={groups_before[0].id}')
            groups_after = self.db.execute_query('SELECT id FROM "group";', as_obj=True, fetchall=True)
        finally:
            self._cleanup()
        return groups_after == []

    def test__edit_group(self):
        try:
            data = {'name': 'test_group', 'color': '#332211'}
            self.send_request(method='POST', endpoint='/api/group', data=data)
            group_from_db = self.db.execute_query('SELECT id FROM "group" WHERE name=%s;',
                                                  params=(data['name'],), as_obj=True)
            edited_data = {'id': group_from_db.id, 'name': 'edited_group'}
            self.send_request(method='PUT', endpoint=f'/api/group', data=edited_data)
            edited_group = self.db.execute_query('SELECT id, name FROM "group";', as_obj=True)
        finally:
            self._cleanup()
        return edited_group.name == edited_data['name']

    def test__get_role(self):
        data = {'name': 'test_role'}
        try:
            self.send_request(method='POST', endpoint='/api/role', data=data)
            role_from_db = self.db.execute_query('SELECT id FROM role WHERE name=%s;',
                                                 params=(data['name'],), as_obj=True)
            role_from_api = self.send_request(method='GET', endpoint=f'/api/role?id={role_from_db.id}')
        finally:
            self._cleanup()
        return role_from_api['data']['name'] == data['name']

    def test__get_roles_list(self):
        data = [{'name': 'admin'}, {'name': 'role_2'}]
        try:
            self.send_request(method='POST', endpoint='/api/role', data=data[1])
            roles_from_api = self.send_request(method='GET', endpoint='/api/roles')
        finally:
            self._cleanup()
        return roles_from_api['data'][0]['name'] == data[0]['name'] and \
               roles_from_api['data'][1]['name'] == data[1]['name']
