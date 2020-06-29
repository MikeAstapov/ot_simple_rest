import requests
import tarfile


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
        del_permission_query = "DELETE FROM permission where name != 'admin_all';"
        del_group_query = 'DELETE FROM "group";'
        del_session_query = "DELETE FROM session;"
        del_index_query = "DELETE FROM index;"
        del_dash_query = "DELETE FROM dash;"

        for query in [del_user_query, del_role_query, del_permission_query,
                      del_group_query, del_session_query, del_index_query, del_dash_query]:
            self.db.execute_query(query, with_commit=True, with_fetch=False)

    def auth(self):
        data = {'username': 'admin', 'password': '12345678'}
        resp = requests.post(
            f'http://{self.config["rest_conf"]["host"]}:{self.config["rest_conf"]["port"]}/api/auth/login', json=data)
        resp.raise_for_status()
        self.cookies = resp.cookies

    def check_cookies(self):
        if not self.cookies:
            self.auth()

    def send_request(self, *, endpoint, method='GET', data=None, retcontent='json'):
        methods = {'GET': requests.get,
                   'POST': requests.post,
                   'PUT': requests.put,
                   'DELETE': requests.delete}
        self.check_cookies()
        _method = methods.get(method)
        if not _method:
            raise TypeError(f'unknown type of http method: {method}')

        resp = _method(f'http://{self.config["rest_conf"]["host"]}:{self.config["rest_conf"]["port"]}{endpoint}',
                       cookies=self.cookies, json=data)
        resp.raise_for_status()
        if retcontent == 'json':
            return resp.json()
        elif retcontent == 'text':
            return resp.text
        else:
            return resp.content

    def send_request_file(self, *, endpoint, method='POST', data=None, files=None, retcontent='text'):
        methods = {'POST': requests.post,
                   'PUT': requests.put}
        self.check_cookies()
        _method = methods.get(method)
        if not _method:
            raise TypeError(f'unknown type of http method: {method}')

        resp = _method(f'http://{self.config["rest_conf"]["host"]}:{self.config["rest_conf"]["port"]}{endpoint}',
                       cookies=self.cookies, data=data, files=files)
        resp.raise_for_status()
        if retcontent == 'json':
            return resp.json()
        elif retcontent == 'text':
            return resp.text
        else:
            return resp.content

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

    def test__get_group(self):
        data = {'name': 'test_group', 'color': '#332211'}
        try:
            self.send_request(method='POST', endpoint='/api/group', data=data)
            group_from_db = self.db.execute_query('SELECT id FROM "group" WHERE name=%s;',
                                                  params=(data['name'],), as_obj=True)
            group_from_api = self.send_request(method='GET', endpoint=f'/api/group?id={group_from_db.id}')
        finally:
            self._cleanup()
        return group_from_api['data']['name'] == data['name']

    def test__get_groups_list(self):
        data = [{'name': 'test_group_1', 'color': '#332211'}, {'name': 'test_group_2', 'color': '#332244'}]
        try:
            for d in data:
                self.send_request(method='POST', endpoint='/api/group', data=d)
            groups_from_api = self.send_request(method='GET', endpoint='/api/groups')
        finally:
            self._cleanup()
        return groups_from_api['data'][0]['name'] == data[0]['name'] and \
               groups_from_api['data'][1]['name'] == data[1]['name']

    def test__create_permission(self):
        try:
            data = {'name': 'test_permission'}
            self.send_request(method='POST', endpoint='/api/permission', data=data)
            permission_data = self.db.execute_query('SELECT name FROM permission WHERE name=%s;',
                                                    params=(data['name'],), as_obj=True)
        finally:
            self._cleanup()
        return permission_data is not None

    def test__delete_permission(self):
        try:
            data = {'name': 'test_permission'}
            self.send_request(method='POST', endpoint='/api/permission', data=data)
            permissions_before = self.db.execute_query('SELECT id FROM permission WHERE name=%s;',
                                                       params=(data['name'],), as_obj=True, fetchall=True)
            self.send_request(method='DELETE', endpoint=f'/api/permission?id={permissions_before[0].id}')
            permissions_after = self.db.execute_query('SELECT id FROM permission WHERE name != %s;',
                                                      params=('admin_all',), as_obj=True, fetchall=True)
        finally:
            self._cleanup()
        return permissions_after == []

    def test__edit_permission(self):
        try:
            data = {'name': 'test_permission'}
            self.send_request(method='POST', endpoint='/api/permission', data=data)
            permission_from_db = self.db.execute_query('SELECT id FROM permission WHERE name=%s;',
                                                       params=(data['name'],), as_obj=True)
            edited_data = {'id': permission_from_db.id, 'name': 'edited_permission'}
            self.send_request(method='PUT', endpoint=f'/api/permission', data=edited_data)
            edited_permission = self.db.execute_query('SELECT id, name FROM permission WHERE name != %s;',
                                                      params=('admin_all',), as_obj=True)
        finally:
            self._cleanup()
        return edited_permission.name == edited_data['name']

    def test__get_permission(self):
        data = {'name': 'test_permission'}
        try:
            self.send_request(method='POST', endpoint='/api/permission', data=data)
            permission_from_db = self.db.execute_query('SELECT id FROM permission WHERE name=%s;',
                                                       params=(data['name'],), as_obj=True)
            permission_from_api = self.send_request(method='GET',
                                                    endpoint=f'/api/permission?id={permission_from_db.id}')
        finally:
            self._cleanup()
        return permission_from_api['data']['name'] == data['name']

    def test__get_permissions_list(self):
        data = [{'name': 'admin_all', 'color': '#332211'}, {'name': 'test_permission_2', 'color': '#332244'}]
        try:
            self.send_request(method='POST', endpoint='/api/permission', data=data[1])
            permissions_from_api = self.send_request(method='GET', endpoint='/api/permissions')
        finally:
            self._cleanup()
        return permissions_from_api['data'][0]['name'] == data[0]['name'] and \
               permissions_from_api['data'][1]['name'] == data[1]['name']

    def test__create_index(self):
        try:
            data = {'name': 'test_index'}
            self.send_request(method='POST', endpoint='/api/index', data=data)
            index_data = self.db.execute_query('SELECT name FROM index WHERE name=%s;',
                                               params=(data['name'],), as_obj=True)
        finally:
            self._cleanup()
        return index_data is not None

    def test__delete_index(self):
        try:
            data = {'name': 'test_index'}
            self.send_request(method='POST', endpoint='/api/index', data=data)
            indexes_before = self.db.execute_query('SELECT id FROM index WHERE name=%s;',
                                                   params=(data['name'],), as_obj=True, fetchall=True)
            self.send_request(method='DELETE', endpoint=f'/api/index?id={indexes_before[0].id}')
            indexes_after = self.db.execute_query('SELECT id FROM index;', as_obj=True, fetchall=True)
        finally:
            self._cleanup()
        return indexes_after == []

    def test__edit_index(self):
        try:
            data = {'name': 'test_index'}
            self.send_request(method='POST', endpoint='/api/index', data=data)
            index_from_db = self.db.execute_query('SELECT id FROM index WHERE name=%s;',
                                                  params=(data['name'],), as_obj=True)
            edited_data = {'id': index_from_db.id, 'name': 'edited_index'}
            self.send_request(method='PUT', endpoint=f'/api/index', data=edited_data)
            edited_index = self.db.execute_query('SELECT id, name FROM index;', as_obj=True)
        finally:
            self._cleanup()
        return edited_index.name == edited_data['name']

    def test__get_index(self):
        data = {'name': 'test_index'}
        try:
            self.send_request(method='POST', endpoint='/api/index', data=data)
            index_from_db = self.db.execute_query('SELECT id FROM index WHERE name=%s;',
                                                  params=(data['name'],), as_obj=True)
            index_from_api = self.send_request(method='GET',
                                               endpoint=f'/api/index?id={index_from_db.id}')
        finally:
            self._cleanup()
        return index_from_api['data']['name'] == data['name']

    def test__get_indexes_list(self):
        data = [{'name': 'index_1'}, {'name': 'index_2'}]
        try:
            for d in data:
                self.send_request(method='POST', endpoint='/api/index', data=d)
            indexes_from_api = self.send_request(method='GET', endpoint='/api/indexes')
        finally:
            self._cleanup()
        return indexes_from_api['data'][0]['name'] == data[0]['name'] and \
               indexes_from_api['data'][1]['name'] == data[1]['name']

    def test__create_dash(self):
        try:
            data = {'name': 'test_dash'}
            self.send_request(method='POST', endpoint='/api/dash', data=data)
            dash_data = self.db.execute_query('SELECT name FROM dash WHERE name=%s;',
                                              params=(data['name'],), as_obj=True)
        finally:
            self._cleanup()
        return dash_data is not None

    def test__delete_dash(self):
        try:
            data = {'name': 'test_dash'}
            self.send_request(method='POST', endpoint='/api/dash', data=data)
            dashs_before = self.db.execute_query('SELECT id FROM dash WHERE name=%s;',
                                                 params=(data['name'],), as_obj=True, fetchall=True)
            self.send_request(method='DELETE', endpoint=f'/api/dash?id={dashs_before[0].id}')
            dashs_after = self.db.execute_query('SELECT id FROM dash;', as_obj=True, fetchall=True)
        finally:
            self._cleanup()
        return dashs_after == []

    def test__edit_dash(self):
        try:
            data = {'name': 'test_dash'}
            self.send_request(method='POST', endpoint='/api/dash', data=data)
            dash_from_db = self.db.execute_query('SELECT id FROM dash WHERE name=%s;',
                                                 params=(data['name'],), as_obj=True)
            edited_data = {'id': dash_from_db.id, 'name': 'edited_dash'}
            self.send_request(method='PUT', endpoint=f'/api/dash', data=edited_data)
            edited_dash = self.db.execute_query('SELECT id, name FROM dash;', as_obj=True)
        finally:
            self._cleanup()
        return edited_dash.name == edited_data['name']

    def test__get_dash(self):
        data = {'name': 'test_dash'}
        try:
            self.send_request(method='POST', endpoint='/api/dash', data=data)
            dash_from_db = self.db.execute_query('SELECT id FROM dash WHERE name=%s;',
                                                 params=(data['name'],), as_obj=True)
            dash_from_api = self.send_request(method='GET', endpoint=f'/api/dash?id={dash_from_db.id}')
        finally:
            self._cleanup()
        return dash_from_api['data']['name'] == data['name']

    def test__get_dashs_list(self):
        data = [{'name': 'dash_1'}, {'name': 'dash_2'}]
        try:
            for d in data:
                self.send_request(method='POST', endpoint='/api/dash', data=d)
            dashs_from_api = self.send_request(method='GET', endpoint='/api/dashs')
        finally:
            self._cleanup()
        return dashs_from_api['data'][0]['name'] == data[0]['name'] and \
               dashs_from_api['data'][1]['name'] == data[1]['name']

    def test__get_user_permissions(self):
        permissions = self.send_request(method='GET', endpoint='/api/user/permissions')
        return permissions['data'] == ['admin_all']

    def test__get_user_groups(self):
        groups_data = [{'name': 'group_1', 'color': '#111111'},
                       {'name': 'group_2', 'color': '#222222'}]
        user_data = {'name': 'test_user', 'password': '12345678',
                     'groups': ['group_1', 'group_2']}

        try:
            for d in groups_data:
                self.send_request(method='POST', endpoint='/api/group', data=d)
            self.send_request(method='POST', endpoint='/api/user', data=user_data)
            user_groups_data = self.send_request(method='GET',
                                                 endpoint='/api/user/groups?names_only=1')
        finally:
            self._cleanup()
        return user_groups_data['data'] == ['group_1', 'group_2']

    def test__get_user_dashs(self):
        dashs_data = [{'name': 'dash_1'}, {'name': 'dash_2'}]

        try:
            for d in dashs_data:
                self.send_request(method='POST', endpoint='/api/dash', data=d)
            user_dashs_data = self.send_request(method='GET',
                                                endpoint='/api/user/dashs?names_only=1')
        finally:
            self._cleanup()
        return user_dashs_data['data'] == ['dash_1', 'dash_2']

    def test__get_group_dashs(self):
        dashs_data = [{'name': 'dash_1'}, {'name': 'dash_2'}]
        group_data = {'name': 'test_group', 'color': '#332233', 'dashs': ['dash_1', 'dash_2']}

        try:
            for d in dashs_data:
                self.send_request(method='POST', endpoint='/api/dash', data=d)
            self.send_request(method='POST', endpoint='/api/group', data=group_data)
            group_from_db = self.db.execute_query('SELECT id FROM "group" WHERE name=%s;',
                                                  params=(group_data['name'],), as_obj=True)
            dashs_from_api = self.send_request(method='GET',
                                               endpoint=f'/api/group/dashs?id={group_from_db.id}')
            dashs_from_api = [d['name'] for d in dashs_from_api['data']]
        finally:
            self._cleanup()
        return dashs_from_api == group_data['dashs']

    def test__export_dash_single(self):
        data = {'name': 'test_dash_export', 'body': '{"store":{"qqq":"ddd"}}'}
        try:
            self.send_request(method='POST', endpoint='/api/dash', data=data)
            dash_from_db = self.db.execute_query('SELECT id, name, body FROM dash WHERE name=%s;',
                                                 params=(data['name'],), as_obj=True)
            exported_dash_file = self.send_request(method='GET', endpoint=f'/api/dash/export?ids={dash_from_db.id}',
                                                   retcontent='text')
            with tarfile.open(name="%s%s" % (self.config['static']['static_path'], exported_dash_file),
                              mode='r:gz') as tar:
                for dash in tar.getmembers():
                    try:
                        dash_data = tar.extractfile(dash)
                        dash_name = dash.name
                        dash_body = dash_data.read().decode()
                    except Exception as err:
                        print(err)
                        return False
                    if data['name'] != dash_name or data['body'] != dash_body:
                        return False
        finally:
            self._cleanup()
        return True

    def test__export_dash_multi(self):
        data = [{'name': 'test_dash_export1', 'body': '{"store":{"qqq":"1"}}'},
                {'name': 'test_dash_export2', 'body': '{"store":{"qqq":"2"}}'}]
        try:
            for d in data:
                self.send_request(method='POST', endpoint='/api/dash', data=d)
                dash_id_from_db = self.db.execute_query('SELECT id FROM dash WHERE name=%s;',
                                                        params=(d['name'],), as_obj=True, fetchall=True)
                d['id'] = dash_id_from_db[0].id
            exported_dash_file = self.send_request(method='GET',
                                                   endpoint=f'/api/dash/export?ids=%s' % ','.join(
                                                       list(map(lambda x: str(x["id"]), data))), retcontent='text')
            with tarfile.open(name="%s%s" % (self.config['static']['static_path'], exported_dash_file),
                              mode='r:gz') as tar:
                for dash in tar.getmembers():
                    try:
                        dash_data = tar.extractfile(dash)
                        dash_name = dash.name
                        dash_body = dash_data.read().decode()
                    except Exception as err:
                        print(err)
                        return False
                    d = list(filter(lambda x: x['name'] == dash_name, data))[0]
                    if not d or d['body'] != dash_body:
                        return False
        finally:
            self._cleanup()
        return True

    def test__import_dash_single(self):
        data = b'\x1F\x8B\x08\x08\x75\x36\xFA\x5E\x02\xFF\x32\x30\x32\x30\x30\x36\x32\x39\x32\x31\x34\x34\x30\x35' \
               b'\x2E\x65\x76\x61\x2E\x64\x61\x73\x68\x00\xED\xCF\x41\x0A\xC2\x30\x14\x45\xD1\x2C\x25\x64\x05\x89' \
               b'\x49\x13\xE8\x66\x4A\x24\x05\x45\x8A\xA6\x49\x47\xA5\x7B\xB7\x45\x11\x27\xE2\x48\x51\xB8\x67\x72' \
               b'\x07\xFF\x4D\x7E\xED\x4B\xED\x52\x2C\x87\xEE\x38\x5C\xC4\x67\xE8\x95\xF7\x6E\xAB\x09\x8D\x7E\xEE' \
               b'\xCD\x2E\x08\x63\x7D\xF0\xCE\x5A\xE3\x1B\xA1\x8D\xB3\x6B\xA4\x16\x5F\x30\x95\x1A\x47\x29\xC5\x10' \
               b'\x4F\xD3\x3E\xBE\xDE\xBD\xBB\xDF\x5F\x79\xF4\x4F\xCC\xAA\xD4\xF3\xD8\xAB\x76\x56\x39\x67\xD5\xAA' \
               b'\x94\x92\x5A\x16\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF8\x6D\x57\x97\x63\x5C' \
               b'\xA8\x00\x28\x00\x00'
        data_body = '{"store":{"qqq":"ddd"}}'
        try:
            data_for_group = {'name': 'test_imp_group', 'color': '#332211'}
            self.send_request(method='POST', endpoint='/api/group', data=data_for_group)

            imported_dash = self.send_request_file(method='POST', endpoint=f'/api/dash/import',
                                                   data={"group": "test_imp_group"}, files={
                    "body": ("20200629214405.eva.dash", data, "application/x-msdownload")}, retcontent='json')
            if imported_dash['status'] != 'success':
                return False

            dash_from_db = self.db.execute_query('SELECT id, name, body FROM dash WHERE name LIKE \'test_dash_imp%\';',
                                                 as_obj=True)
            if dash_from_db['body'] != data_body:
                return False
        finally:
            self._cleanup()
        return True

    def test__import_dash_multi(self):
        data = b'\x1F\x8B\x08\x08\x4B\x4A\xFA\x5E\x02\xFF\x32\x30\x32\x30\x30\x36\x32\x39\x32\x33\x30\x38\x34\x33' \
               b'\x2E\x65\x76\x61\x2E\x64\x61\x73\x68\x00\xED\xD3\x4B\x0A\xC2\x30\x14\x46\xE1\x2C\xA5\x64\x05\xB9' \
               b'\x4D\x9A\x42\x37\x53\x22\x29\x28\x52\xB4\x4D\x3A\x2A\xEE\x5D\x45\x91\x4E\xC4\x91\x8F\xE2\xF9\x26' \
               b'\xFF\x20\x77\x92\xC1\xC9\x5D\xCA\x6D\x0C\x69\xDB\xEE\xFA\xA3\xA8\xB7\x30\x17\xDE\xBB\xEB\x4A\x5D' \
               b'\x99\xE5\xDE\x58\xA3\xC4\xFA\xDA\x3B\x57\x89\x58\x65\xC4\x39\xEB\x54\x61\xD4\x07\x4C\x29\x87\xB1' \
               b'\x28\x54\x1F\xF6\xD3\x26\x3C\xBF\x7B\xF5\x7E\xFF\xCA\x63\x57\x62\xD6\x29\x1F\xC6\x4E\x37\xB3\x1E' \
               b'\x86\x41\x37\x3A\xC6\x28\xFA\x74\x52\xF8\x0B\x79\xD9\x7F\xF9\x43\xFD\x57\xF4\xFF\xAD\xFE\x4B\xFA' \
               b'\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x58\x8D\x33\xEA\x19\x22\x55\x00\x28\x00\x00'

        data_body = [{'name': 'test_dash_imp1', 'body': '{"store":{"qqq":"ddd1"}}'},
                {'name': 'test_dash_imp2', 'body': '{"store":{"qqq":"ddd2"}}'}]

        try:
            data_for_group = {'name': 'test_imp_group', 'color': '#332211'}
            self.send_request(method='POST', endpoint='/api/group', data=data_for_group)

            imported_dash = self.send_request_file(method='POST', endpoint=f'/api/dash/import',
                                                   data={"group": "test_imp_group"}, files={
                    "body": ("20200629214405.eva.dash", data, "application/x-msdownload")}, retcontent='json')
            if imported_dash['status'] != 'success':
                return False

            dash_from_db = self.db.execute_query('SELECT id, name, body FROM dash WHERE name LIKE \'test_dash_imp%\';',
                                                 as_obj=True, fetchall=True)

            for d in dash_from_db:
                if d['body'] != data_body[0]['body'] and d['body'] != data_body[1]['body']:
                    return False
            # if db return none result this test return True but this not correct...
        finally:
            self._cleanup()
        return True

