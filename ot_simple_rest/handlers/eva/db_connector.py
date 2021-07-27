from tools.pg_connector import PGConnector
from copy import deepcopy

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.1.2"
__maintainer__ = "Andrey Starchenkov"
__email__ = "akhromov@ot.ru"
__status__ = "Production"


def flat_to_set(arr):
    return {i[0] for i in arr} if arr else set()


def flat_to_list(arr):
    return [i[0] for i in arr] if arr else list()


class PostgresConnector(PGConnector):
    def __init(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # __AUTH__ ######################################################################

    def get_user_tokens(self, user_id):
        tokens = self.execute_query("SELECT token FROM session WHERE user_id = %s;",
                                    params=(user_id,), fetchall=True)
        tokens = flat_to_set(tokens)
        return tokens

    def get_auth_data(self, user_id):
        login_pass = self.execute_query('SELECT name, password FROM "user" WHERE id = %s;',
                                        params=(user_id,), as_obj=True)
        return login_pass

    def add_session(self, *, user_id, token, expired_date):
        self.execute_query("INSERT INTO session (token, user_id, expired_date) VALUES (%s, %s, %s);",
                           params=(token, user_id, expired_date), with_commit=True, with_fetch=False)

    # __USERS__ #######################################################################

    def check_user_exists(self, name):
        user = self.execute_query('SELECT * FROM "user" WHERE name = %s;',
                                  params=(name,), as_obj=True)
        return user

    def get_users_data(self, *, user_id=None, names_only=False):
        if user_id:
            users = self.execute_query('SELECT id, name FROM "user" WHERE id = %s;',
                                       params=(user_id,), fetchall=True, as_obj=True)
        else:
            users = self.execute_query('SELECT id, name FROM "user";', fetchall=True, as_obj=True)
        if names_only:
            users = [u['name'] for u in users]
        else:
            for user in users:
                user_roles = self.execute_query("SELECT name FROM role WHERE id IN "
                                                "(SELECT role_id FROM user_role WHERE user_id = %s);",
                                                params=(user.id,), fetchall=True)
                user_roles = flat_to_list(user_roles)
                user['roles'] = user_roles

                user_groups = self.execute_query('SELECT name FROM "group" WHERE id IN '
                                                 '(SELECT group_id FROM user_group WHERE user_id = %s);',
                                                 params=(user.id,), fetchall=True)
                user_groups = flat_to_list(user_groups)
                user['groups'] = user_groups
        return users

    def get_user_data(self, *, user_id):
        user_data = self.execute_query('SELECT id, name FROM "user" WHERE id = %s;',
                                       params=(user_id,), as_obj=True)
        user_roles = self.execute_query("SELECT name FROM role WHERE id IN "
                                        "(SELECT role_id FROM user_role WHERE user_id = %s);",
                                        params=(user_id,), fetchall=True)
        user_roles = flat_to_list(user_roles)
        user_data['roles'] = user_roles

        user_groups = self.execute_query('SELECT name FROM "group" WHERE id IN '
                                         '(SELECT group_id FROM user_group WHERE user_id = %s);',
                                         params=(user_data.id,), fetchall=True)
        user_groups = flat_to_list(user_groups)
        user_data['groups'] = user_groups

        return user_data

    def add_user(self, *, name, password, roles, groups):
        if self.check_user_exists(name):
            raise QueryError(f'user {name} already exists')

        with self.transaction('add_user_data') as conn:
            user_id = self.execute_query('INSERT INTO "user" (name, password) VALUES (%s, %s) RETURNING id;',
                                         conn=conn, params=(name, password))
            if roles:
                for role in roles:
                    self.execute_query("INSERT INTO user_role (role_id, user_id) "
                                       "VALUES ((SELECT id FROM role WHERE name = %s), %s);",
                                       conn=conn, params=(role, user_id), with_fetch=False)
            if groups:
                for group in groups:
                    self.execute_query('INSERT INTO user_group (group_id, user_id) '
                                       'VALUES ((SELECT id FROM "group" WHERE name = %s), %s);',
                                       conn=conn, params=(group, user_id), with_fetch=False)
        return user_id

    def update_user(self, *, user_id, name, password, roles=None, groups=None):
        if name:
            self.execute_query('UPDATE "user" SET name = %s WHERE id = %s;', params=(name, user_id),
                               with_commit=True, with_fetch=False)
        if password:
            self.execute_query('UPDATE "user" SET password = %s WHERE id = %s;', params=(password, user_id),
                               with_commit=True, with_fetch=False)

        if isinstance(roles, list):
            current_roles = self.execute_query("SELECT name FROM role WHERE id IN "
                                               "(SELECT role_id FROM user_role WHERE user_id = %s);",
                                               params=(user_id,), fetchall=True)
            current_roles = flat_to_set(current_roles)
            target_roles = set(roles)

            roles_for_add = target_roles - current_roles
            roles_for_delete = tuple(current_roles - target_roles)

            with self.transaction('update_user_roles') as conn:
                for role in roles_for_add:
                    self.execute_query("INSERT INTO user_role (role_id, user_id) "
                                       "VALUES ((SELECT id FROM role WHERE name = %s), %s);",
                                       conn=conn, params=(role, user_id,), with_fetch=False)
                if roles_for_delete:
                    self.execute_query("DELETE FROM user_role WHERE role_id IN (SELECT id FROM role WHERE name IN %s) "
                                       "AND user_id = %s;",
                                       conn=conn, params=(roles_for_delete, user_id,), with_fetch=False)

        if isinstance(groups, list):
            current_groups = self.execute_query('SELECT name FROM "group" WHERE id IN '
                                                '(SELECT group_id FROM user_group WHERE user_id = %s);',
                                                params=(user_id,), fetchall=True)
            current_groups = flat_to_set(current_groups)
            target_groups = set(groups)

            groups_for_add = target_groups - current_groups
            groups_for_delete = tuple(current_groups - target_groups)

            with self.transaction('update_user_groups') as conn:
                for group in groups_for_add:
                    group_id = self.execute_query('SELECT id FROM "group" WHERE name = %s;',
                                                  conn=conn, params=(group,))
                    self.execute_query("INSERT INTO user_group (user_id, group_id) VALUES (%s, %s);",
                                       conn=conn, params=(user_id, group_id,), with_fetch=False)
                if groups_for_delete:
                    self.execute_query('DELETE FROM user_group WHERE user_id = %s AND group_id IN '
                                       '(SELECT id FROM "group" WHERE name in %s);',
                                       conn=conn, params=(user_id, groups_for_delete,), with_fetch=False)
        return user_id

    def delete_user(self, user_id):
        self.execute_query('DELETE FROM "user" WHERE id = %s;', params=(user_id,),
                           with_commit=True, with_fetch=False)
        return user_id

    def get_user_setting(self, user_id):
        user_data = self.execute_query('SELECT id, setting FROM "user_settings" WHERE id = %s;',
                                       params=(user_id,), as_obj=True)
        if user_data:
            return user_data
        else:
            return {'id': user_id, 'setting': ''}

    def update_user_setting(self, user_id, setting):
        with self.transaction('update_user_setting') as conn:
            self.logger.debug(('DELETE FROM "user_settings" WHERE id = %s;' % user_id))
            self.execute_query('DELETE FROM "user_settings" WHERE id = %s;', conn=conn, params=(user_id,),
                               with_fetch=False, with_commit=True)
            self.logger.debug('INSERT INTO "user_settings" (id, setting) VALUES (%s, %s);' % (user_id, str(setting)))
            self.execute_query('INSERT INTO "user_settings" (id, setting) VALUES (%s, %s);', conn=conn,
                               params=(user_id, str(setting)), with_fetch=False, with_commit=True)
        return user_id

    # __ROLES__ #####################################################################

    def check_role_exists(self, role_name):
        role_id = self.execute_query("SELECT id FROM role WHERE name = %s;", params=(role_name,))
        return role_id

    def get_roles_data(self, user_id=None, names_only=False, with_relations=False):
        if user_id:
            roles = self.execute_query("SELECT * FROM role WHERE id IN "
                                       "(SELECT role_id FROM user_role WHERE user_id = %s);",
                                       fetchall=True, params=(user_id,), as_obj=True)
        else:
            roles = self.execute_query("SELECT * FROM role;", fetchall=True, as_obj=True)
        if names_only:
            roles = [r['name'] for r in roles]
        else:
            for role in roles:
                permissions = self.execute_query("SELECT name FROM permission WHERE id IN "
                                                 "(SELECT permission_id FROM role_permission WHERE role_id = %s);",
                                                 params=(role.id,), fetchall=True)
                permissions = flat_to_list(permissions)
                role['permissions'] = permissions
                users = self.execute_query('SELECT name FROM "user" WHERE id in '
                                           '(SELECT user_id FROM user_role WHERE role_id = %s)',
                                           params=(role.id,), fetchall=True)
                users = flat_to_list(users)
                role['users'] = users

            if with_relations:
                all_permissions = self.get_permissions_data(names_only=True)
                all_users = self.get_users_data(names_only=True)
                roles = {'roles': roles, 'permissions': all_permissions, 'users': all_users}
        return roles

    def get_role_data(self, role_id):
        role = self.execute_query("SELECT * FROM role WHERE id = %s;",
                                  params=(role_id,), as_obj=True)
        permissions = self.execute_query("SELECT name FROM permission WHERE id IN "
                                         "(SELECT permission_id FROM role_permission WHERE role_id = %s);",
                                         params=(role.id,), fetchall=True)
        permissions = flat_to_list(permissions)
        role['permissions'] = permissions
        users = self.execute_query('SELECT name FROM "user" WHERE id in '
                                   '(SELECT user_id FROM user_role WHERE role_id = %s)',
                                   params=(role.id,), fetchall=True)
        users = flat_to_list(users)
        role['users'] = users
        return role

    def add_role(self, *, name, users, permissions):
        if self.check_role_exists(name):
            raise QueryError(f'role {name} already exists')

        with self.transaction('add_role_data') as conn:
            role_id = self.execute_query("INSERT INTO role (name) VALUES (%s) RETURNING id;",
                                         conn=conn, params=(name,))
            if users:
                for user in users:
                    self.execute_query('INSERT INTO user_role (user_id, role_id) '
                                       'VALUES ((SELECT id FROM "user" WHERE name = %s), %s);',
                                       conn=conn, params=(user, role_id,), with_fetch=False)
            if permissions:
                for permission in permissions:
                    self.execute_query("INSERT INTO role_permission (permission_id, role_id) "
                                       "VALUES ((SELECT id FROM permission WHERE name = %s), %s);",
                                       conn=conn, params=(permission, role_id,), with_fetch=False)
        return role_id

    def update_role(self, *, role_id, name, users=None, permissions=None):
        if name:
            self.execute_query("UPDATE role SET name = %s WHERE id = %s;", params=(name, role_id),
                               with_commit=True, with_fetch=False)
        if isinstance(users, list):
            current_users = self.execute_query('SELECT name FROM "user" WHERE id IN '
                                               '(SELECT user_id FROM user_role WHERE role_id = %s);',
                                               params=(role_id,), fetchall=True)
            current_users = flat_to_set(current_users)
            target_users = set(users)

            users_for_add = target_users - current_users
            users_for_delete = tuple(current_users - target_users)

            with self.transaction('update_role_users') as conn:

                for user in users_for_add:
                    self.execute_query('INSERT INTO user_role (user_id, role_id) '
                                       'VALUES ((SELECT id FROM "user" WHERE name = %s), %s);',
                                       conn=conn, params=(user, role_id,), with_fetch=False)
                if users_for_delete:
                    self.execute_query('DELETE FROM user_role WHERE user_id IN '
                                       '(SELECT id FROM "user" WHERE name IN %s) AND role_id = %s;',
                                       conn=conn, params=(users_for_delete, role_id), with_fetch=False)

        if isinstance(permissions, list):
            current_permissions = self.execute_query("SELECT name FROM permission WHERE id IN "
                                                     "(SELECT permission_id FROM role_permission WHERE role_id = %s);",
                                                     params=(role_id,), fetchall=True)
            current_permissions = flat_to_set(current_permissions)
            target_permissions = set(permissions)

            permissions_for_add = target_permissions - current_permissions
            permissions_for_delete = tuple(current_permissions - target_permissions)

            with self.transaction('update_role_permissions') as conn:

                for permission in permissions_for_add:
                    self.execute_query("INSERT INTO role_permission (permission_id, role_id) "
                                       "VALUES ((SELECT id FROM permission WHERE name = %s), %s);",
                                       conn=conn, params=(permission, role_id,), with_fetch=False)
                if permissions_for_delete:
                    self.execute_query("DELETE FROM role_permission WHERE permission_id IN "
                                       "(SELECT id FROM permission WHERE name IN %s) AND role_id = %s;",
                                       conn=conn, params=(permissions_for_delete, role_id), with_fetch=False)
        return role_id

    def get_role(self, role_id):
        role_data = self.execute_query("SELECT * FROM role WHERE id = %s;", params=(role_id,), as_obj=True)
        return role_data

    def delete_role(self, role_id):
        self.execute_query("DELETE FROM role WHERE id = %s;", params=(role_id,),
                           with_commit=True, with_fetch=False)
        return role_id

    # __GROUPS__ ###################################################################

    def check_group_exists(self, group_name):
        group_id = self.execute_query('SELECT id FROM "group" WHERE name = %s;', params=(group_name,))
        return group_id

    def get_groups_data(self, *, user_id=None, names_only=False):
        if user_id:
            groups = self.execute_query('SELECT * FROM "group" WHERE id IN '
                                        '(SELECT group_id FROM user_group WHERE user_id = %s);',
                                        fetchall=True, params=(user_id,), as_obj=True)
        else:
            groups = self.execute_query('SELECT * FROM "group";', fetchall=True, as_obj=True)

        if names_only:
            groups = [g['name'] for g in groups]
        else:
            for group in groups:
                users = self.execute_query('SELECT name FROM "user" WHERE id IN '
                                           '(SELECT user_id FROM user_group WHERE group_id = %s);',
                                           params=(group.id,), fetchall=True)
                users = flat_to_list(users)
                group['users'] = users

                dashboards = self.execute_query("SELECT name FROM dash WHERE id IN "
                                                "(SELECT dash_id FROM dash_group WHERE group_id = %s);",
                                                params=(group.id,), fetchall=True)
                dashboards = flat_to_list(dashboards)
                group['dashs'] = dashboards

                indexes = self.execute_query("SELECT name FROM index WHERE id IN "
                                             "(SELECT index_id FROM index_group WHERE group_id = %s);",
                                             params=(group.id,), fetchall=True)
                indexes = flat_to_list(indexes)
                group['indexes'] = indexes
        return groups

    def get_group_data(self, group_id):
        group = self.execute_query('SELECT * FROM "group" WHERE id = %s;',
                                   params=(group_id,), as_obj=True)
        if not group:
            raise ValueError(f'group with id={group_id} is not exists')

        users = self.execute_query('SELECT name FROM "user" WHERE id IN '
                                   '(SELECT user_id FROM user_group WHERE group_id = %s);',
                                   params=(group.id,), fetchall=True)
        users = flat_to_list(users)
        group['users'] = users

        dashboards = self.execute_query("SELECT name FROM dash WHERE id IN "
                                        "(SELECT dash_id FROM dash_group WHERE group_id = %s);",
                                        params=(group.id,), fetchall=True)
        dashboards = flat_to_list(dashboards)
        group['dashs'] = dashboards

        indexes = self.execute_query("SELECT name FROM index WHERE id IN "
                                     "(SELECT index_id FROM index_group WHERE group_id = %s);",
                                     params=(group.id,), fetchall=True)
        indexes = flat_to_list(indexes)
        group['indexes'] = indexes
        return group

    def add_group(self, *, name, color, users=None, indexes=None, dashs=None):
        if self.check_group_exists(name):
            raise QueryError(f'group {name} already exists')

        with self.transaction('add_group_data') as conn:
            group_id = self.execute_query('INSERT INTO "group" (name, color) VALUES (%s, %s) RETURNING id;',
                                          conn=conn, params=(name, color))
            if users:
                for user in users:
                    self.execute_query('INSERT INTO user_group (user_id, group_id) '
                                       'VALUES ((SELECT id FROM "user" WHERE name = %s), %s);',
                                       conn=conn, params=(user, group_id,), with_fetch=False)
            if indexes:
                for index in indexes:
                    self.execute_query("INSERT INTO index_group (index_id, group_id) "
                                       "VALUES ((SELECT id FROM index WHERE name = %s), %s);",
                                       conn=conn, params=(index, group_id,), with_fetch=False)

            if dashs:
                for dash in dashs:
                    self.execute_query("INSERT INTO dash_group (dash_id, group_id) "
                                       "VALUES ((SELECT id FROM dash WHERE name = %s), %s);",
                                       conn=conn, params=(dash, group_id,), with_fetch=False)
        return group_id

    def update_group(self, *, group_id, name, color, users=None, indexes=None, dashs=None):
        if name:
            self.execute_query('UPDATE "group" SET name = %s WHERE id = %s;',
                               params=(name, group_id), with_commit=True, with_fetch=False)
        if color:
            self.execute_query('UPDATE "group" SET color = %s WHERE id = %s;',
                               params=(color, group_id), with_commit=True, with_fetch=False)

        if isinstance(users, list):
            current_users = self.execute_query('SELECT name FROM "user" WHERE id IN '
                                               '(SELECT user_id FROM user_group WHERE group_id = %s);',
                                               params=(group_id,), fetchall=True)
            current_users = flat_to_set(current_users)
            target_users = set(users)

            users_for_add = target_users - current_users
            users_for_delete = tuple(current_users - target_users)

            with self.transaction('update_group_users') as conn:
                for user in users_for_add:
                    self.execute_query('INSERT INTO user_group (user_id, group_id) '
                                       'VALUES ((SELECT id FROM "user" WHERE name = %s), %s);',
                                       conn=conn, params=(user, group_id,), with_fetch=False)
                if users_for_delete:
                    self.execute_query('DELETE FROM user_group WHERE user_id IN '
                                       '(SELECT id FROM "user" WHERE name IN %s) AND group_id = %s;',
                                       conn=conn, params=(users_for_delete, group_id), with_fetch=False)

        if isinstance(indexes, list):
            current_indexes = self.execute_query("SELECT name FROM index WHERE id IN "
                                                 "(SELECT index_id FROM index_group WHERE group_id = %s);",
                                                 params=(group_id,), fetchall=True)
            current_indexes = flat_to_set(current_indexes)
            target_indexes = set(indexes)

            indexes_for_add = target_indexes - current_indexes
            indexes_for_delete = tuple(current_indexes - target_indexes)

            with self.transaction('update_group_indexes') as conn:
                for index in indexes_for_add:
                    self.execute_query("INSERT INTO index_group (index_id, group_id) "
                                       "VALUES ((SELECT id FROM index WHERE name = %s), %s);",
                                       conn=conn, params=(index, group_id,), with_fetch=False)
                if indexes_for_delete:
                    self.execute_query("DELETE FROM index_group WHERE index_id IN "
                                       "(SELECT id FROM index WHERE name IN %s) AND group_id = %s;",
                                       conn=conn, params=(indexes_for_delete, group_id), with_fetch=False)

        if isinstance(dashs, list):
            current_dashs = self.execute_query("SELECT name FROM dash WHERE id IN "
                                               "(SELECT dash_id FROM dash_group WHERE group_id = %s);",
                                               params=(group_id,), fetchall=True)
            current_dashs = flat_to_set(current_dashs)
            target_dashs = set(dashs)

            dashs_for_add = target_dashs - current_dashs
            dashs_for_delete = tuple(current_dashs - target_dashs)

            with self.transaction('update_group_dashs') as conn:
                for dash in dashs_for_add:
                    self.execute_query("INSERT INTO dash_group (dash_id, group_id) "
                                       "VALUES ((SELECT id FROM dash WHERE name = %s), %s);",
                                       conn=conn, params=(dash, group_id,), with_fetch=False)
                if dashs_for_delete:
                    self.execute_query("DELETE FROM dash_group WHERE dash_id IN "
                                       "(SELECT id FROM index WHERE name IN %s) AND group_id = %s;",
                                       conn=conn, params=(dashs_for_delete, group_id), with_fetch=False)
        return group_id

    def delete_group(self, group_id):
        self.execute_query('DELETE FROM "group" WHERE id = %s;', params=(group_id,),
                           with_commit=True, with_fetch=False)
        return group_id

    # __INDEXES___ #################################################################

    def check_index_exists(self, index_name):
        index_id = self.execute_query("SELECT id FROM index WHERE name = %s;", params=(index_name,))
        return index_id

    def get_indexes_data(self, *, user_id=None, names_only=False):
        if user_id:
            user_groups = self.get_groups_data(user_id=user_id)
            indexes = list()

            for group in user_groups:
                group_indexes = self.execute_query("SELECT * FROM index WHERE id IN "
                                                   "(SELECT index_id FROM index_group WHERE group_id = %s);",
                                                   fetchall=True, params=(group.id,), as_obj=True)
                indexes.extend(group_indexes)
            indexes = list({v['id']: v for v in indexes}.values())
        else:
            indexes = self.execute_query('SELECT * FROM index;', fetchall=True, as_obj=True)

        if names_only:
            indexes = [i['name'] for i in indexes]
        else:
            for index in indexes:
                groups = self.execute_query('SELECT name FROM "group" WHERE id IN '
                                            '(SELECT group_id FROM index_group WHERE index_id = %s);',
                                            params=(index.id,), fetchall=True)
                groups = flat_to_list(groups)
                index['groups'] = groups
        return indexes

    def get_index_data(self, index_id):
        index = self.execute_query("SELECT * FROM index WHERE id = %s;",
                                   params=(index_id,), as_obj=True)
        groups = self.execute_query('SELECT name FROM "group" WHERE id IN '
                                    '(SELECT group_id FROM index_group WHERE index_id = %s);',
                                    params=(index.id,), fetchall=True)
        groups = flat_to_list(groups)
        index['groups'] = groups
        return index

    def add_index(self, *, name, groups):
        if self.check_index_exists(name):
            raise QueryError(f'index {name} already exists')

        with self.transaction('add_index_data') as conn:
            index_id = self.execute_query("INSERT INTO index (name) VALUES (%s) RETURNING id;",
                                          conn=conn, params=(name,))
            if groups:
                for group in groups:
                    self.execute_query('INSERT INTO index_group (group_id, index_id) '
                                       'VALUES ((SELECT id FROM "group" WHERE name = %s), %s);',
                                       conn=conn, params=(group, index_id,), with_fetch=False)
        return index_id

    def update_index(self, *, index_id, name, groups=None):
        if name:
            self.execute_query("UPDATE index SET name = %s WHERE id = %s;", params=(name, index_id),
                               with_commit=True, with_fetch=False)
        if isinstance(groups, list):
            current_groups = self.execute_query('SELECT name FROM "group" WHERE id IN '
                                                '(SELECT group_id FROM index_group WHERE index_id = %s);',
                                                params=(index_id,), fetchall=True)
            current_groups = flat_to_set(current_groups)
            target_groups = set(groups)

            groups_for_add = target_groups - current_groups
            groups_for_delete = tuple(current_groups - target_groups)

            with self.transaction('update_index_groups') as conn:
                for group in groups_for_add:
                    self.execute_query('INSERT INTO index_group (group_id, index_id) '
                                       'VALUES ((SELECT id FROM "group" WHERE name = %s), %s);',
                                       conn=conn, params=(group, index_id,), with_fetch=False)
                if groups_for_delete:
                    self.execute_query('DELETE FROM index_group  WHERE group_id IN '
                                       '(SELECT id FROM "group" WHERE name IN %s) AND index_id = %s;',
                                       conn=conn, params=(groups_for_delete, index_id), with_fetch=False)
        return index_id

    def delete_index(self, index_id):
        self.execute_query("DELETE FROM index WHERE id = %s;", params=(index_id,),
                           with_commit=True, with_fetch=False)
        return index_id

    # __PERMISSIONS__ ###############################################################

    def check_permission_exists(self, permission_name):
        permission_id = self.execute_query("SELECT id from permission WHERE name = %s;",
                                           params=(permission_name,))
        return permission_id

    def get_permissions_data(self, *, user_id=None, names_only=False):
        if user_id:
            user_roles = self.get_roles_data(user_id=user_id, names_only=True)
            permissions = list()
            for role in user_roles:
                role_id = self.execute_query("SELECT id FROM role WHERE name = %s;", params=(role,))
                role_permissions = self.execute_query("SELECT * FROM permission WHERE id IN "
                                                      "(SELECT permission_id FROM role_permission WHERE role_id = %s);",
                                                      params=(role_id,), fetchall=True, as_obj=True)
                permissions.extend(role_permissions)
            permissions = list({v['id']: v for v in permissions}.values())
        else:
            permissions = self.execute_query("SELECT * FROM permission;", fetchall=True, as_obj=True)
        if names_only:
            permissions = [p['name'] for p in permissions]
        else:
            for permission in permissions:
                roles = self.execute_query("SELECT name FROM role WHERE id IN "
                                           "(SELECT role_id FROM role_permission WHERE permission_id = %s);",
                                           params=(permission.id,), fetchall=True)
                roles = flat_to_list(roles)
                permission['roles'] = roles
        return permissions

    def get_permission_data(self, permission_id):
        permission = self.execute_query("SELECT * FROM permission WHERE id = %s;",
                                        params=(permission_id,), as_obj=True)
        roles = self.execute_query("SELECT name FROM role WHERE id IN "
                                   "(SELECT role_id FROM role_permission WHERE permission_id = %s);",
                                   params=(permission.id,), fetchall=True)
        roles = flat_to_list(roles)
        permission['roles'] = roles
        return permission

    def add_permission(self, *, name, roles):
        if self.check_permission_exists(name):
            raise QueryError(f'group {name} already exists')

        with self.transaction('add_permission_data') as conn:
            permission_id = self.execute_query("INSERT INTO permission (name) VALUES (%s) RETURNING id;",
                                               conn=conn, params=(name,))
            if roles:
                for role in roles:
                    self.execute_query("INSERT INTO role_permission (role_id, permission_id) "
                                       "VALUES ((SELECT id FROM role WHERE name = %s), %s);",
                                       conn=conn, params=(role, permission_id,), with_fetch=False)
        return permission_id

    def update_permission(self, *, permission_id, name, roles=None):
        if name:
            self.execute_query("UPDATE permission SET name = %s WHERE id = %s;",
                               params=(name, permission_id), with_commit=True, with_fetch=False)

        if isinstance(roles, list):
            current_roles = self.execute_query("SELECT name FROM role WHERE id IN "
                                               "(SELECT role_id FROM role_permission WHERE permission_id = %s);",
                                               params=(permission_id,), fetchall=True)
            current_roles = flat_to_set(current_roles)
            target_roles = set(roles)

            roles_for_add = target_roles - current_roles
            roles_for_delete = tuple(current_roles - target_roles)

            with self.transaction('update_group_users') as conn:
                for role in roles_for_add:
                    self.execute_query("INSERT INTO role_permission (role_id, permission_id) "
                                       "VALUES ((SELECT id FROM role WHERE name = %s), %s);",
                                       conn=conn, params=(role, permission_id,), with_fetch=False)
                if roles_for_delete:
                    self.execute_query("DELETE FROM role_permission WHERE role_id IN "
                                       "(SELECT id FROM role WHERE name IN %s) AND permission_id = %s;",
                                       conn=conn, params=(roles_for_delete, permission_id), with_fetch=False)
        return permission_id

    def delete_permission(self, permission_id):
        self.execute_query("DELETE FROM permission WHERE id = %s;", params=(permission_id,),
                           with_commit=True, with_fetch=False)
        return permission_id

    # __DASHBOARDS__ ###############################################################

    def check_dash_exists(self, dash_name):
        dash_id = self.execute_query("SELECT id FROM dash WHERE name = %s;", params=(dash_name,))
        return dash_id

    def get_dashs_data(self, *, group_id=None, names_only=False):
        if group_id:
            dashs = self.execute_query("SELECT id, name, body, round(extract(epoch from modified)) as modified "
                                       "FROM dash WHERE id IN (SELECT dash_id FROM dash_group WHERE group_id = %s);",
                                       params=(group_id,), fetchall=True, as_obj=True)
        else:
            dashs = self.execute_query("SELECT id, name, body, round(extract(epoch from modified)) as modified "
                                       "FROM dash;", fetchall=True, as_obj=True)

        if names_only:
            dashs = [d['name'] for d in dashs]
        else:
            for dash in dashs:
                groups = self.execute_query('SELECT name FROM "group" WHERE id IN '
                                            '(SELECT group_id FROM dash_group WHERE dash_id = %s);',
                                            params=(dash.id,), fetchall=True, as_obj=True)
                groups = list({v['name']: v for v in groups}.values())
                dash['groups'] = groups
        return dashs

    def get_dash_data(self, dash_id):
        dash_data = self.execute_query("SELECT id, name, body, round(extract(epoch from modified)) as modified "
                                       "FROM dash WHERE id = %s;", params=(dash_id,), as_obj=True)
        if not dash_data:
            raise ValueError(f'Dash with id={dash_id} is not exists')

        groups = self.execute_query('SELECT name FROM "group" WHERE id IN '
                                    '(SELECT group_id FROM dash_group WHERE dash_id = %s);',
                                    params=(dash_id,), fetchall=True)
        groups = flat_to_list(groups)
        dash_data['groups'] = groups
        return dash_data

    def get_dash_data_by_name(self, dash_name, dash_group):
        dash_data = self.execute_query("SELECT id, name, body, round(extract(epoch from modified)) as modified "
                                       "FROM dash WHERE name = %s LIMIT 1;", params=(dash_name,), as_obj=True)
        if not dash_data:
            raise ValueError(f'Dash with name={dash_name} is not exists')

        groups = self.execute_query('SELECT name FROM "group" WHERE id IN '
                                    '(SELECT group_id FROM dash_group WHERE dash_id = %s);',
                                    params=(dash_data['id'],), fetchall=True)
        groups = flat_to_list(groups)
        dash_data['groups'] = groups
        return dash_data

    def add_dash(self, *, name, body, groups=None):
        dash_id = self.check_dash_exists(dash_name=name)
        if dash_id:
            raise QueryError(f'dash with name={name} is already exists')

        with self.transaction('add_dashboard_data') as conn:
            dash = self.execute_query("INSERT INTO dash (name, body) VALUES (%s, %s) "
                                      "RETURNING id, round(extract(epoch from modified)) as modified;",
                                      conn=conn, params=(name, body,), as_obj=True)
            if isinstance(groups, list):
                for group in groups:
                    self.execute_query('INSERT INTO dash_group (group_id, dash_id) '
                                       'VALUES ((SELECT id FROM "group" WHERE name = %s), %s);',
                                       conn=conn, params=(group, dash.id,), with_fetch=False)
        return dash.id, dash.modified

    def update_dash(self, *, dash_id, name, body, groups=None):
        dash_name = self.execute_query("SELECT name FROM dash WHERE id = %s;", params=(dash_id,))
        if not dash_name:
            raise QueryError(f'dash with id={dash_id} is not exists')

        with self.transaction('update_dash_data') as conn:
            dash = self.execute_query("UPDATE dash SET modified = now() WHERE id = %s "
                                      "RETURNING name, round(extract(epoch from modified)) as modified;",
                                      conn=conn, params=(dash_id,), as_obj=True)
            if name:
                self.execute_query("UPDATE dash SET name = %s WHERE id = %s;",
                                   conn=conn, params=(name, dash_id), with_fetch=False)
            if body:
                self.execute_query("UPDATE dash SET body = %s WHERE id = %s;",
                                   conn=conn, params=(body, dash_id), with_fetch=False)

        if isinstance(groups, list):
            current_groups = self.execute_query('SELECT name FROM "group" WHERE id IN '
                                                '(SELECT group_id FROM dash_group WHERE dash_id = %s);',
                                                params=(dash_id,), fetchall=True)
            current_groups = flat_to_set(current_groups)
            target_groups = set(groups)

            groups_for_add = target_groups - current_groups
            groups_for_delete = tuple(current_groups - target_groups)

            with self.transaction('update_dash_groups') as conn:
                for group in groups_for_add:
                    self.execute_query('INSERT INTO dash_group (group_id, dash_id) '
                                       'VALUES ((SELECT id FROM "group" WHERE name = %s), %s);',
                                       conn=conn, params=(group, dash_id,), with_fetch=False)
                if groups_for_delete:
                    self.execute_query('DELETE FROM dash_group  WHERE group_id IN '
                                       '(SELECT id FROM "group" WHERE name IN %s) AND dash_id = %s;',
                                       conn=conn, params=(groups_for_delete, dash_id), with_fetch=False)
        return dash.name, dash.modified

    def delete_dash(self, dash_id):
        self.execute_query("DELETE FROM dash WHERE id = %s;",
                           params=(dash_id,), with_commit=True, with_fetch=False)
        return dash_id

    # __QUIZS__ ###############################################################

    QUIZ_TABLES = {'bool': 'boolAnswer', 'date': 'dateAnswer', 'text': 'textAnswer',
                   'cascade': 'cascadeAnswer', 'multi': 'multiAnswer', 'catalog': 'catalogAnswer'}

    def check_quiz_exists(self, quiz_name):
        quiz_id = self.execute_query("SELECT id FROM quiz WHERE name = %s;", params=(quiz_name,))
        return quiz_id

    def get_quizs_count(self):
        count = self.execute_query("SELECT COUNT(id) FROM quiz;")
        return count[0]

    def get_quizs(self, *, limit, offset):
        quizs_data = self.execute_query("SELECT * FROM quiz ORDER BY id limit %s offset %s;",
                                        params=(limit, offset), fetchall=True, as_obj=True)
        return quizs_data

    def get_quiz(self, quiz_id):
        quiz_data = self.execute_query("SELECT * FROM quiz WHERE id = %s;", params=(quiz_id,), as_obj=True)
        if not quiz_data:
            raise QueryError(f'quiz with id={quiz_id} not exists')
        questions_data = self.execute_query("SELECT id, text, description, type, sid FROM question "
                                            "WHERE quiz_id = %s;", params=(quiz_id,), fetchall=True, as_obj=True)
        for q in questions_data:
            q_id = q.pop('id')
            if q['type'] == 'cascade':
                q['childs'] = self.load_cascade(root_id=q_id)

        quiz_data['questions'] = questions_data
        return quiz_data

    def add_quiz(self, *, name, questions):
        if self.check_quiz_exists(quiz_name=name):
            raise QueryError(f'quiz {name} already exists')

        with self.transaction('create_quiz_data') as conn:
            quiz_id = self.execute_query("INSERT INTO quiz (name) values (%s) RETURNING id;",
                                         conn=conn, params=(name,))
            if not isinstance(questions, list):
                return quiz_id
            for sid, q in enumerate(questions, 1):
                if q['type'] == 'cascade':
                    self.save_cascade(question=q, quiz_id=quiz_id, sid=sid, conn=conn)
                else:
                    self.execute_query(
                        "INSERT INTO question (text, type, is_sign, description, label, sid, quiz_id, catalog_id) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;",
                        conn=conn, params=(q['text'], q['type'], q.get('is_sign', False), q.get('description'),
                                           q.get('label'), sid, quiz_id, q.get('catalog_id')))
        return quiz_id

    def save_cascade(self, *, parent_id=None, quiz_id=None, sid=None, question, conn):
        saved_id = self.execute_query(
            "INSERT INTO question (text, type, is_sign, description, label, sid, quiz_id, parent_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;",
            conn=conn, params=(question['text'], question['type'], question.get('is_sign', False),
                               question.get('description'), question.get('label'), sid, quiz_id, parent_id))
        childs = question.get('childs')
        if not childs:
            return

        for child in childs:
            if child['type'] == 'cascade':
                self.save_cascade(parent_id=saved_id, question=child, conn=conn)
            else:
                self.execute_query(
                    "INSERT INTO question (text, type, is_sign, description, label, parent_id) "
                    "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
                    conn=conn, params=(child['text'], child['type'], child.get('is_sign', False),
                                       child.get('description'), child.get('label'), saved_id))

    def load_cascade(self, root_id):
        childs = self.execute_query("SELECT id, type, text, sid FROM question WHERE parent_id = %s ORDER BY id;",
                                    params=(root_id,), fetchall=True, as_obj=True)
        for child in childs:
            c_id = child.pop('id')
            if child['type'] == 'cascade':
                child['childs'] = self.load_cascade(root_id=c_id)
        return childs

    def update_quiz(self, *, quiz_id, name, questions=None):
        with self.transaction('update_quiz_data') as conn:
            if name:
                self.execute_query("UPDATE quiz SET name = %s WHERE id = %s;",
                                   conn=conn, params=(name, quiz_id), with_fetch=False)
            if not isinstance(questions, list):
                return quiz_id

            self.execute_query("DELETE FROM question WHERE quiz_id = %s;",
                               conn=conn, params=(quiz_id,), with_fetch=False)
            for sid, q in enumerate(questions, 1):
                if q['type'] == 'cascade':
                    self.save_cascade(question=q, quiz_id=quiz_id, sid=sid, conn=conn)
                else:
                    self.execute_query(
                        "INSERT INTO question (text, type, is_sign, description, label, sid, quiz_id, catalog_id) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;",
                        conn=conn, params=(q['text'], q['type'], q.get('is_sign', False),
                                           q.get('description'), q.get('label'), sid, quiz_id, q['catalog_id']))
        return quiz_id

    def get_quiz_questions(self, quiz_ids):
        quizs = dict()
        questions_data = self.execute_query(
            "select question.id as id, sid, text, description, type, is_sign, catalog_id, "
            "label, quiz.name as quiz_name, quiz.id as qid from question inner join quiz on quiz_id=quiz.id "
            "where question.quiz_id in %s order by sid;", params=(tuple(quiz_ids),), fetchall=True,
            as_obj=True)

        for question in questions_data:
            quiz_name = question['quiz_name']
            quiz_id = question['qid']

            if question['type'] == 'cascade':
                question['childs'] = self.load_cascade(root_id=question.pop('id'))

            if question.qid in quizs:
                quizs[quiz_id]['questions'].append(question)
            else:
                quizs[quiz_id] = {'id': quiz_id, 'name': quiz_name, 'questions': [question]}
        return list(quizs.values())

    def get_filled_quizs_count(self, quiz_id):
        count = self.execute_query("SELECT COUNT(id) FROM filled_quiz WHERE quiz_id = %s;", params=(quiz_id,))
        return count[0]

    def get_filled_quiz(self, *, offset=0, limit=1, quiz_id=None, current=False):
        # Get current quiz from filled_quiz table
        if quiz_id and current:
            f_quizs = self.execute_query(
                "SELECT * FROM filled_quiz WHERE id = %s limit %s offset %s;",
                params=(quiz_id, limit, offset), fetchall=True, as_obj=True)
            self.logger.debug("SELECT * FROM filled_quiz WHERE id = %s limit %s offset %s;" % (quiz_id, limit, offset))
            self.logger.debug(f_quizs)

            answer_id = quiz_id
            quiz_id = f_quizs[0].quiz_id if f_quizs else None
            quiz_name = self.execute_query("SELECT name FROM quiz WHERE id = %s;", params=(quiz_id,), as_obj=True)
            self.logger.debug("SELECT name FROM quiz WHERE id = %s;" % quiz_id)
            self.logger.debug(quiz_name)

        # Get filled quizs for current base quiz
        elif quiz_id:
            f_quizs = self.execute_query("SELECT * FROM filled_quiz WHERE quiz_id = %s order by id limit %s offset %s;",
                                         params=(quiz_id, limit, offset), fetchall=True, as_obj=True)
            quiz_name = self.execute_query('SELECT name FROM quiz WHERE id = %s;',
                                           params=(quiz_id,), as_obj=True)
        # If not quiz_id get statistic by quiz name, filler and last fill date
        # TODO: Maybe better to move this section in separate handler
        else:
            f_quizs = self.execute_query("select distinct on (quiz_id) quiz_id, fill_date, filler, "
                                         "quiz.name as name from filled_quiz inner join quiz on quiz.id=quiz_id "
                                         "order by quiz_id, fill_date desc;", fetchall=True, as_obj=True)
            for quiz in f_quizs:
                quiz['fill_date'] = str(quiz.fill_date)
            return f_quizs

        _questions = self.execute_query("SELECT id, sid, type, text, is_sign, label FROM question "
                                        "WHERE quiz_id = %s ORDER BY sid;", as_obj=True,
                                        params=(quiz_id,), fetchall=True)
        self.logger.debug(
            "SELECT id, sid, type, text, is_sign, label FROM question WHERE quiz_id = %s ORDER BY sid;" % quiz_id)

        for quiz in f_quizs:
            questions = deepcopy(_questions)
            answers = self.execute_query(
                'select filled_quiz.id as filled_quiz_id, question.type, question.sid as sid, '
                'coalesce(textanswer.value, cascadeanswer.value, multianswer.value::text, '
                'cataloganswer.value, dateanswer.value::text) as value, '
                'coalesce (textanswer.description, cascadeanswer.description, multianswer.description, '
                'cataloganswer.description, dateanswer.description) as description from filled_quiz '
                'join question on question.quiz_id=filled_quiz.quiz_id '
                'left join textanswer on textanswer.id=filled_quiz.id and textanswer.sid=question.sid '
                'left join cascadeanswer on cascadeanswer.id=filled_quiz.id and cascadeanswer.sid=question.sid '
                'left join multianswer on multianswer.id=filled_quiz.id and multianswer.sid=question.sid '
                'left join cataloganswer on cataloganswer.id=filled_quiz.id and cataloganswer.sid=question.sid '
                'left join dateanswer on dateanswer.id=filled_quiz.id and dateanswer.sid=question.sid '
                'where filled_quiz.id = %s order by question.sid;',
                params=(quiz.id,), fetchall=True, as_obj=True
            )
            self.logger.debug('select filled_quiz.id as filled_quiz_id, question.type, question.sid as sid, '
                              'coalesce(textanswer.value, cascadeanswer.value, multianswer.value::text, '
                              'cataloganswer.value, dateanswer.value::text) as value, '
                              'coalesce (textanswer.description, cascadeanswer.description, multianswer.description, '
                              'cataloganswer.description, dateanswer.description) as description from filled_quiz '
                              'join question on question.quiz_id=filled_quiz.quiz_id '
                              'left join textanswer on textanswer.id=filled_quiz.id and textanswer.sid=question.sid '
                              'left join cascadeanswer on cascadeanswer.id=filled_quiz.id and cascadeanswer.sid=question.sid '
                              'left join multianswer on multianswer.id=filled_quiz.id and multianswer.sid=question.sid '
                              'left join cataloganswer on cataloganswer.id=filled_quiz.id and cataloganswer.sid=question.sid '
                              'left join dateanswer on dateanswer.id=filled_quiz.id and dateanswer.sid=question.sid '
                              'where filled_quiz.id = %s order by question.sid;' % quiz.id)

            for q, a in zip(questions, answers):
                q['answer'] = a
            quiz['questions'] = questions
            quiz['name'] = quiz_name.name
            quiz['fill_date'] = str(quiz.fill_date)
        return f_quizs

    def save_filled_quiz(self, *, user_id, quiz_id, questions):
        with self.transaction('save_quiz') as conn:
            user = self.execute_query('SELECT name FROM "user" WHERE id = %s;', conn=conn,
                                      params=(user_id,), as_obj=True)
            quiz = self.execute_query("INSERT INTO filled_quiz (filler, quiz_id) VALUES (%s, %s) RETURNING id;",
                                      conn=conn, params=(user.name, quiz_id,), as_obj=True)

            for sid, q in enumerate(questions, 1):
                answer = q.pop('answer', None)
                if not answer:
                    continue
                table = self.QUIZ_TABLES.get(q['type'])
                if not table:
                    raise QueryError(f'answer with type {q["type"]} is not exists')
                query = "INSERT INTO %s (id, sid, value, description) VALUES (%%s, %%s, %%s, %%s);" % table
                self.execute_query(query, conn=conn, with_fetch=False,
                                   params=(quiz.id, sid, answer['value'], answer.get('description')))
        return quiz_id

    def delete_quiz(self, quiz_id):
        self.execute_query("DELETE FROM quiz WHERE id = %s;",
                           params=(quiz_id,), with_commit=True, with_fetch=False)
        return quiz_id

    # __CATALOGS__ #############################################################

    def check_catalog_exists(self, name):
        catalog_id = self.execute_query("SELECT id FROM catalog WHERE name = %s;", params=(name,))
        return catalog_id

    def get_catalogs_count(self):
        count = self.execute_query("SELECT COUNT(id) FROM catalog;")
        return count[0]

    def get_catalogs_data(self, *, limit, offset):
        return self.execute_query("SELECT id, name FROM catalog ORDER BY id limit %s offset %s;",
                                  params=(limit, offset), fetchall=True, as_obj=True)

    def add_catalog(self, *, name, content):
        if self.check_catalog_exists(name):
            raise QueryError(f'catalog {name} already exists')
        catalog_id = self.execute_query("INSERT INTO catalog (name, content) VALUES (%s, %s) RETURNING id;",
                                        params=(name, content), with_commit=True)
        return catalog_id

    def update_catalog(self, *, catalog_id, name, content):
        with self.transaction('update_catalog_data') as conn:
            if name:
                self.execute_query("UPDATE catalog SET name = %s WHERE id = %s;",
                                   conn=conn, params=(name, catalog_id), with_fetch=False)
            if content:
                self.execute_query("UPDATE catalog SET content = %s WHERE id = %s;",
                                   conn=conn, params=(content, catalog_id), with_fetch=False)
        return catalog_id

    def get_catalog(self, catalog_id):
        return self.execute_query("SELECT * FROM catalog WHERE id = %s;",
                                  params=(catalog_id,), as_obj=True)

    def delete_catalog(self, catalog_id):
        self.execute_query("DELETE FROM catalog WHERE id = %s;",
                           params=(catalog_id,), with_commit=True, with_fetch=False)
        return catalog_id

    def get_themes_data(self, *, limit, offset):
        self.logger.debug(f'SELECT name FROM theme ORDER BY name limit {limit} offset {offset};')
        return self.execute_query("SELECT name FROM theme ORDER BY name limit %s offset %s;",
                                  params=list([limit, offset]), fetchall=True, as_obj=True)

    def get_theme(self, theme_name):
        self.logger.debug(f'SELECT * FROM theme WHERE name = "{theme_name}";')
        return self.execute_query("SELECT * FROM theme WHERE name = %s;",
                                  params=list([theme_name]), as_obj=True)

    def check_theme_exists(self, theme_name):
        self.logger.debug(f'SELECT name FROM theme WHERE name = "{theme_name}";')
        return self.execute_query("SELECT name FROM theme WHERE name = %s;", params=list([theme_name]))

    def add_theme(self, *, theme_name, content):
        if self.check_theme_exists(theme_name):
            raise Exception(f'theme "{theme_name}" already exists')
        self.logger.debug(f'INSERT INTO theme (name, content) VALUES ("{theme_name}", "{content}") RETURNING content;')
        theme = self.execute_query("INSERT INTO theme (name, content) VALUES (%s, %s) RETURNING content;",
                                        params=list([theme_name, content]), with_commit=True)
        return theme

    def delete_theme(self, theme_name):
        self.logger.debug(f'DELETE FROM theme WHERE name = "{theme_name}";')
        self.execute_query("DELETE FROM theme WHERE name = %s;",
                           params=list([theme_name]), with_commit=True, with_fetch=False)
        return theme_name
