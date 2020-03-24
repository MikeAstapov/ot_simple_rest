import logging
from contextlib import contextmanager

import tornado.util
import psycopg2

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Andrey Starchenkov"
__email__ = "akhromov@ot.ru"
__status__ = "Development"


class QueryError(Exception):
    pass


def flat_to_set(arr):
    return {i[0] for i in arr} if arr else set()


def flat_to_list(arr):
    return [i[0] for i in arr] if arr else list()


class PostgresConnector:
    def __init__(self, conn_pool):
        self.pool = conn_pool
        self.logger = logging.getLogger('osr')

    @contextmanager
    def transaction(self, name="transaction", **kwargs):
        options = {
            "isolation_level": kwargs.get("isolation_level", None),
            "readonly": kwargs.get("readonly", None),
            "deferrable": kwargs.get("deferrable", None),
        }
        conn = self.pool.getconn()
        try:
            conn.set_session(**options)
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Transaction {name} error: {e}")
            raise RuntimeError(f"Transaction {name} failed")
        finally:
            conn.reset()
            self.pool.putconn(conn)

    def row_to_obj(self, row, cur):
        """Convert a SQL row to an object supporting dict and attribute access."""
        obj = tornado.util.ObjectDict()
        for val, desc in zip(row, cur.description):
            obj[desc.name] = val
        return obj

    def execute_query(self, query, conn=None, params=None, with_commit=False,
                      with_fetch=True, as_obj=False, fetchall=False):
        fetch_result = None

        if not conn:
            with_transaction = False
            conn = self.pool.getconn()
        else:
            with_transaction = True

        cur = conn.cursor()

        try:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            if with_fetch:
                if fetchall:
                    fetch_result = cur.fetchall()
                    if as_obj:
                        fetch_result = [self.row_to_obj(row, cur) for row in fetch_result]
                else:
                    fetch_result = cur.fetchone()
                    if as_obj and fetch_result:
                        fetch_result = self.row_to_obj(fetch_result, cur)
            if with_commit:
                conn.commit()
        except psycopg2.OperationalError as err:
            self.logger.error(f'SQL Error: {err}')
        else:
            return fetch_result
        finally:
            cur.close()
            if not with_transaction:
                self.pool.putconn(conn)

    # __AUTH__ ######################################################################

    def get_user_tokens(self, user_id):
        tokens = self.execute_query("""SELECT token FROM session WHERE user_id = %s;""",
                                    params=(user_id,), fetchall=True)
        tokens = flat_to_set(tokens)
        return tokens

    def get_auth_data(self, user_id):
        login_pass = self.execute_query("""SELECT name, password FROM "user" WHERE id = %s;""",
                                        params=(user_id,), as_obj=True)
        return login_pass

    def add_session(self, *, user_id, token, expired_date):
        self.execute_query("""INSERT INTO session (token, user_id, expired_date) VALUES (%s, %s, %s);""",
                           params=(token, user_id, expired_date), with_commit=True, with_fetch=False)

    # __USERS__ #######################################################################

    def check_user_exists(self, name):
        user = self.execute_query("""SELECT * FROM "user" WHERE name = %s;""",
                                  params=(name,), as_obj=True)
        return user

    def get_users_data(self, *, user_id=None, names_only=False):
        if user_id:
            users = self.execute_query("""SELECT id, name FROM "user" WHERE id = %s;""",
                                       params=(user_id,), fetchall=True, as_obj=True)
        else:
            users = self.execute_query("""SELECT id, name FROM "user";""", fetchall=True, as_obj=True)
        if names_only:
            users = [u['name'] for u in users]
        else:
            for user in users:
                user_roles = self.execute_query("SELECT name FROM role WHERE id IN "
                                                "(SELECT role_id FROM user_role WHERE user_id = %s);",
                                                params=(user.id,), fetchall=True)
                user_roles = flat_to_list(user_roles)
                user['roles'] = user_roles

                user_groups = self.execute_query("""SELECT name FROM "group" WHERE id IN 
                                                (SELECT group_id FROM user_group WHERE user_id = %s);""",
                                                 params=(user.id,), fetchall=True)
                user_groups = flat_to_list(user_groups)
                user['groups'] = user_groups
        return users

    def get_user_data(self, *, user_id):
        user_data = self.execute_query("""SELECT id, name FROM "user" WHERE id = %s;""",
                                       params=(user_id,), as_obj=True)
        user_roles = self.execute_query("""SELECT name FROM role WHERE id IN 
                                        (SELECT role_id FROM user_role WHERE user_id = %s);""",
                                        params=(user_id,), fetchall=True)
        user_roles = flat_to_list(user_roles)
        user_data['roles'] = user_roles

        user_groups = self.execute_query("""SELECT name FROM "group" WHERE id IN 
                                        (SELECT group_id FROM user_group WHERE user_id = %s);""",
                                         params=(user_data.id,), fetchall=True)
        user_groups = flat_to_list(user_groups)
        user_data['groups'] = user_groups

        return user_data

    def add_user(self, *, name, password, roles, groups):
        if self.check_user_exists(name):
            raise QueryError(f'user {name} already exists')

        with self.transaction('add_user_data') as conn:
            user_id = self.execute_query("""INSERT INTO "user" (name, password) VALUES (%s, %s) RETURNING id;""",
                                         conn=conn, params=(name, password))
            if roles:
                for role in roles:
                    self.execute_query("""INSERT INTO user_role (role_id, user_id) 
                                        VALUES ((SELECT id FROM role WHERE name = %s), %s);""",
                                       conn=conn, params=(role, user_id), with_fetch=False)
            if groups:
                for group in groups:
                    self.execute_query("""INSERT INTO user_group (group_id, user_id) 
                                        VALUES ((SELECT id FROM "group" WHERE name = %s), %s);""",
                                       conn=conn, params=(group, user_id), with_fetch=False)
        return user_id

    def update_user(self, *, user_id, name, password, roles=None, groups=None):
        if name:
            self.execute_query("""UPDATE "user" SET name = %s WHERE id = %s;""", params=(name, user_id),
                               with_commit=True, with_fetch=False)
        if password:
            self.execute_query("""UPDATE "user" SET password = %s WHERE id = %s;""", params=(password, user_id),
                               with_commit=True, with_fetch=False)

        if isinstance(roles, list):
            current_roles = self.execute_query("""SELECT name FROM role WHERE id IN 
                                                (SELECT role_id FROM user_role WHERE user_id = %s);""",
                                               params=(user_id,), fetchall=True)
            current_roles = flat_to_set(current_roles)
            target_roles = set(roles)

            roles_for_add = target_roles - current_roles
            roles_for_delete = tuple(current_roles - target_roles)

            with self.transaction('update_user_roles') as conn:
                for role in roles_for_add:
                    self.execute_query("""INSERT INTO user_role (role_id, user_id) 
                                        VALUES ((SELECT id FROM role WHERE name = %s), %s);""",
                                       conn=conn, params=(role, user_id,), with_fetch=False)
                if roles_for_delete:
                    self.execute_query("""DELETE FROM user_role WHERE role_id IN (SELECT id FROM role WHERE name IN %s) 
                                        AND user_id = %s;""",
                                       conn=conn, params=(roles_for_delete, user_id,), with_fetch=False)

        if isinstance(groups, list):
            current_groups = self.execute_query("""SELECT name FROM "group" WHERE id IN 
                                                    (SELECT group_id FROM user_group WHERE user_id = %s);""",
                                                params=(user_id,), fetchall=True)
            current_groups = flat_to_set(current_groups)
            target_groups = set(groups)

            groups_for_add = target_groups - current_groups
            groups_for_delete = tuple(current_groups - target_groups)

            with self.transaction('update_user_groups') as conn:
                for group in groups_for_add:
                    group_id = self.execute_query("""SELECT id FROM "group" WHERE name = %s;""",
                                                  conn=conn, params=(group,))
                    self.execute_query("""INSERT INTO user_group (user_id, group_id) VALUES (%s, %s);""",
                                       conn=conn, params=(user_id, group_id,), with_fetch=False)
                if groups_for_delete:
                    self.execute_query("""DELETE FROM user_group WHERE user_id = %s AND group_id IN 
                                        (SELECT id FROM "group" WHERE name in %s);""",
                                       conn=conn, params=(user_id, groups_for_delete,), with_fetch=False)
        return user_id

    def delete_user(self, user_id):
        self.execute_query("""DELETE FROM "user" WHERE id = %s;""", params=(user_id,),
                           with_commit=True, with_fetch=False)
        return user_id

    # __ROLES__ #####################################################################

    def check_role_exists(self, role_name):
        role_id = self.execute_query("""SELECT id FROM role WHERE name = %s;""", params=(role_name,))
        return role_id

    def get_roles_data(self, user_id=None, names_only=False, with_relations=False):
        if user_id:
            roles = self.execute_query("""SELECT * FROM role WHERE id IN 
                                        (SELECT role_id FROM user_role WHERE user_id = %s);""",
                                       fetchall=True, params=(user_id,), as_obj=True)
        else:
            roles = self.execute_query("""SELECT * FROM role;""", fetchall=True, as_obj=True)
        if names_only:
            roles = [r['name'] for r in roles]
        else:
            for role in roles:
                permissions = self.execute_query("""SELECT name FROM permission WHERE id IN 
                                                (SELECT permission_id FROM role_permission WHERE role_id = %s);""",
                                                 params=(role.id,), fetchall=True)
                permissions = flat_to_list(permissions)
                role['permissions'] = permissions
                users = self.execute_query("""SELECT name FROM "user" WHERE id in 
                                                (SELECT user_id FROM user_role WHERE role_id = %s)""",
                                           params=(role.id,), fetchall=True)
                users = flat_to_list(users)
                role['users'] = users

            if with_relations:
                all_permissions = self.get_permissions_data(names_only=True)
                all_users = self.get_users_data(names_only=True)
                roles = {'roles': roles, 'permissions': all_permissions, 'users': all_users}
        return roles

    def get_role_data(self, role_id):
        role = self.execute_query("""SELECT * FROM role WHERE id = %s;""",
                                  params=(role_id,), as_obj=True)
        permissions = self.execute_query("""SELECT name FROM permission WHERE id IN 
                                        (SELECT permission_id FROM role_permission WHERE role_id = %s);""",
                                         params=(role.id,), fetchall=True)
        permissions = flat_to_list(permissions)
        role['permissions'] = permissions
        users = self.execute_query("""SELECT name FROM "user" WHERE id in 
                                (SELECT user_id FROM user_role WHERE role_id = %s)""",
                                   params=(role.id,), fetchall=True)
        users = flat_to_list(users)
        role['users'] = users
        return role

    def add_role(self, *, name, users, permissions):
        if self.check_role_exists(name):
            raise QueryError(f'role {name} already exists')

        with self.transaction('add_role_data') as conn:
            role_id = self.execute_query("""INSERT INTO role (name) VALUES (%s) RETURNING id;""",
                                         conn=conn, params=(name,))
            if users:
                for user in users:
                    self.execute_query("""INSERT INTO user_role (user_id, role_id) 
                                        VALUES ((SELECT id FROM "user" WHERE name = %s), %s);""",
                                       conn=conn, params=(user, role_id,), with_fetch=False)
            if permissions:
                for permission in permissions:
                    self.execute_query("""INSERT INTO role_permission (permission_id, role_id) 
                                        VALUES ((SELECT id FROM permission WHERE name = %s), %s);""",
                                       conn=conn, params=(permission, role_id,), with_fetch=False)
        return role_id

    def update_role(self, *, role_id, name, users=None, permissions=None):
        if name:
            self.execute_query("""UPDATE role SET name = %s WHERE id = %s;""", params=(name, role_id),
                               with_commit=True, with_fetch=False)
        if isinstance(users, list):
            current_users = self.execute_query("""SELECT name FROM "user" WHERE id IN 
                                                (SELECT user_id FROM user_role WHERE role_id = %s);""",
                                               params=(role_id,), fetchall=True)
            current_users = flat_to_set(current_users)
            target_users = set(users)

            users_for_add = target_users - current_users
            users_for_delete = tuple(current_users - target_users)

            with self.transaction('update_role_users') as conn:

                for user in users_for_add:
                    self.execute_query("""INSERT INTO user_role (user_id, role_id) 
                                        VALUES ((SELECT id FROM "user" WHERE name = %s), %s);""",
                                       conn=conn, params=(user, role_id,), with_fetch=False)
                if users_for_delete:
                    self.execute_query("""DELETE FROM user_role WHERE user_id IN 
                                        (SELECT id FROM "user" WHERE name IN %s) AND role_id = %s;""",
                                       conn=conn, params=(users_for_delete, role_id), with_fetch=False)

        if isinstance(permissions, list):
            current_permissions = self.execute_query("""SELECT name FROM permission WHERE id IN 
                                                    (SELECT permission_id FROM role_permission WHERE role_id = %s);""",
                                                     params=(role_id,), fetchall=True)
            current_permissions = flat_to_set(current_permissions)
            target_permissions = set(permissions)

            permissions_for_add = target_permissions - current_permissions
            permissions_for_delete = tuple(current_permissions - target_permissions)

            with self.transaction('update_role_permissions') as conn:

                for permission in permissions_for_add:
                    self.execute_query("""INSERT INTO role_permission (permission_id, role_id) 
                                        VALUES ((SELECT id FROM permission WHERE name = %s), %s);""",
                                       conn=conn, params=(permission, role_id,), with_fetch=False)
                if permissions_for_delete:
                    self.execute_query("""DELETE FROM role_permission WHERE permission_id IN 
                                        (SELECT id FROM permission WHERE name IN %s) AND role_id = %s;""",
                                       conn=conn, params=(permissions_for_delete, role_id), with_fetch=False)
        return role_id

    def get_role(self, role_id):
        role_data = self.execute_query("""SELECT * FROM role WHERE id = %s;""", params=(role_id,), as_obj=True)
        return role_data

    def delete_role(self, role_id):
        self.execute_query("""DELETE FROM role WHERE id = %s;""", params=(role_id,),
                           with_commit=True, with_fetch=False)
        return role_id

    # __GROUPS__ ###################################################################

    def check_group_exists(self, group_name):
        group_id = self.execute_query("""SELECT id FROM "group" WHERE name = %s;""", params=(group_name,))
        return group_id

    def get_groups_data(self, *, user_id=None, names_only=False, with_relations=True):
        if user_id:
            groups = self.execute_query("""SELECT * FROM "group" WHERE id IN 
                                        (SELECT group_id FROM user_group WHERE user_id = %s);""",
                                        fetchall=True, params=(user_id,), as_obj=True)
        else:
            groups = self.execute_query("""SELECT * FROM "group";""", fetchall=True, as_obj=True)

        if names_only:
            groups = [g['name'] for g in groups]
        elif with_relations:
            for group in groups:
                users = self.execute_query("""SELECT name FROM "user" WHERE id IN 
                                            (SELECT user_id FROM user_group WHERE group_id = %s);""",
                                           params=(group.id,), fetchall=True)
                users = flat_to_list(users)
                group['users'] = users

                dashboards = self.execute_query("""SELECT name FROM dash WHERE id IN 
                                                (SELECT dash_id FROM dash_group WHERE group_id = %s);""",
                                                params=(group.id,), fetchall=True)
                dashboards = flat_to_list(dashboards)
                group['dashs'] = dashboards

                indexes = self.execute_query("""SELECT name FROM index WHERE id IN 
                                            (SELECT index_id FROM index_group WHERE group_id = %s);""",
                                             params=(group.id,), fetchall=True)
                indexes = flat_to_list(indexes)
                group['indexes'] = indexes
        return groups

    def get_group_data(self, group_id):
        group = self.execute_query("""SELECT * FROM "group" WHERE id = %s;""",
                                   params=(group_id,), as_obj=True)
        if not group:
            raise ValueError(f'group with id={group_id} is not exists')

        users = self.execute_query("""SELECT name FROM "user" WHERE id IN 
                                    (SELECT user_id FROM user_group WHERE group_id = %s);""",
                                   params=(group.id,), fetchall=True)
        users = flat_to_list(users)
        group['users'] = users

        dashboards = self.execute_query("""SELECT name FROM dash WHERE id IN 
                                        (SELECT dash_id FROM dash_group WHERE group_id = %s);""",
                                        params=(group.id,), fetchall=True)
        dashboards = flat_to_list(dashboards)
        group['dashs'] = dashboards

        indexes = self.execute_query("""SELECT name FROM index WHERE id IN 
                                    (SELECT index_id FROM index_group WHERE group_id = %s);""",
                                     params=(group.id,), fetchall=True)
        indexes = flat_to_list(indexes)
        group['indexes'] = indexes
        return group

    def add_group(self, *, name, color, users=None, indexes=None, dashs=None):
        if self.check_group_exists(name):
            raise QueryError(f'group {name} already exists')

        with self.transaction('add_group_data') as conn:
            group_id = self.execute_query("""INSERT INTO "group" (name, color) VALUES (%s, %s) RETURNING id;""",
                                          conn=conn, params=(name, color))
            if users:
                for user in users:
                    self.execute_query("""INSERT INTO user_group (user_id, group_id) 
                                        VALUES ((SELECT id FROM "user" WHERE name = %s), %s);""",
                                       conn=conn, params=(user, group_id,), with_fetch=False)
            if indexes:
                for index in indexes:
                    self.execute_query("""INSERT INTO index_group (index_id, group_id) 
                                        VALUES ((SELECT id FROM index WHERE name = %s), %s);""",
                                       conn=conn, params=(index, group_id,), with_fetch=False)

            if dashs:
                for dash in dashs:
                    self.execute_query("""INSERT INTO dash_group (dash_id, group_id) 
                                        VALUES ((SELECT id FROM dash WHERE name = %s), %s);""",
                                       conn=conn, params=(dash, group_id,), with_fetch=False)
        return group_id

    def update_group(self, *, group_id, name, color, users=None, indexes=None, dashs=None):
        if name:
            self.execute_query("""UPDATE "group" SET name = %s WHERE id = %s;""",
                               params=(name, group_id), with_commit=True, with_fetch=False)
        if color:
            self.execute_query("""UPDATE "group" SET color = %s WHERE id = %s;""",
                               params=(color, group_id), with_commit=True, with_fetch=False)

        if isinstance(users, list):
            current_users = self.execute_query("""SELECT name FROM "user" WHERE id IN 
                                                (SELECT user_id FROM user_group WHERE group_id = %s);""",
                                               params=(group_id,), fetchall=True)
            current_users = flat_to_set(current_users)
            target_users = set(users)

            users_for_add = target_users - current_users
            users_for_delete = tuple(current_users - target_users)

            with self.transaction('update_group_users') as conn:
                for user in users_for_add:
                    self.execute_query("""INSERT INTO user_group (user_id, group_id) 
                                        VALUES ((SELECT id FROM "user" WHERE name = %s), %s);""",
                                       conn=conn, params=(user, group_id,), with_fetch=False)
                if users_for_delete:
                    self.execute_query("""DELETE FROM user_group
                                        WHERE user_id IN (SELECT id FROM "user" WHERE name IN %s) AND group_id = %s;""",
                                       conn=conn, params=(users_for_delete, group_id), with_fetch=False)

        if isinstance(indexes, list):
            current_indexes = self.execute_query("""SELECT name FROM index WHERE id IN 
                                                (SELECT index_id FROM index_group WHERE group_id = %s);""",
                                                 params=(group_id,), fetchall=True)
            current_indexes = flat_to_set(current_indexes)
            target_indexes = set(indexes)

            indexes_for_add = target_indexes - current_indexes
            indexes_for_delete = tuple(current_indexes - target_indexes)

            with self.transaction('update_group_indexes') as conn:
                for index in indexes_for_add:
                    self.execute_query("""INSERT INTO index_group (index_id, group_id) 
                                        VALUES ((SELECT id FROM index WHERE name = %s), %s);""",
                                       conn=conn, params=(index, group_id,), with_fetch=False)
                if indexes_for_delete:
                    self.execute_query("""DELETE FROM index_group WHERE index_id IN 
                                        (SELECT id FROM index WHERE name IN %s) AND group_id = %s;""",
                                       conn=conn, params=(indexes_for_delete, group_id), with_fetch=False)

        if isinstance(dashs, list):
            current_dashs = self.execute_query("""SELECT name FROM dash WHERE id IN 
                                                (SELECT dash_id FROM dash_group WHERE group_id = %s);""",
                                               params=(group_id,), fetchall=True)
            current_dashs = flat_to_set(current_dashs)
            target_dashs = set(dashs)

            dashs_for_add = target_dashs - current_dashs
            dashs_for_delete = tuple(current_dashs - target_dashs)

            with self.transaction('update_group_dashs') as conn:
                for dash in dashs_for_add:
                    self.execute_query("""INSERT INTO dash_group (dash_id, group_id) 
                                        VALUES ((SELECT id FROM dash WHERE name = %s), %s);""",
                                       conn=conn, params=(dash, group_id,), with_fetch=False)
                if dashs_for_delete:
                    self.execute_query("""DELETE FROM dash_group WHERE dash_id IN 
                                        (SELECT id FROM index WHERE name IN %s) AND group_id = %s;""",
                                       conn=conn, params=(dashs_for_delete, group_id), with_fetch=False)
        return group_id

    def delete_group(self, group_id):
        self.execute_query("""DELETE FROM "group" WHERE id = %s;""", params=(group_id,),
                           with_commit=True, with_fetch=False)
        return group_id

    # __INDEXES___ #################################################################

    def check_index_exists(self, index_name):
        index_id = self.execute_query("""SELECT id FROM index WHERE name = %s;""", params=(index_name,))
        return index_id

    def get_indexes_data(self, *, user_id=None, names_only=False):
        if user_id:
            user_groups = self.get_groups_data(user_id=user_id)
            indexes = list()

            for group in user_groups:
                group_indexes = self.execute_query("""SELECT * FROM index WHERE id IN 
                                                    (SELECT index_id FROM index_group WHERE group_id = %s);""",
                                                   fetchall=True, params=(group.id,), as_obj=True)
                indexes.extend(group_indexes)
            indexes = list({v['id']: v for v in indexes}.values())
        else:
            indexes = self.execute_query("""SELECT * FROM index;""", fetchall=True, as_obj=True)

        if names_only:
            indexes = [i['name'] for i in indexes]
        else:
            for index in indexes:
                groups = self.execute_query("""SELECT name FROM "group" WHERE id IN 
                                            (SELECT group_id FROM index_group WHERE index_id = %s);""",
                                            params=(index.id,), fetchall=True)
                groups = flat_to_list(groups)
                index['groups'] = groups
        return indexes

    def get_index_data(self, index_id):
        index = self.execute_query("""SELECT * FROM index WHERE id = %s;""",
                                   params=(index_id,), as_obj=True)
        groups = self.execute_query("""SELECT name FROM "group" WHERE id IN 
                                    (SELECT group_id FROM index_group WHERE index_id = %s);""",
                                    params=(index.id,), fetchall=True)
        groups = flat_to_list(groups)
        index['groups'] = groups
        return index

    def add_index(self, *, name, groups):
        if self.check_index_exists(name):
            raise QueryError(f'index {name} already exists')

        with self.transaction('add_index_data') as conn:
            index_id = self.execute_query("""INSERT INTO index (name) VALUES (%s) RETURNING id;""",
                                          conn=conn, params=(name,))
            if groups:
                for group in groups:
                    self.execute_query("""INSERT INTO index_group (group_id, index_id) 
                                        VALUES ((SELECT id FROM "group" WHERE name = %s), %s);""",
                                       conn=conn, params=(group, index_id,), with_fetch=False)
        return index_id

    def update_index(self, *, index_id, name, groups=None):
        if name:
            self.execute_query("""UPDATE index SET name = %s WHERE id = %s;""", params=(name, index_id),
                               with_commit=True, with_fetch=False)
        if isinstance(groups, list):
            current_groups = self.execute_query("""SELECT name FROM "group" WHERE id IN 
                                                (SELECT group_id FROM index_group WHERE index_id = %s);""",
                                                params=(index_id,), fetchall=True)
            current_groups = flat_to_set(current_groups)
            target_groups = set(groups)

            groups_for_add = target_groups - current_groups
            groups_for_delete = tuple(current_groups - target_groups)

            with self.transaction('update_index_groups') as conn:
                for group in groups_for_add:
                    self.execute_query("""INSERT INTO index_group (group_id, index_id) 
                                        VALUES ((SELECT id FROM "group" WHERE name = %s), %s);""",
                                       conn=conn, params=(group, index_id,), with_fetch=False)
                if groups_for_delete:
                    self.execute_query("""DELETE FROM index_group  WHERE group_id IN 
                                        (SELECT id FROM "group" WHERE name IN %s) AND index_id = %s;""",
                                       conn=conn, params=(groups_for_delete, index_id), with_fetch=False)
        return index_id

    def delete_index(self, index_id):
        self.execute_query("""DELETE FROM index WHERE id = %s;""", params=(index_id,),
                           with_commit=True, with_fetch=False)
        return index_id

    # __PERMISSIONS__ ###############################################################

    def check_permission_exists(self, permission_name):
        permission_id = self.execute_query("""SELECT id from permission WHERE name = %s;""",
                                           params=(permission_name,))
        return permission_id

    def get_permissions_data(self, *, user_id=None, names_only=False):
        if user_id:
            user_roles = self.get_roles_data(user_id=user_id, names_only=True)
            permissions = list()
            for role in user_roles:
                role_id = self.execute_query("""SELECT id FROM role WHERE name = %s;""", params=(role,))
                role_permissions = self.execute_query("""SELECT * FROM permission WHERE id IN 
                                                    (SELECT permission_id FROM role_permission WHERE role_id = %s);""",
                                                      params=(role_id,), fetchall=True, as_obj=True)
                permissions.extend(role_permissions)
            permissions = list({v['id']: v for v in permissions}.values())
        else:
            permissions = self.execute_query("""SELECT * FROM permission;""", fetchall=True, as_obj=True)
        if names_only:
            permissions = [p['name'] for p in permissions]
        else:
            for permission in permissions:
                roles = self.execute_query("""SELECT name FROM role WHERE id IN 
                                          (SELECT role_id FROM role_permission WHERE permission_id = %s);""",
                                           params=(permission.id,), fetchall=True)
                roles = flat_to_list(roles)
                permission['roles'] = roles
        return permissions

    def get_permission_data(self, permission_id):
        permission = self.execute_query("""SELECT * FROM permission WHERE id = %s;""",
                                        params=(permission_id,), as_obj=True)
        roles = self.execute_query("""SELECT name FROM role WHERE id IN 
                                    (SELECT role_id FROM role_permission WHERE permission_id = %s);""",
                                   params=(permission.id,), fetchall=True)
        roles = flat_to_list(roles)
        permission['roles'] = roles
        return permission

    def add_permission(self, *, name, roles):
        if self.check_permission_exists(name):
            raise QueryError(f'group {name} already exists')

        with self.transaction('add_permission_data') as conn:
            permission_id = self.execute_query("""INSERT INTO permission (name) VALUES (%s) RETURNING id;""",
                                               conn=conn, params=(name,))
            if roles:
                for role in roles:
                    self.execute_query("""INSERT INTO role_permission (role_id, permission_id) 
                                        VALUES ((SELECT id FROM role WHERE name = %s), %s);""",
                                       conn=conn, params=(role, permission_id,), with_fetch=False)
        return permission_id

    def update_permission(self, *, permission_id, name, roles=None):
        if name:
            self.execute_query("""UPDATE permission SET name = %s WHERE id = %s;""",
                               params=(name, permission_id), with_commit=True, with_fetch=False)

        if isinstance(roles, list):
            current_roles = self.execute_query("""SELECT name FROM role WHERE id IN 
                                                (SELECT role_id FROM role_permission WHERE permission_id = %s);""",
                                               params=(permission_id,), fetchall=True)
            current_roles = flat_to_set(current_roles)
            target_roles = set(roles)

            roles_for_add = target_roles - current_roles
            roles_for_delete = tuple(current_roles - target_roles)

            with self.transaction('update_group_users') as conn:
                for role in roles_for_add:
                    self.execute_query("""INSERT INTO role_permission (role_id, permission_id) 
                                        VALUES ((SELECT id FROM role WHERE name = %s), %s);""",
                                       conn=conn, params=(role, permission_id,), with_fetch=False)
                if roles_for_delete:
                    self.execute_query("""DELETE FROM role_permission WHERE role_id IN 
                                        (SELECT id FROM role WHERE name IN %s) AND permission_id = %s;""",
                                       conn=conn, params=(roles_for_delete, permission_id), with_fetch=False)
        return permission_id

    def delete_permission(self, permission_id):
        self.execute_query("""DELETE FROM permission WHERE id = %s;""", params=(permission_id,),
                           with_commit=True, with_fetch=False)
        return permission_id

    # __DASHBOARDS__ ###############################################################

    def check_dash_exists(self, dash_id):
        dash = self.execute_query("""SELECT name FROM dash WHERE id = %s;""", params=(dash_id,), as_obj=True)
        return dash.name

    def get_dashs_data(self, *, group_id=None, names_only=False):
        if group_id:
            dashs = self.execute_query("""SELECT * FROM dash WHERE id IN
                                        (SELECT dash_id FROM dash_group WHERE group_id = %s);""",
                                       params=(group_id,), fetchall=True, as_obj=True)
        else:
            dashs = self.execute_query("""SELECT * FROM dash;""", fetchall=True, as_obj=True)

        if names_only:
            dashs = [d['name'] for d in dashs]
        else:
            for dash in dashs:
                groups = self.execute_query("""SELECT name FROM "group" WHERE id IN 
                                            (SELECT group_id FROM dash_group WHERE dash_id = %s);""",
                                            params=(dash.id,), fetchall=True, as_obj=True)
                groups = list({v['name']: v for v in groups}.values())
                dash['groups'] = groups
        return dashs

    def get_dash_data(self, dash_id):
        dash_data = self.execute_query("""SELECT * FROM dash WHERE id = %s;""",
                                       params=(dash_id,), as_obj=True)
        if not dash_data:
            raise ValueError(f'Dash with id={dash_id} is not exists')

        groups = self.execute_query("""SELECT name FROM "group" WHERE id IN 
                                    (SELECT group_id FROM dash_group WHERE dash_id = %s);""",
                                    params=(dash_id,), fetchall=True)
        groups = flat_to_list(groups)
        dash_data['groups'] = groups
        return dash_data

    def add_dash(self, *, name, body, groups=None):
        dash_id = self.check_dash_exists(dash_name=name)
        if dash_id:
            raise QueryError(f'dash with name={name} is already exists')

        with self.transaction('add_dashboard_data') as conn:
            dash = self.execute_query("""INSERT INTO dash (name, body) VALUES (%s, %s) RETURNING id;""",
                                      conn=conn, params=(name, body,), as_obj=True)
            if isinstance(groups, list):
                for group in groups:
                    self.execute_query("""INSERT INTO dash_group (group_id, dash_id) 
                                        VALUES ((SELECT id FROM "group" WHERE name = %s), %s);""",
                                       conn=conn, params=(group, dash.id,), with_fetch=False)
        return dash.id

    def update_dash(self, *, dash_id, name, body, groups=None):
        dash_name = self.check_dash_exists(dash_id)
        if not dash_name:
            raise QueryError(f'dash with id={dash_id} is not exists')

        with self.transaction('update_dash_data') as conn:
            if name:
                self.execute_query("""UPDATE dash SET name = %s WHERE id = %s;""",
                                   conn=conn, params=(name, dash_id), with_fetch=False)
            if body:
                self.execute_query("""UPDATE dash SET body = %s WHERE id = %s;""",
                                   conn=conn, params=(body, dash_id), with_fetch=False)

        if isinstance(groups, list):
            current_groups = self.execute_query("""SELECT name FROM "group" WHERE id IN 
                                                (SELECT group_id FROM dash_group WHERE dash_id = %s);""",
                                                params=(dash_id,), fetchall=True)
            current_groups = flat_to_set(current_groups)
            target_groups = set(groups)

            groups_for_add = target_groups - current_groups
            groups_for_delete = tuple(current_groups - target_groups)

            with self.transaction('update_dash_groups') as conn:
                for group in groups_for_add:
                    self.execute_query("""INSERT INTO dash_group (group_id, dash_id) 
                                        VALUES ((SELECT id FROM "group" WHERE name = %s), %s);""",
                                       conn=conn, params=(group, dash_id,), with_fetch=False)
                if groups_for_delete:
                    self.execute_query("""DELETE FROM dash_group  WHERE group_id IN 
                                        (SELECT id FROM "group" WHERE name IN %s) AND dash_id = %s;""",
                                       conn=conn, params=(groups_for_delete, dash_id), with_fetch=False)
        return dash_name

    def delete_dash(self, dash_id):
        self.execute_query("""DELETE FROM dash WHERE id = %s;""",
                           params=(dash_id,), with_commit=True, with_fetch=False)
        return dash_id

    # __QUIZS__ ###############################################################

    def check_quiz_exists(self, quiz_name):
        quiz_id = self.execute_query("""SELECT id FROM quiz WHERE name = %s;""", params=(quiz_name,))
        return quiz_id

    def get_quizs_data(self):
        quizs_data = self.execute_query("""SELECT * FROM quiz;""", fetchall=True, as_obj=True)
        return quizs_data

    def add_quiz(self, *, name, questions):
        if self.check_quiz_exists(quiz_name=name):
            raise QueryError(f'quiz {name} already exists')

        with self.transaction('create_quiz_data') as conn:
            quiz_id = self.execute_query("""INSERT INTO quiz (name) values (%s) RETURNING id;""",
                                         conn=conn, params=(name,))
            if isinstance(questions, list):
                for question in questions:
                    question_id = self.execute_query(
                        """INSERT INTO question (text, type) VALUES (%s, %s) RETURNING id;""",
                        conn=conn, params=(question['text'], question['type']), )
                    self.execute_query("""INSERT INTO quiz_question (quiz_id, question_id, sid) VALUES
                                        (%s, %s, %s);""", conn=conn, params=(quiz_id, question_id, question['sid'],),
                                       with_fetch=False)

        return quiz_id

    def get_quiz_data(self, quiz_id):
        quiz_data = self.execute_query("""SELECT * FROM quiz WHERE id = %s;""", params=(quiz_id,), as_obj=True)
        if not quiz_data:
            raise QueryError(f'quiz with id={quiz_id} not exists')
        questions_data = self.execute_query("""SELECT text, type, sid FROM question, quiz_question 
                                            WHERE id IN (SELECT question_id FROM quiz_question WHERE quiz_id = %s);""",
                                            params=(quiz_id,), fetchall=True, as_obj=True)
        quiz_data['questions'] = questions_data
        return quiz_data

    def delete_quiz(self, quiz_id):
        self.execute_query("""DELETE FROM quiz WHERE id = %s;""",
                           params=(quiz_id,), with_commit=True, with_fetch=False)
        return quiz_id
