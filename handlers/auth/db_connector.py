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


class AccessDeniedError(Exception):
    pass


class QueryError(Exception):
    pass


def flat_to_set(arr):
    return {i[0] for i in arr} if arr else set()


def flat_to_list(arr):
    return [i[0] for i in arr] if arr else []


class PostgresConnector:
    def __init__(self, conn_pool):
        self.pool = conn_pool
        self.logger = logging.getLogger('osr')

    def get_column_names(self, table_name):
        conn = self.pool.getconn()
        cur = conn.cursor()
        cur.execute('SELECT * FROM "%s" LIMIT 0;' % table_name)
        columns = [desc[0] for desc in cur.description]
        cur.close()
        self.pool.putconn(conn)
        return columns

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
            self.logger.error("Transaction {} error: {}".format(name, e))
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

    def check_user_exists(self, username):
        user = self.execute_query("""SELECT * FROM "user" WHERE username = %s;""",
                                  params=(username,), as_obj=True)
        return user

    def get_user_permissions(self, user_id):
        roles = self.get_roles_data(user_id=user_id, names_only=True)
        permissions = list()
        for role in roles:
            role = self.execute_query("""SELECT id FROM role WHERE name = %s;""", params=(role,), as_obj=True)
            role_permissions = self.execute_query("""SELECT name FROM permission WHERE id IN 
                                                 (SELECT permission_id FROM role_permission WHERE role_id = %s);""",
                                                  params=(role.id,), fetchall=True)
            permissions.extend(list(role_permissions))
        permissions = flat_to_list(permissions)
        return permissions

    def get_user_tokens(self, user_id):
        tokens = self.execute_query("""SELECT token FROM session WHERE user_id = %s;""",
                                    params=(user_id,), fetchall=True)
        tokens = flat_to_set(tokens)
        return tokens

    def get_user_roles(self, user_id):
        roles = self.execute_query("""SELECT name FROM role WHERE id IN (SELECT role_id FROM user_role 
                                    WHERE user_id = %s);""", params=(user_id,), fetchall=True)
        roles = [p[0] for p in roles] if roles else []
        return roles

    def get_auth_data(self, user_id):
        login_pass = self.execute_query("""SELECT username, password FROM "user" WHERE id = %s;""",
                                        params=(user_id,), as_obj=True)
        return login_pass

    def add_session(self, *, user_id, token, expired_date):
        self.execute_query("""INSERT INTO session (token, user_id, expired_date) VALUES (%s, %s, %s);""",
                           params=(token, user_id, expired_date), with_commit=True, with_fetch=False)

    # __USERS__ #######################################################################

    def get_users_data(self, names_only=False):
        users = self.execute_query("""SELECT id, username FROM "user";""", fetchall=True, as_obj=True)
        if names_only:
            users = [u['username'] for u in users]
        else:
            for user in users:
                user_roles = self.execute_query("""SELECT name FROM role WHERE id IN 
                                                (SELECT role_id FROM user_role WHERE user_id = %s);""",
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
        user_data = self.execute_query("""SELECT id, username FROM "user" WHERE id = %s;""",
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

    def add_user(self, *, username, password, roles, groups):
        if self.check_user_exists(username):
            raise QueryError(f'user {username} already exists')

        with self.transaction('add_user_data') as conn:
            user_id = self.execute_query("""INSERT INTO "user" (username, password) VALUES (%s, %s) RETURNING id;""",
                                         conn=conn, params=(username, password))
            for role in roles:
                self.execute_query("""INSERT INTO user_role (role_id, user_id) 
                                    VALUES ((SELECT id FROM role WHERE name = %s), %s);""",
                                   conn=conn, params=(role, user_id), with_fetch=False)
            for group in groups:
                self.execute_query("""INSERT INTO user_group (group_id, user_id) 
                                    VALUES ((SELECT id FROM "group" WHERE name = %s), %s);""",
                                   conn=conn, params=(group, user_id), with_fetch=False)
            return user_id

    def update_user(self, *, user_id, username, password, roles, groups):
        if username:
            self.execute_query("""UPDATE "user" SET username = %s WHERE id = %s;""", params=(username, user_id),
                               with_commit=True, with_fetch=False)
        if password:
            self.execute_query("""UPDATE "user" SET password = %s WHERE id = %s;""", params=(password, user_id),
                               with_commit=True, with_fetch=False)

        if roles:
            current_roles = self.execute_query("""SELECT name FROM role WHERE id IN 
                                                (SELECT role_id FROM user_role WHERE user_id = %s);""",
                                               params=(user_id,), fetchall=True)
            current_roles = flat_to_set(current_roles)
            target_roles = set(roles)

            roles_for_add = target_roles - current_roles
            roles_for_delete = current_roles - target_roles

            with self.transaction('update_user_roles') as conn:
                for role in roles_for_add:
                    self.execute_query("""INSERT INTO user_role (role_id, user_id) 
                                        VALUES ((SELECT id FROM role WHERE name = %s), %s);""",
                                       conn=conn, params=(role, user_id,), with_commit=True, with_fetch=False)
                for role in roles_for_delete:
                    self.execute_query("""DELETE FROM user_role WHERE role_id = (SELECT id FROM role WHERE name = %s) 
                                        AND user_id = %s;""",
                                       conn=conn, params=(role, user_id,), with_commit=True, with_fetch=False)

        if groups:
            current_groups = self.execute_query("""SELECT name FROM "group" WHERE id IN 
                                                    (SELECT group_id FROM user_group WHERE user_id = %s);""",
                                                params=(user_id,), fetchall=True)
            current_groups = flat_to_set(current_groups)
            target_groups = set(groups)

            groups_for_add = target_groups - current_groups
            groups_for_delete = current_groups - target_groups

            with self.transaction('update_user_groups') as conn:
                for group in groups_for_add:
                    group_id = self.execute_query("""SELECT id FROM "group" WHERE name = %s;""", conn=conn, params=(group,))
                    self.execute_query("""INSERT INTO user_group (user_id, group_id) VALUES (%s, %s);""",
                                       conn=conn, params=(user_id, group_id,), with_commit=True, with_fetch=False)
                for group in groups_for_delete:
                    group_id = self.execute_query("""SELECT id FROM "group" WHERE name = %s;""", conn=conn, params=(group,))
                    self.execute_query("""DELETE FROM user_group WHERE user_id = %s AND group_id = %s;""",
                                       conn=conn, params=(user_id, group_id,), with_commit=True, with_fetch=False)
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
                users = self.execute_query("""SELECT username FROM "user" WHERE id in 
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
        users = self.execute_query("""SELECT username FROM "user" WHERE id in 
                                (SELECT user_id FROM user_role WHERE role_id = %s)""",
                                   params=(role.id,), fetchall=True)
        users = flat_to_list(users)
        role['users'] = users
        return role

    def add_role(self, *, role_name, users, permissions):
        if self.check_role_exists(role_name):
            raise QueryError(f'role {role_name} already exists')
        with self.transaction('add_role_data') as conn:
            role_id = self.execute_query("""INSERT INTO role (name) VALUES (%s) RETURNING id;""",
                                         conn=conn, params=(role_name,))
            for user in users:
                user_id = self.execute_query("""SELECT id FROM "user" WHERE username = %s;""", params=(user,))
                if not user_id:
                    raise QueryError(f'user {user} not exists')
                self.execute_query("""INSERT INTO user_role (user_id, role_id) VALUES (%s, %s);""",
                                   conn=conn, params=(user_id, role_id,), with_fetch=False)
            for permission in permissions:
                permission_id = self.execute_query("""SELECT id FROM permission WHERE name = %s;""",
                                                   params=(permission,))
                if not permission_id:
                    raise QueryError(f'permission {permission_id} not exists')
                self.execute_query("""INSERT INTO role_permission (permission_id, role_id) VALUES (%s, %s);""",
                                   conn=conn, params=(permission_id, role_id,), with_fetch=False)
        return role_id

    def update_role(self, *, role_id, role_name, users, permissions):
        if role_name:
            self.execute_query("""UPDATE role SET name = %s WHERE id = %s;""", params=(role_name, role_id),
                               with_commit=True, with_fetch=False)
        if users:
            current_users = self.execute_query("""SELECT username FROM "user" WHERE id IN 
                                                (SELECT user_id FROM user_role WHERE role_id = %s);""",
                                               params=(role_id,), fetchall=True)
            current_users = flat_to_set(current_users)
            target_users = set(users)

            users_for_add = target_users - current_users
            users_for_delete = current_users - target_users

            with self.transaction('update_role_users') as conn:

                for user in users_for_add:
                    user_id = self.execute_query("""SELECT id FROM "user" WHERE username = %s;""",
                                                 conn=conn, params=(user,))
                    self.execute_query("""INSERT INTO user_role (user_id, role_id) VALUES (%s, %s);""",
                                       conn=conn, params=(user_id, role_id,), with_fetch=False)
                for user in users_for_delete:
                    user_id = self.execute_query("""SELECT id FROM "user" WHERE username = %s;""",
                                                 conn=conn, params=(user,))
                    self.execute_query("""DELETE FROM user_role WHERE user_id = %s AND role_id = %s;""",
                                       conn=conn, params=(user_id, role_id), with_fetch=False)

        if permissions:
            current_permissions = self.execute_query("""SELECT name FROM permission WHERE id IN 
                                                    (SELECT permission_id FROM role_permission WHERE role_id = %s);""",
                                                     params=(role_id,), fetchall=True)
            current_permissions = flat_to_set(current_permissions)
            target_permissions = set(permissions)

            permissions_for_add = target_permissions - current_permissions
            permissions_for_delete = current_permissions - target_permissions

            with self.transaction('update_role_permissions') as conn:

                for permission in permissions_for_add:
                    permission_id = self.execute_query("""SELECT id FROM permission WHERE name = %s;""",
                                                       conn=conn, params=(permission,))
                    self.execute_query("""INSERT INTO role_permission (permission_id, role_id) VALUES (%s, %s);""",
                                       conn=conn, params=(permission_id, role_id,), with_fetch=False)
                for permission in permissions_for_delete:
                    permission_id = self.execute_query("""SELECT id FROM permission WHERE name = %s;""",
                                                       conn=conn, params=(permission,))
                    self.execute_query("""DELETE FROM role_permission WHERE permission_id = %s AND role_id = %s;""",
                                       conn=conn, params=(permission_id, role_id), with_fetch=False)
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

    def get_groups_data(self, user_id=None, names_only=False):
        if user_id:
            groups = self.execute_query("""SELECT * FROM "group" WHERE id IN 
                                        (SELECT group_id FROM user_group WHERE user_id = %s);""",
                                        fetchall=True, params=(user_id,), as_obj=True)
        else:
            groups = self.execute_query("""SELECT * FROM "group";""", fetchall=True, as_obj=True)

        if names_only:
            groups = [g['name'] for g in groups]
        else:
            for group in groups:
                users = self.execute_query("""SELECT username FROM "user" WHERE id IN 
                                            (SELECT user_id FROM user_group WHERE group_id = %s);""",
                                           params=(group.id,), fetchall=True)
                users = flat_to_list(users)
                group['users'] = users

                indexes = self.execute_query("""SELECT name FROM index WHERE id IN 
                                            (SELECT index_id FROM index_group WHERE group_id = %s);""",
                                             params=(group.id,), fetchall=True)
                indexes = flat_to_list(indexes)
                group['indexes'] = indexes
        return groups

    def get_group_data(self, group_id):
        group = self.execute_query("""SELECT * FROM "group" WHERE id = %s;""",
                                   params=(group_id,), as_obj=True)
        users = self.execute_query("""SELECT username FROM "user" WHERE id IN 
                                    (SELECT user_id FROM user_group WHERE group_id = %s);""",
                                   params=(group.id,), fetchall=True)
        users = flat_to_list(users)
        group['users'] = users

        indexes = self.execute_query("""SELECT name FROM index WHERE id IN 
                                    (SELECT index_id FROM index_group WHERE group_id = %s);""",
                                     params=(group.id,), fetchall=True)
        indexes = flat_to_list(indexes)
        group['indexes'] = indexes
        return group

    def add_group(self, *, group_name, color, users, indexes):
        if self.check_group_exists(group_name):
            raise QueryError(f'group {group_name} already exists')

        with self.transaction('add_group_data') as conn:
            group_id = self.execute_query("""INSERT INTO "group" (name, color) VALUES (%s, %s) RETURNING id;""",
                                          conn=conn, params=(group_name, color))
            for user in users:
                self.execute_query("""INSERT INTO user_group (user_id, group_id) 
                                    VALUES ((SELECT id FROM "user" WHERE username = %s), %s);""",
                                   conn=conn, params=(user, group_id,), with_fetch=False)
            for index in indexes:
                self.execute_query("""INSERT INTO index_group (index_id, group_id) 
                                    VALUES ((SELECT id FROM index WHERE name = %s), %s);""",
                                   conn=conn, params=(index, group_id,), with_fetch=False)
            return group_id

    def update_group(self, group_id, name, color, users, indexes):
        if name:
            self.execute_query("""UPDATE "group" SET name = %s WHERE id = %s;""",
                               params=(name, group_id), with_commit=True, with_fetch=False)
        if color:
            self.execute_query("""UPDATE "group" SET color = %s WHERE id = %s;""",
                               params=(color, group_id), with_commit=True, with_fetch=False)

        current_users = self.execute_query("""SELECT username FROM "user" WHERE id IN 
                                            (SELECT user_id FROM user_group WHERE group_id = %s);""",
                                           params=(group_id,), fetchall=True)
        current_users = flat_to_set(current_users)
        target_users = set(users)

        users_for_add = target_users - current_users
        users_for_delete = current_users - target_users

        with self.transaction('update_group_users') as conn:
            for user in users_for_add:
                self.execute_query("""INSERT INTO user_group (user_id, group_id) 
                                    VALUES ((SELECT id FROM "user" WHERE username = %s), %s);""",
                                   conn=conn, params=(user, group_id,), with_commit=True, with_fetch=False)
            for user in users_for_delete:
                self.execute_query("""DELETE FROM user_group
                                    WHERE user_id = (SELECT id FROM "user" WHERE username = %s) AND group_id = %s;""",
                                   conn=conn, params=(user, group_id), with_commit=True)

        current_indexes = self.execute_query("""SELECT name FROM index WHERE id IN 
                                            (SELECT index_id FROM index_group WHERE group_id = %s);""",
                                             params=(group_id,), fetchall=True)
        current_indexes = flat_to_set(current_indexes)
        target_indexes = set(indexes)

        indexes_for_add = target_indexes - current_indexes
        indexes_for_delete = current_indexes - target_indexes

        with self.transaction('update_group_indexes') as conn:
            for index in indexes_for_add:
                self.execute_query("""INSERT INTO index_group (index_id, group_id) 
                                    VALUES ((SELECT id FROM index WHERE name = %s), %s);""",
                                   conn=conn, params=(index, group_id,), with_commit=True, with_fetch=False)
            for index in indexes_for_delete:
                self.execute_query("""DELETE FROM index_group WHERE index_id = (SELECT id FROM index WHERE name = %s)
                                    AND group_id = %s;""",
                                   conn=conn, params=(index, group_id), with_commit=True)
        return group_id

    def delete_group(self, group_id):
        self.execute_query("""DELETE FROM "group" WHERE id = %s;""", params=(group_id,),
                           with_commit=True, with_fetch=False)
        return group_id

    # __INDEXES___ #################################################################

    def get_indexes_list(self):
        pass

    def add_index(self):
        pass

    def update_index(self):
        pass

    def delete_index(self):
        pass

    # __PERMISSIONS__ ###############################################################

    def get_permissions_data(self, user_id=None, names_only=False):
        if user_id:
            user_roles = self.get_roles_data(user_id=user_id, names_only=True)
            permissions = list()
            for role in user_roles:
                role_id = self.execute_query("""SELECT id FROM role WHERE name = %s;""", params=(role,))
                role_permissions = self.execute_query("""SELECT * FROM permission WHERE id IN 
                                                    (SELECT permission_id FROM role_permission WHERE role_id = %s);""",
                                                      params=(role_id,), fetchall=True, as_obj=True)
                permissions.extend(role_permissions)
        else:
            permissions = self.execute_query("""SELECT * FROM permission;""", fetchall=True, as_obj=True)
            if names_only:
                permissions = [p['name'] for p in permissions]
        return permissions

    def add_permission(self):
        pass
