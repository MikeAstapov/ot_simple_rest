import logging

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


class PostgresConnector:
    def __init__(self, conn_pool):
        self.pool = conn_pool
        self.logger = logging.getLogger('osr')

    def row_to_obj(self, row, cur):
        """Convert a SQL row to an object supporting dict and attribute access."""
        obj = tornado.util.ObjectDict()
        for val, desc in zip(row, cur.description):
            obj[desc.name] = val
        return obj

    def execute_query(self, query, params=None, with_commit=False,
                      with_fetch=True, as_obj=False, fetchall=False):
        fetch_result = None
        conn = self.pool.getconn()
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
                    if as_obj:
                        fetch_result = self.row_to_obj(fetch_result, cur)
            if with_commit:
                conn.commit()
        except psycopg2.OperationalError as err:
            self.logger.error(f'SQL Error: {err}')
        else:
            return fetch_result
        finally:
            self.pool.putconn(conn)

    #######################################################################

    def check_permission(self, *, role_id, permission):
        permission_ids = self.execute_query("""SELECT permission_id FROM role_permission WHERE role_id = %s 
                                               AND value = True;""", (role_id,), fetchall=True)
        permissions = self.execute_query("""SELECT name FROM permission WHERE id IN %s;""",
                                         tuple(permission_ids), fetchall=True)
        permissions = [p[0] for p in permissions] if permissions else []

        return permission in permissions or 'admin_all' in permissions

    def get_users(self, token):
        user_id = self.execute_query("""SELECT user_id FROM session WHERE token = %s;""", (token,))
        role_id = self.execute_query("""SELECT role_id FROM user_role WHERE user_id = %s;""", (user_id,))
        if self.check_permission(role_id=role_id, permission='list_users'):
            users_list = self.execute_query("""SELECT * FROM "user";""", fetchall=True)
        else:
            users_list = self.execute_query("""SELECT * FROM "user" WHERE id = %s;""", (user_id,), fetchall=True)
        return users_list

    def check_user_exists(self, username):
        user_id = self.execute_query("""SELECT id FROM "user" WHERE username = %s;""", (username,))
        return user_id

    def get_user_info(self, token):
        user_id = self.execute_query("""SELECT user_id FROM session WHERE token = %s;""", (token,))
        user_info = self.execute_query("""SELECT * FROM "user" WHERE id = %s;""", (user_id,))
        return user_info

    def create_update_user(self, *, token, username, password, groups, roles):
        user_id = self.execute_query("""SELECT user_id FROM session WHERE token = %s;""", (token,))
        role_id = self.execute_query("""SELECT role_id FROM user_role WHERE user_id = %s;""", (user_id,))

        if self.check_user_exists(username):
            # Change user data
            if not self.check_permission(role_id=role_id, permission='manage_users'):
                raise AccessDeniedError('no permission for edit user')
        else:
            # Create user
            if not self.check_permission(role_id=role_id, permission='create_users'):
                raise AccessDeniedError('no permission for create user')

        # TODO: Make with transaction
        user_id = self.execute_query("""INSERT INTO "user" (username, password) VALUES (%s, %s) ON CONFLICT (username) 
                                        DO UPDATE SET username = "user".username, password = %s RETURNING id;""",
                                     (username, password,), with_commit=True)

        current_groups = self.execute_query("""SELECT group_id FROM user_group WHERE user_id = %s;""",
                                            (user_id,), fetchall=True)
        current_groups = {g[0] for g in current_groups} if current_groups else {}
        target_groups = set(groups)

        groups_for_add = target_groups - current_groups
        groups_for_delete = current_groups - target_groups

        for group in groups_for_add:
            group_id = self.execute_query("""SELECT id FROM group WHERE name = %s;""", (group,))
            self.execute_query("""INSERT INTO user_group (user_id, group_id) VALUES (%s, %s);""",
                               (user_id, group_id,), with_commit=True)
        for group in groups_for_delete:
            group_id = self.execute_query("""SELECT id FROM group WHERE name = %s;""", (group,))
            self.execute_query("""DELETE FROM user_group WHERE group_id = %s;""", (group_id,), with_commit=True)

        current_roles = self.execute_query("""SELECT role_id FROM user_role WHERE user_id = %s;""",
                                           (user_id,), fetchall=True)
        current_roles = {g[0] for g in current_roles} if current_roles else {}
        target_roles = set(roles)

        roles_for_add = target_roles - current_roles
        roles_for_delete = current_roles - target_roles

        for role in roles_for_add:
            role_id = self.execute_query("""SELECT id FROM role WHERE name = %s;""", (role,))
            self.execute_query("""INSERT INTO user_role (user_id, role_id) VALUES (%s, %s);""",
                               (user_id, role_id,), with_commit=True)
        for role in roles_for_delete:
            role_id = self.execute_query("""SELECT id FROM role WHERE name = %s;""", (role,))
            self.execute_query("""DELETE FROM user_role WHERE role_id = %s;""", (role_id,), with_commit=True)

    def delete_user(self, *, token, target_user_id):
        user_id = self.execute_query("""SELECT user_id FROM session WHERE token = %s;""", (token,))
        role_id = self.execute_query("""SELECT role_id FROM user_role WHERE user_id = %s;""", (user_id,))
        if self.check_permission(role_id=role_id, permission='delete_users'):
            self.execute_query("""DELETE FROM "user" WHERE id = %s;""", (target_user_id,), with_commit=True)
        else:
            raise AccessDeniedError('no permission for delete user')

    #####################################################################

    def get_roles(self, token):
        user_id = self.execute_query("""SELECT user_id FROM session WHERE token = %s;""", (token,))
        role_id = self.execute_query("""SELECT role_id FROM user_role WHERE user_id = %s;""", (user_id,))
        if self.check_permission(role_id=role_id, permission='list_roles'):
            roles_list = self.execute_query("""SELECT * FROM role;""", fetchall=True)
        else:
            roles_list = self.execute_query("""SELECT * FROM role WHERE id = %s;""", (role_id,), fetchall=True)
        return roles_list

    def get_permissions(self, *, token, target_role_id):
        user_id = self.execute_query("""SELECT user_id FROM session WHERE token = %s;""", (token,))
        role_id = self.execute_query("""SELECT role_id FROM user_role WHERE user_id = %s;""", (user_id,))
        if self.check_permission(role_id=role_id, permission='list_permissions'):
            permissions_list = self.execute_query("""SELECT * FROM permissions;""", fetchall=True)
        else:
            permissions_list = self.execute_query("""SELECT permission_id FROM role_permissions WHERE role_id = %s;""",
                                                  (target_role_id,), fetchall=True)
        return permissions_list

    def check_role_exists(self, role_name):
        user_id = self.execute_query("""SELECT user_id FROM session WHERE username = %s;""", (role_name,))
        return user_id if user_id else False

    def create_update_role(self, *, token, role_name, users, permissions):
        user_id = self.execute_query("""SELECT user_id FROM session WHERE token = %s;""", (token,))
        role_id = self.execute_query("""SELECT role_id FROM user_role WHERE user_id = %s;""", (user_id,))

        if self.check_role_exists(role_name):
            # Change role data
            if not self.check_permission(role_id=role_id, permission='manage_roles'):
                raise AccessDeniedError('no permission for edit role')
        else:
            # Create role
            if not self.check_permission(role_id=role_id, permission='create_roles'):
                raise AccessDeniedError('no permission for create role')

        # TODO: Make with transaction
        role_id = self.execute_query("""INSERT INTO role (name) VALUES (%s) ON CONFLICT (name) 
                                        DO UPDATE SET name = role.name RETURNING id;""",
                                     (role_name,), with_commit=True)
        current_users = self.execute_query("""SELECT user_id FROM user_role WHERE role_id = %s;""",
                                           (role_id,), fetchall=True)
        current_users = {u[0] for u in current_users} if current_users else {}
        target_users = set(users)

        users_for_add = target_users - current_users
        users_for_delete = current_users - target_users

        for user in users_for_add:
            user_id = self.execute_query("""SELECT id FROM "user" WHERE username = %s;""", (user,))
            self.execute_query("""INSERT INTO user_role (user_id, role_id) VALUES (%s, %s);""",
                               (user_id, role_id,), with_commit=True)
        for user in users_for_delete:
            user_id = self.execute_query("""SELECT id FROM "user" WHERE username = %s;""", (user,))
            self.execute_query("""DELETE FROM user_role WHERE user_id = %s;""", (user_id,), with_commit=True)

        current_permissions = self.execute_query("""SELECT permission_id FROM role_permission WHERE role_id = %s;""",
                                                 (role_id,), fetchall=True)
        current_permissions = {p[0] for p in current_permissions} if current_permissions else {}
        target_permissions = set(permissions)

        permissions_for_add = target_permissions - current_permissions
        permissions_for_delete = current_permissions - target_permissions

        for permission in permissions_for_add:
            permission_id = self.execute_query("""SELECT id FROM permission WHERE name = %s;""", (permission,))
            self.execute_query("""INSERT INTO role_permission (permission_id, role_id) VALUES (%s, %s);""",
                               (permission_id, role_id,), with_commit=True)
        for permission in permissions_for_delete:
            permission_id = self.execute_query("""SELECT id FROM permission WHERE name = %s;""", (permission,))
            self.execute_query("""DELETE FROM role_permission WHERE permission_id = %s;""",
                               (permission_id,), with_commit=True)

###################################################################

    def get_groups(self, token):
        user_id = self.execute_query("""SELECT user_id FROM session WHERE token = %s;""", (token,))
        role_id = self.execute_query("""SELECT role_id FROM user_role WHERE user_id = %s;""", (user_id,))
        if self.check_permission(role_id=role_id, permission='list_groups'):
            groups_list = self.execute_query("""SELECT * FROM "group";""", fetchall=True)
        else:
            groups_list = self.execute_query("""SELECT * FROM role WHERE id = %s;""", (role_id,), fetchall=True)
        return groups_list
