from datetime import datetime, timedelta
import logging
import json

import bcrypt
import jwt

import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.locks
import tornado.web
import tornado.util

from handlers.auth.db_connector import PostgresConnector

SECRET_KEY = '8b62abb2-bbf6-4e0e-a7c1-2e4734bebbd9'

logger = logging.getLogger('osr')


class NoResultError(Exception):
    pass


class BaseHandler(tornado.web.RequestHandler):
    def initialize(self, db_conn_pool):
        """
        Gets config and init logger.

        :param db_conn_pool: Postgres DB connection pool object.
        :return:
        """
        self.db = PostgresConnector(db_conn_pool)
        self.permissions = None
        self.data = None

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Credentials', True)
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', '*')

    def options(self, *args, **kwargs):
        self.set_status(204)
        self.finish()

    def decode_token(self, token):
        return jwt.decode(token, SECRET_KEY, algorithms='HS256')

    def generate_token(self, payload):
        return jwt.encode(payload, SECRET_KEY, algorithm='HS256')

    async def prepare(self):
        self.data = json.loads(self.request.body) if self.request.body else dict()
        client_token = self.get_cookie('eva_token')
        if client_token:
            try:
                token_data = self.decode_token(client_token)
                user_id = token_data['user_id']
                self.permissions = self.db.get_permissions_data(user_id=user_id,
                                                                names_only=True)
            except (jwt.ExpiredSignatureError, jwt.DecodeError):
                pass
            else:
                self.current_user = user_id

        if not self.current_user:
            raise tornado.web.HTTPError(401, "unauthorized")


class AuthLoginHandler(BaseHandler):
    async def prepare(self):
        self.data = json.loads(self.request.body) if self.request.body else dict()

    async def post(self):
        user = self.db.check_user_exists(self.data.get("name"))
        if not user:
            raise tornado.web.HTTPError(400, "incorrect login")

        password_equal = await tornado.ioloop.IOLoop.current().run_in_executor(
            None,
            bcrypt.checkpw,
            tornado.escape.utf8(self.data.get("password")),
            tornado.escape.utf8(user.password),
        )
        if not password_equal:
            raise tornado.web.HTTPError(400, "incorrect password")

        user_tokens = self.db.get_user_tokens(user.id)
        client_token = self.get_cookie('eva_token')

        if not client_token:
            payload = {'user_id': user.id, 'username': user.name,
                       'exp': int((datetime.now() + timedelta(hours=12)).timestamp())}
            token = self.generate_token(payload)
            expired_date = datetime.now() + timedelta(hours=12)
            self.db.add_session(
                token=token.decode('utf-8'),
                user_id=user.id,
                expired_date=expired_date
            )

            self.current_user = user.id
            self.set_cookie('eva_token', token, expires=expired_date)
            self.write({'status': 'success'})

        elif client_token not in user_tokens:
            raise tornado.web.HTTPError(401, "unauthorized")
        else:
            self.write({'status': 'success'})


class UsersHandler(BaseHandler):
    async def get(self):
        kwargs = {}

        if 'list_users' in self.permissions or 'admin_all' in self.permissions:
            names_only = self.get_argument('names_only', None)
            if names_only:
                kwargs['names_only'] = names_only
        else:
            kwargs['user_id'] = self.current_user

        users = self.db.get_users_data(**kwargs)
        self.write({'data': users})


class UserHandler(BaseHandler):
    async def get(self):
        target_user_id = self.get_argument('id', None)
        if not target_user_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        if 'read_users' in self.permissions or 'admin_all' in self.permissions:
            user_data = self.db.get_user_data(user_id=target_user_id)
            all_roles = self.db.get_roles_data(names_only=True)
            all_groups = self.db.get_groups_data(names_only=True)
            user_data = {'data': user_data, 'roles': all_roles, 'groups': all_groups}
        elif int(target_user_id) == self.current_user:
            user_data = self.db.get_user_data(user_id=target_user_id)
            user_data = {'data': user_data}
        else:
            raise tornado.web.HTTPError(403, "no permission for read users")
        self.write(user_data)

    async def post(self):
        if 'create_users' in self.permissions or 'admin_all' in self.permissions:
            password = self.data.get("password", None)
            username = self.data.get("name", None)
            if None in [password, username]:
                raise tornado.web.HTTPError(400, "params 'name' and 'password' is required")

            hashed_password = await tornado.ioloop.IOLoop.current().run_in_executor(
                None,
                bcrypt.hashpw,
                tornado.escape.utf8(password),
                bcrypt.gensalt(),
            )
            try:
                user_id = self.db.add_user(name=username,
                                           password=tornado.escape.to_unicode(hashed_password),
                                           roles=self.data.get('roles', None),
                                           groups=self.data.get('groups', None))
                self.write({'id': user_id})
            except Exception as err:
                raise tornado.web.HTTPError(409, str(err))
        else:
            raise tornado.web.HTTPError(403, "no permission for create roles")

    async def put(self):
        target_user_id = self.data.get('id', None)
        if not target_user_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")

        if 'manage_users' in self.permissions or 'admin_all' in self.permissions:
            user_data = self.db.get_auth_data(user_id=target_user_id)
            old_password = user_data.pop("password")
            old_username = user_data.pop("name")

            new_password = self.data.get("password", None)
            if new_password:
                password_equal = await tornado.ioloop.IOLoop.current().run_in_executor(
                    None,
                    bcrypt.checkpw,
                    tornado.escape.utf8(new_password),
                    tornado.escape.utf8(old_password),
                )
                new_hashed_password = await tornado.ioloop.IOLoop.current().run_in_executor(
                    None,
                    bcrypt.hashpw,
                    tornado.escape.utf8(new_password),
                    bcrypt.gensalt(),
                )
                new_password = tornado.escape.to_unicode(new_hashed_password) if not password_equal else None

            new_username = self.data.get('name', None)
            new_username = new_username if new_username != old_username else None

            user_id = self.db.update_user(user_id=target_user_id,
                                          name=new_username,
                                          password=new_password,
                                          roles=self.data.get('roles', None),
                                          groups=self.data.get('groups', None))
        else:
            raise tornado.web.HTTPError(403, "no permission for manage users")
        self.write({'id': user_id})

    async def delete(self):
        target_user_id = self.get_argument('id', None)
        if not target_user_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")

        if 'delete_users' in self.permissions or 'admin_all' in self.permissions:
            user_id = self.db.delete_user(target_user_id)
        else:
            raise tornado.web.HTTPError(403, "no permission for delete roles")
        self.write({'id': user_id})


class RolesHandler(BaseHandler):
    async def get(self):
        kwargs = {}

        if 'list_roles' in self.permissions or 'admin_all' in self.permissions:
            target_user_id = self.get_argument('id', None)
            if target_user_id:
                kwargs['user_id'] = target_user_id
            names_only = self.get_argument('names_only', None)
            if names_only:
                kwargs['names_only'] = names_only
        else:
            kwargs['user_id'] = self.current_user

        roles = self.db.get_roles_data(**kwargs)
        self.write({'data': roles})


class RoleHandler(BaseHandler):
    async def get(self):
        role_id = self.get_argument('id', None)
        if not role_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        if 'read_roles' in self.permissions or 'admin_all' in self.permissions:
            role_data = self.db.get_role_data(role_id)
            all_users = self.db.get_users_data(names_only=True)
            all_permissions = self.db.get_permissions_data(names_only=True)
        else:
            raise tornado.web.HTTPError(403, "no permission for read roles")
        self.write({'data': role_data, 'users': all_users, 'permissions': all_permissions})

    async def post(self):
        role_name = self.data.get('name', None)
        if not role_name:
            raise tornado.web.HTTPError(400, "param 'name' is required")
        if 'create_roles' not in self.permissions and 'admin_all' not in self.permissions:
            raise tornado.web.HTTPError(403, "no permission for create roles")

        try:
            role_id = self.db.add_role(name=role_name,
                                       users=self.data.get('users', None),
                                       permissions=self.data.get('permissions', None))
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.write({'id': role_id})

    async def put(self):
        role_id = self.data.get('id', None)
        if not role_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        if 'manage_roles' not in self.permissions and 'admin_all' not in self.permissions:
            raise tornado.web.HTTPError(403, "no permission for manage roles")

        self.db.update_role(role_id=role_id,
                            name=self.data.get('name', None),
                            users=self.data.get('users', None),
                            permissions=self.data.get('permissions', None))

        self.write({'id': role_id})

    async def delete(self):
        role_id = self.get_argument('id', None)
        if not role_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")

        if 'delete_roles' in self.permissions or 'admin_all' in self.permissions:
            role_id = self.db.delete_role(role_id)
        else:
            raise tornado.web.HTTPError(403, "no permission for delete roles")
        self.write({'id': role_id})


class GroupsHandler(BaseHandler):
    async def get(self):
        kwargs = {}

        if 'list_groups' in self.permissions or 'admin_all' in self.permissions:
            target_user_id = self.get_argument('id', None)
            if target_user_id:
                kwargs['user_id'] = target_user_id
            names_only = self.get_argument('names_only', None)
            if names_only:
                kwargs['names_only'] = names_only
        else:
            kwargs['user_id'] = self.current_user

        groups = self.db.get_groups_data(**kwargs)
        self.write({'data': groups})


class GroupHandler(BaseHandler):
    async def get(self):
        group_id = self.get_argument('id', None)
        if not group_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        if 'read_groups' not in self.permissions and 'admin_all' not in self.permissions:
            raise tornado.web.HTTPError(403, "no permission for read groups")

        group_data = self.db.get_group_data(group_id)
        all_users = self.db.get_users_data(names_only=True)
        all_indexes = self.db.get_indexes_data(names_only=True)
        self.write({'data': group_data, 'users': all_users, 'indexes': all_indexes})

    async def post(self):
        group_name = self.data.get('name', None)
        color = self.data.get('color', None)
        if None in [group_name, color]:
            raise tornado.web.HTTPError(400, "params 'name' and 'color' is required")
        if 'create_groups' not in self.permissions and 'admin_all' not in self.permissions:
            raise tornado.web.HTTPError(403, "no permission for create groups")

        try:
            group_id = self.db.add_group(name=group_name,
                                         color=color,
                                         users=self.data.get('users', None),
                                         indexes=self.data.get('indexes', None))
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.write({'id': group_id})

    async def put(self):
        group_id = self.data.get('id', None)
        if not group_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        if 'manage_groups' not in self.permissions and 'admin_all' not in self.permissions:
            raise tornado.web.HTTPError(403, "no permission for manage groups")

        self.db.update_group(group_id=group_id,
                             name=self.data.get('name', None),
                             color=self.data.get('color', None),
                             users=self.data.get('users', None),
                             indexes=self.data.get('indexes', None))
        self.write({'id': group_id})

    async def delete(self):
        group_id = self.get_argument('id', None)
        if not group_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        if 'delete_groups' in self.permissions or 'admin_all' in self.permissions:
            group_id = self.db.delete_group(group_id)
        else:
            raise tornado.web.HTTPError(403, "no permission for delete roles")
        self.write({'id': group_id})


class PermissionsHandler(BaseHandler):
    async def get(self):
        kwargs = {}

        if 'list_permissions' in self.permissions or 'admin_all' in self.permissions:
            target_user_id = self.get_argument('id', None)
            if target_user_id:
                kwargs['user_id'] = target_user_id
            names_only = self.get_argument('names_only', None)
            if names_only:
                kwargs['names_only'] = names_only
        else:
            kwargs['user_id'] = self.current_user

        permissions = self.db.get_permissions_data(**kwargs)
        self.write({'data': permissions})


class PermissionHandler(BaseHandler):
    async def get(self):
        permission_id = self.get_argument('id', None)
        if not permission_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        if 'read_permission' not in self.permissions and 'admin_all' not in self.permissions:
            raise tornado.web.HTTPError(403, "no permission for read permissions")

        permission_data = self.db.get_permission_data(permission_id)
        all_roles = self.db.get_roles_data(names_only=True)
        self.write({'data': permission_data, 'roles': all_roles})

    async def post(self):
        permission_name = self.data.get('name', None)
        if not permission_name:
            raise tornado.web.HTTPError(400, "param 'name' is required")
        if 'create_permissions' not in self.permissions and 'admin_all' not in self.permissions:
            raise tornado.web.HTTPError(403, "no permission for create groups")

        try:
            group_id = self.db.add_permission(name=permission_name,
                                              roles=self.data.get('roles', None))
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.write({'id': group_id})

    async def put(self):
        permission_id = self.data.get('id', None)
        if not permission_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        if 'manage_permissions' not in self.permissions and 'admin_all' not in self.permissions:
            raise tornado.web.HTTPError(403, "no permission for manage permissions")

        self.db.update_permission(permission_id=permission_id,
                                  name=self.data.get('name', None),
                                  roles=self.data.get('roles', None))
        self.write({'id': permission_id})

    async def delete(self):
        permission_id = self.get_argument('id', None)
        if not permission_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        if 'delete_permissions' in self.permissions or 'admin_all' in self.permissions:
            group_id = self.db.delete_permission(permission_id)
        else:
            raise tornado.web.HTTPError(403, "no permission for delete permissions")
        self.write({'id': group_id})


class IndexesHandler(BaseHandler):
    async def get(self):
        kwargs = {}

        if 'list_indexes' in self.permissions or 'admin_all' in self.permissions:
            target_user_id = self.get_argument('id', None)
            if target_user_id:
                kwargs['user_id'] = target_user_id
            names_only = self.get_argument('names_only', None)
            if names_only:
                kwargs['names_only'] = names_only
        else:
            kwargs['user_id'] = self.current_user

        permissions = self.db.get_indexes_data(**kwargs)
        self.write({'data': permissions})


class IndexHandler(BaseHandler):
    async def get(self):
        index_id = self.get_argument('id', None)
        if not index_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        if 'read_indexes' not in self.permissions and 'admin_all' not in self.permissions:
            raise tornado.web.HTTPError(403, "no permission for read indexes")

        index_data = self.db.get_index_data(index_id)
        all_groups = self.db.get_groups_data(names_only=True)
        self.write({'data': index_data, 'groups': all_groups})

    async def post(self):
        index_name = self.data.get('name', None)
        if not index_name:
            raise tornado.web.HTTPError(400, "param 'name' is required")
        if 'create_indexes' not in self.permissions and 'admin_all' not in self.permissions:
            raise tornado.web.HTTPError(403, "no permission for create indexes")

        try:
            index_id = self.db.add_index(name=index_name,
                                         groups=self.data.get('groups', None))
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.write({'id': index_id})

    async def put(self):
        index_id = self.data.get('id', None)
        if not index_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        if 'manage_indexes' not in self.permissions and 'admin_all' not in self.permissions:
            raise tornado.web.HTTPError(403, "no permission for manage indexes")

        self.db.update_index(index_id=index_id,
                             name=self.data.get('name', None),
                             groups=self.data.get('groups', None))
        self.write({'id': index_id})

    async def delete(self):
        index_id = self.get_argument('id', None)
        if not index_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        if 'delete_indexes' in self.permissions or 'admin_all' in self.permissions:
            group_id = self.db.delete_index(index_id)
        else:
            raise tornado.web.HTTPError(403, "no permission for delete indexes")
        self.write({'id': group_id})


class DashboardHandler(BaseHandler):
    async def get(self):
        dash_name = self.get_argument('name', None)
        if not dash_name:
            raise tornado.web.HTTPError(400, "param 'name' is needed")

        dash = self.db.load_dashboard(name=dash_name)
        self.write({'data': dash})

    async def post(self):
        dash_name = self.data.get('name', None)
        dash_body = self.data.get('body', None)
        if None in [dash_name, dash_body]:
            raise tornado.web.HTTPError(400, "params 'name' and 'body' is needed")
        dash_id = self.db.save_dashboard(name=dash_name,
                                         body=dash_body)
        self.write({'id': dash_id})

    async def delete(self):
        dash_name = self.get_argument('name', None)
        if not dash_name:
            raise tornado.web.HTTPError(400, "param 'name' is needed")
        dash_id = self.db.delete_dashboard(name=dash_name)
        self.write({'id': dash_id})


class UserPermissionsHandler(BaseHandler):
    async def get(self):
        self.write({'data': self.permissions})
