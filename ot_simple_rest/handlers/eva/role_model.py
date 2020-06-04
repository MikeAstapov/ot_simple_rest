import bcrypt

import tornado.escape
import tornado.ioloop
import tornado.web

from handlers.eva.base import BaseHandler

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Andrey Starchenkov"
__email__ = "akhromov@ot.ru"
__status__ = "Production"


class UsersHandler(BaseHandler):
    async def get(self):
        kwargs = {}

        names_only = self.get_argument('names_only', None)
        if names_only:
            kwargs['names_only'] = names_only

        if 'list_users' not in self.permissions and 'admin_all' not in self.permissions:
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
            new_password = self.data.get("password", None)
            if new_password:
                new_hashed_password = await tornado.ioloop.IOLoop.current().run_in_executor(
                    None,
                    bcrypt.hashpw,
                    tornado.escape.utf8(new_password),
                    bcrypt.gensalt(),
                )
                new_password = tornado.escape.to_unicode(new_hashed_password)
        else:
            user_data = self.db.get_auth_data(user_id=target_user_id)
            stored_password = user_data.pop("password")

            old_password = self.data.get("old_password", None)
            new_password = self.data.get("new_password", None)

            if new_password and old_password:
                password_equal = await tornado.ioloop.IOLoop.current().run_in_executor(
                    None,
                    bcrypt.checkpw,
                    tornado.escape.utf8(old_password),
                    tornado.escape.utf8(stored_password),
                )

                if not password_equal:
                    raise tornado.web.HTTPError(403, "old password value is incorrect")

                new_hashed_password = await tornado.ioloop.IOLoop.current().run_in_executor(
                    None,
                    bcrypt.hashpw,
                    tornado.escape.utf8(new_password),
                    bcrypt.gensalt(),
                )
                new_password = tornado.escape.to_unicode(new_hashed_password)

        new_username = self.data.get('name', None)
        user_id = self.db.update_user(user_id=target_user_id,
                                      name=new_username,
                                      password=new_password,
                                      roles=self.data.get('roles', None),
                                      groups=self.data.get('groups', None))

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

        try:
            group_data = self.db.get_group_data(group_id)
        except Exception as err:
            raise tornado.web.HTTPError(400, str(err))

        all_users = self.db.get_users_data(names_only=True)
        all_indexes = self.db.get_indexes_data(names_only=True)
        all_dashs = self.db.get_dashs_data(names_only=True)
        self.write({'data': group_data, 'users': all_users,
                    'indexes': all_indexes, 'dashs': all_dashs})

    async def post(self):
        group_name = self.data.get('name', None)
        color = self.data.get('color', None)
        if None in [group_name, color]:
            raise tornado.web.HTTPError(400, "params 'name' and 'color' is required")
        # if 'create_groups' not in self.permissions and 'admin_all' not in self.permissions:
        #     raise tornado.web.HTTPError(403, "no permission for create groups")

        try:
            group_id = self.db.add_group(name=group_name,
                                         color=color,
                                         users=self.data.get('users', None),
                                         dashs=self.data.get('dashs', None),
                                         indexes=self.data.get('indexes', None))
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.write({'id': group_id})

    async def put(self):
        group_id = self.data.get('id', None)
        if not group_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        # if 'manage_groups' not in self.permissions and 'admin_all' not in self.permissions:
        #     raise tornado.web.HTTPError(403, "no permission for manage groups")
        self.db.update_group(group_id=group_id,
                             name=self.data.get('name', None),
                             color=self.data.get('color', None),
                             users=self.data.get('users', None),
                             dashs=self.data.get('dashs', None),
                             indexes=self.data.get('indexes', None))
        self.write({'id': group_id})

    async def delete(self):
        group_id = self.get_argument('id', None)
        if not group_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        if 'delete_groups' in self.permissions or 'admin_all' in self.permissions:
            group_id = self.db.delete_group(group_id)
        else:
            raise tornado.web.HTTPError(403, "no permission for delete groups")
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
            raise tornado.web.HTTPError(403, "no permission for create permissions")

        try:
            permission_id = self.db.add_permission(name=permission_name,
                                                   roles=self.data.get('roles', None))
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.write({'id': permission_id})

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
            permission_id = self.db.delete_permission(permission_id)
        else:
            raise tornado.web.HTTPError(403, "no permission for delete permissions")
        self.write({'id': permission_id})


class IndexesHandler(BaseHandler):
    async def get(self):
        kwargs = {}

        names_only = self.get_argument('names_only', None)
        if names_only:
            kwargs['names_only'] = names_only

        if 'list_indexes' in self.permissions or 'admin_all' in self.permissions:
            target_user_id = self.get_argument('id', None)
            if target_user_id:
                kwargs['user_id'] = target_user_id
        else:
            kwargs['user_id'] = self.current_user

        indexes = self.db.get_indexes_data(**kwargs)
        self.write({'data': indexes})


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
            index_id = self.db.delete_index(index_id)
        else:
            raise tornado.web.HTTPError(403, "no permission for delete indexes")
        self.write({'id': index_id})


class UserPermissionsHandler(BaseHandler):
    async def get(self):
        self.write({'data': self.permissions})


class UserGroupsHandler(BaseHandler):
    async def get(self):
        kwargs = {}

        if 'read_groups' not in self.permissions and 'admin_all' not in self.permissions:
            kwargs['user_id'] = self.current_user

        kwargs['names_only'] = self.get_argument('names_only', None)
        user_groups = self.db.get_groups_data(**kwargs)
        self.write({'data': user_groups})


class UserDashboardsHandler(BaseHandler):
    async def get(self):
        if 'admin_all' in self.permissions:
            return self.write({'data': self.db.get_dashs_data(names_only=True)})

        names_only = self.get_argument('names_only', None)

        dashs = list()
        user_groups = self.db.get_groups_data(user_id=self.current_user)

        for group in user_groups:
            user_dashs = self.db.get_dashs_data(group_id=group['id'], names_only=names_only)
            dashs.extend(user_dashs)

        if names_only:
            dashs = list(set(dashs))
        else:
            dashs = list({v['id']: v for v in dashs}.values())
        self.write({'data': dashs})


class GroupDashboardsHandler(BaseHandler):
    async def get(self):
        group_id = self.get_argument('id', None)
        if not group_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")

        group_dashs = self.db.get_dashs_data(group_id=group_id)
        self.write({'data': group_dashs})
