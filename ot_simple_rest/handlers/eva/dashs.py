import os
import logging

import tornado.web
import tornado.httputil

from handlers.eva.base import BaseHandler

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Anton Khromov"
__email__ = "akhromov@ot.ru"
__status__ = "Production"


class DashboardsHandler(BaseHandler):
    async def get(self):
        kwargs = {}

        if 'list_dashs' in self.permissions or 'admin_all' in self.permissions:
            target_group_id = self.get_argument('id', None)
            if target_group_id:
                kwargs['group_id'] = target_group_id
            names_only = self.get_argument('names_only', None)
            if names_only:
                kwargs['names_only'] = names_only
        else:
            raise tornado.web.HTTPError(403, "no permission for list dashs")

        roles = self.db.get_dashs_data(**kwargs)
        self.write({'data': roles})


# TODO: Make two separate handlers for full dash data and data without body
class DashboardHandler(BaseHandler):
    async def get(self):
        dash_id = self.get_argument('id', None)
        if not dash_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        try:
            dash = self.db.get_dash_data(dash_id=dash_id)
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        all_groups = self.db.get_groups_data(names_only=True)
        self.write({'data': dash, 'groups': all_groups})

    async def post(self):
        dash_name = self.data.get('name', None)
        dash_body = self.data.get('body', "")
        dash_groups = self.data.get('groups', None)
        if not dash_name:
            raise tornado.web.HTTPError(400, "params 'name' is needed")
        try:
            _id, modified = self.db.add_dash(name=dash_name,
                                             body=dash_body,
                                             groups=dash_groups)
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.write({'id': _id, 'modified': modified})

    async def put(self):
        dash_id = self.data.get('id', None)
        if not dash_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")

        try:
            name, modified = self.db.update_dash(dash_id=dash_id,
                                                 name=self.data.get('name', None),
                                                 body=self.data.get('body', None),
                                                 groups=self.data.get('groups', None))
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.write({'id': dash_id, 'name': name, 'modified': modified})

    async def delete(self):
        dash_id = self.get_argument('id', None)
        if not dash_id:
            raise tornado.web.HTTPError(400, "param 'name' is needed")
        dash_id = self.db.delete_dash(dash_id=dash_id)
        self.write({'id': dash_id})


class SvgLoadHandler(BaseHandler):
    def initialize(self, **kwargs):
        super().initialize(kwargs['db_conn_pool'])
        self.static_conf = kwargs['static_conf']
        self.logger = logging.getLogger('osr')

    async def prepare(self):
        client_token = self.get_cookie('eva_token')
        if client_token:
            self.token = client_token
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

    async def post(self):
        body = self.request.body
        args = {}
        files = {}
        tornado.httputil.parse_body_arguments(self.request.headers['Content-Type'], body, args, files)
        svg_path = self.static_conf['static_path'] + 'svg'
        _file = files['file'][0]

        saving_full_path = os.path.join(svg_path, _file['filename'])
        if not os.path.exists(saving_full_path):
            with open(saving_full_path, 'wb') as f:
                f.write(_file['body'])
        self.write({'status': 'ok'})
