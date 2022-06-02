import os
import jwt
import logging

import tornado.web
import tornado.httputil

from handlers.eva.base import BaseHandler
from tools.svg_manager import SVGManager, DELETE_OK, DELETE_FAIL


class SvgLoadHandler(BaseHandler):
    def initialize(self, **kwargs):
        super().initialize(kwargs['db_conn_pool'])
        self.static_conf = kwargs['static_conf']
        self.logger = logging.getLogger('osr')
        svg_path = os.path.join(self.static_conf['static_path'], 'svg')
        self.svg_manager = SVGManager(svg_path)

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
        _file = files['file'][0]

        new_name = self.svg_manager.write(_file['filename'], _file['body'])
        self.write({'status': 'ok'})

    async def delete(self):
        filename = self.get_argument('filename')
        status = self.svg_manager.delete(filename)
        if status == DELETE_OK:
            self.write({'status': 'ok'})
        else:
            self.write({'status': 'no such file'})
