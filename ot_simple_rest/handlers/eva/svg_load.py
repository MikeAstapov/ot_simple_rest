import json
import os
import sys
import logging

import tornado.web
import tornado.httputil

from handlers.eva.base import BaseHandler
from tools.svg_manager import SVGManager


class SvgLoadHandler(BaseHandler):

    MAX_FILE_SIZE = 1024

    def initialize(self, **kwargs):
        super().initialize(kwargs['db_conn_pool'])
        self.file_conf = kwargs['file_upload_conf']
        self.static_conf = kwargs['static_conf']
        self.logger = logging.getLogger('osr')
        svg_path = self.file_conf.get('svg_path', os.path.join(self.static_conf['static_path'], 'svg'))
        self.svg_manager = SVGManager(svg_path)

    async def post(self):
        try:
            body = self.request.body
            args, files = {}, {}
            tornado.httputil.parse_body_arguments(self.request.headers['Content-Type'], body, args, files)
            _file = files['file'][0]
            if sys.getsizeof(_file) > self.MAX_FILE_SIZE:
                error_msg = f'File size more than 1 Mb; must be less.'
                self.logger.error(error_msg)
                response = {'status': 'failed', 'error': f'{error_msg}', 'notifications': [{'code': 4}]}
                self.write(json.dumps(response))
            else:
                named_as = self.svg_manager.write(_file['filename'], _file['body'])
                response = {'status': 'ok', 'filename': named_as, 'notifications': [{'code': 3}]}
                self.write(json.dumps(response))
        except Exception as e:
            self.logger.error(f'Error while writing file: {e}')
            response = {'status': 'failed', 'error': f'{e}', 'notifications': [{'code': 4}]}
            self.write(json.dumps(response))

    async def delete(self):
        try:
            filename = self.get_argument('filename')
            deleted = self.svg_manager.delete(filename)
            if deleted:
                self.write(json.dumps({'status': 'ok'}))
            else:
                self.write(json.dumps({'status': 'failed', 'error': 'file not found'}))
        except Exception as e:
            self.logger.error(f'Error while deleting file: {e}')
            self.write(json.dumps({'status': 'failed', 'error': f'{e}'}, default=str))
