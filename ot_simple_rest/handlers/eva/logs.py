import os
from hashlib import blake2b

import tornado.web

from handlers.eva.base import BaseHandler


__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Anton Khromov"
__email__ = "akhromov@ot.ru"
__status__ = "Production"


class LogsHandler(BaseHandler):
    def initialize(self, **kwargs):
        super().initialize(kwargs['db_conn_pool'])
        self.logs_path = kwargs['logs_path']

    async def post(self):
        logs_text = self.data.get('log', None)
        if not logs_text:
            raise tornado.web.HTTPError(400, "params 'log' is needed")

        base_logs_dir = os.path.join(self.logs_path, 'cli_logs')
        if not os.path.exists(base_logs_dir):
            os.makedirs(base_logs_dir)

        h = blake2b(digest_size=4)
        h.update(self.token.encode())
        client_id = str(self.current_user) + '_' + h.hexdigest()

        log_file_path = os.path.join(base_logs_dir, client_id)
        try:
            with open(f'{log_file_path}.log', 'a+') as f:
                f.write(logs_text)
        except Exception as err:
            raise tornado.web.HTTPError(405, str(err))
        self.write({'status': 'success'})
