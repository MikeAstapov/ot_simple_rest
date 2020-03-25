import os
from hashlib import blake2b

import tornado.web

from handlers.eva.base import BaseHandler


BASE_LOGS_DIR = '/opt/otp/ot_simple_rest/logs/clients'


class LogsHandler(BaseHandler):
    async def post(self):
        logs_text = self.data.get('log', None)
        if not logs_text:
            raise tornado.web.HTTPError(400, "params 'log' is needed")

        if not os.path.exists(BASE_LOGS_DIR):
            os.makedirs(BASE_LOGS_DIR)

        h = blake2b(digest_size=4)
        h.update(self.token.encode())
        client_id = str(self.current_user) + '_' + h.hexdigest()

        log_file_path = os.path.join(BASE_LOGS_DIR, client_id)
        try:
            with open(f'{log_file_path}.log', 'a+') as f:
                f.write(logs_text)
        except Exception as err:
            raise tornado.web.HTTPError(405, str(err))
        self.write({'status': 'OK'})
