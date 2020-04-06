from datetime import datetime, timedelta
import logging
import json

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
__maintainer__ = "Anton Khromov"
__email__ = "akhromov@ot.ru"
__status__ = "Production"

logger = logging.getLogger('osr')


class AuthLoginHandler(BaseHandler):
    async def prepare(self):
        self.data = json.loads(self.request.body) if self.request.body else dict()

    async def post(self):
        user = self.db.check_user_exists(self.data.get("username"))
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

