from datetime import datetime, timedelta

import bcrypt
import jwt

import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.locks
import tornado.web
import tornado.util

SECRET_KEY = 'SOME_SECRET_KEY'


class NoResultError(Exception):
    pass


class BaseHandler(tornado.web.RequestHandler):
    def initialize(self, db_conn):
        """
        Gets config and init logger.

        :param db_conn: DB connector object.
        :return:
        """
        self.db = db_conn

    def set_default_headers(self):
        self.set_header('Access-Control-Expose-Headers', "*")
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header('Access-Control-Allow-Credentials', True)
        # self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        # self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def options(self, *args, **kwargs):
        self.set_header('Access-Control-Allow-Headers', 'Set-Cookie, *')
        self.set_header('Access-Control-Allow-Methods', '*')
        self.set_status(204)
        self.finish()

    def row_to_obj(self, row, cur):
        """Convert a SQL row to an object supporting dict and attribute access."""
        obj = tornado.util.ObjectDict()
        for val, desc in zip(row, cur.description):
            obj[desc.name] = val
        return obj

    def decode_token(self, token):
        return jwt.decode(token, SECRET_KEY, algorithms='HS256')

    def generate_token(self, payload):
        return jwt.encode(payload, SECRET_KEY, algorithm='HS256')

    # async def prepare(self):
    #     token = self.request.headers.get('Authorization')
    #     if not token:
    #         self.redirect('/auth/login')
    #     else:
    #         token = token.split('Bearer ')[-1]
    #         try:
    #             data = self.decode_token(token)
    #         except jwt.exceptions.InvalidSignatureError as err:
    #             print(f'Token: {token}, Error: {err}')
    #             raise tornado.web.HTTPError(401, "unauthorised")
    #         else:
    #             self.current_user = data


class AuthCreateHandler(BaseHandler):
    async def post(self):
        if self.db.check_user_exists(self.get_argument("username")):
            raise tornado.web.HTTPError(400, "author already created")
        hashed_password = await tornado.ioloop.IOLoop.current().run_in_executor(
            None,
            bcrypt.hashpw,
            tornado.escape.utf8(self.get_argument("password")),
            bcrypt.gensalt(),
        )
        self.db.add_user(
            role=self.get_argument('role'),
            username=self.get_argument('username'),
            password=tornado.escape.to_unicode(hashed_password)
        )
        self.write("{'status': 'success'}")


class AuthLoginHandler(BaseHandler):
    async def post(self):
        try:
            user = self.db.get_user_data(self.get_argument("username"))
            user_tokens = [u.token for u in user]
            user = user[0]
        except NoResultError:
            raise tornado.web.HTTPError(400, "incorrect login")
        else:
            password_equal = await tornado.ioloop.IOLoop.current().run_in_executor(
                None,
                bcrypt.checkpw,
                tornado.escape.utf8(self.get_argument("password")),
                tornado.escape.utf8(user.password),
            )
            if not password_equal:
                raise tornado.web.HTTPError(400, "incorrect password")

            client_token = self.get_cookie('token')
            client_token = client_token.split('Bearer ')[-1] if client_token else None

            if not client_token:
                payload = {'user_id': user.id, 'role': user.role_name,
                           'exp': int((datetime.now() + timedelta(hours=12)).timestamp())}
                token = self.generate_token(payload)
                self.db.add_session(
                    user.id,
                    token.decode('utf-8')
                )
                self.set_secure_cookie('cookie', str(user.username), expires_days=1, domain='172.25.12.186')
                self.set_cookie('token', token, expires_days=1, domain='172.25.12.186')
                self.write({'status': 'success'})

            elif client_token not in user_tokens:
                raise tornado.web.HTTPError(401, "unauthorized")
            else:
                self.write({'status': 'success'})
