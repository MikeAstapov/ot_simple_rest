import tornado.web
from handlers.eva.base import BaseHandler


class Settings(BaseHandler):
    async def get(self):
        setting_id = self.get_argument('id', None)
        if not setting_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")
        try:
            setting = self.db.get_setting(setting_id=setting_id)
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.write({'setting': setting})

    async def post(self):
        if 'admin_all' not in self.permissions:
            raise tornado.web.HTTPError(403, 'Not allowed')

        setting_name = self.data.get('name', None)
        setting_body = self.data.get('body', "")
        if not setting_name:
            raise tornado.web.HTTPError(400, "params 'name' is needed")
        try:
            new_setting_id = self.db.add_setting(
                name=setting_name,
                body=setting_body,
            )
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.write({'id': new_setting_id})

    async def put(self):
        if 'admin_all' not in self.permissions:
            raise tornado.web.HTTPError(403, 'Not allowed')

        setting_id = self.data.get('id', None)
        if not setting_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")

        try:
            name, modified = self.db.update_setting(
                setting_id=setting_id,
                name=self.data.get('name', None),
                body=self.data.get('body', None),
            )
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.write({'id': setting_id, 'name': name})

    async def delete(self):
        setting_id = self.get_argument('id', None)
        if not setting_id:
            raise tornado.web.HTTPError(400, "param 'name' is needed")
        setting_id = self.db.delete_setting(setting_id=setting_id)
        self.write({'id': setting_id})
