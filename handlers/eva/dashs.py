import tornado.web

from handlers.eva.base import BaseHandler


class DashboardsHandler(BaseHandler):
    async def get(self):
        kwargs = {}

        if 'list_dashs' in self.permissions or 'admin_all' in self.permissions:
            target_user_id = self.get_argument('id', None)
            if target_user_id:
                kwargs['user_id'] = target_user_id
            names_only = self.get_argument('names_only', None)
            if names_only:
                kwargs['names_only'] = names_only
        else:
            kwargs['user_id'] = self.current_user

        roles = self.db.get_dashs_data(**kwargs)
        self.write({'data': roles})


class DashboardHandler(BaseHandler):
    async def get(self):
        dash_name = self.get_argument('name', None)
        if not dash_name:
            raise tornado.web.HTTPError(400, "param 'name' is needed")

        dash = self.db.load_dash(name=dash_name)
        self.write({'data': dash})

    async def post(self):
        dash_name = self.data.get('name', None)
        dash_body = self.data.get('body', None)
        dash_groups = self.data.get('groups', None)
        if None in [dash_name, dash_body]:
            raise tornado.web.HTTPError(400, "params 'name' and 'body' is needed")

        dash_id = self.db.save_dash(name=dash_name,
                                    body=dash_body,
                                    groups=dash_groups)
        self.write({'id': dash_id})

    async def delete(self):
        dash_name = self.get_argument('name', None)
        if not dash_name:
            raise tornado.web.HTTPError(400, "param 'name' is needed")
        dash_id = self.db.delete_dash(name=dash_name)
        self.write({'id': dash_id})
