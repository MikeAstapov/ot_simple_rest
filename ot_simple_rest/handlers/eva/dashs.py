import tornado.web

from handlers.eva.base import BaseHandler


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
            dash_id = self.db.add_dash(name=dash_name,
                                       body=dash_body,
                                       groups=dash_groups)
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.write({'id': dash_id})

    async def put(self):
        dash_id = self.data.get('id', None)
        if not dash_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")

        try:
            dash_name = self.db.update_dash(dash_id=dash_id,
                                            name=self.data.get('name', None),
                                            body=self.data.get('body', None),
                                            groups=self.data.get('groups', None))
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.write({'name': dash_name})

    async def delete(self):
        dash_id = self.get_argument('id', None)
        if not dash_id:
            raise tornado.web.HTTPError(400, "param 'name' is needed")
        dash_id = self.db.delete_dash(dash_id=dash_id)
        self.write({'id': dash_id})

