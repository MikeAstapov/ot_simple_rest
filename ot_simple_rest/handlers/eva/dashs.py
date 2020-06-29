import os
import io
import jwt
import uuid
import json
import tempfile
import tarfile
from datetime import datetime
import logging

import tornado.web
import tornado.httputil

from handlers.eva.base import BaseHandler

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.2"
__maintainer__ = "Andrey Starchenkov"
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

        dashs = self.db.get_dashs_data(**kwargs)
        self.write({'data': dashs})


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


def make_unique_name(path_template, name, num=0):
    if os.path.exists(path_template.format(name)):
        num = num + 1
        if not os.path.exists(path_template.format(f'{name}-{num}')):
            return name
        else:
            make_unique_name(path_template, name, num)
    return name


class DashExportHandler(BaseHandler):
    """
    There is method for export one or more dash object in '.json' format files.
    Json files returns in 'eva.dash' package with datetime-name.
    """

    def initialize(self, **kwargs):
        super().initialize(kwargs['db_conn_pool'])
        self.static_conf = kwargs['static_conf']
        self.static_dir_name = 'storage'

    async def get(self):
        dash_ids = self.get_argument('ids', None)
        if not dash_ids:
            raise tornado.web.HTTPError(400, "param 'ids' is needed")
        dash_ids = dash_ids.split(',')
        dash_ids = [int(_) for _ in dash_ids]

        with tempfile.TemporaryDirectory() as tmp_dir:
            archive_name = f"{datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')}.eva.dash"
            _dirname = str(uuid.uuid4())
            _base_path = os.path.join(self.static_conf['static_path'], self.static_dir_name, _dirname)
            if not os.path.exists(_base_path):
                os.makedirs(_base_path)

            archive_path = os.path.join(_base_path, archive_name)
            archive = tarfile.open(archive_path, mode='x:gz')

            for did in dash_ids:
                try:
                    dash_data = self.db.get_dash_data(dash_id=did)
                    if not dash_data:
                        raise tornado.web.HTTPError(404, f'No dash with id={did}')
                except Exception as err:
                    raise tornado.web.HTTPError(409, str(err))

                path_template = os.path.join(tmp_dir, '{}.json')
                filename = make_unique_name(path_template, dash_data['name'])
                filepath = path_template.format(filename)

                if not os.path.exists(filepath):
                    with open(filepath, 'w+') as f:
                        f.write(dash_data['body'])

                archive.add(filepath, filename)
            archive.close()
        self.write(f'{self.static_dir_name}/{_dirname}/{archive_name}')


class DashImportHandler(BaseHandler):
    """
    That handler allows to import dashs, exported with DashExportHandler.
    Or you can put your own 'eva.dash' file with inner dashs json files.
    """

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
        group = args.get('group')

        if not files or not files.get('body'):
            return self.write({'status': 'no file in payload'})
        tar_file = files['body'][0]

        # wraps bytes to work with it like with file
        file_like_object = io.BytesIO(tar_file['body'])
        with tarfile.open(mode='r:gz', fileobj=file_like_object) as tar:
            dtn = datetime.now().strftime('%Y%m%d%H%M%S')
            for dash in tar.getmembers():
                try:
                    dash_data = tar.extractfile(dash)
                    self.db.add_dash(name=f'{dash.name}_imported_{dtn}',
                                     body=dash_data.read().decode(),
                                     groups=[group[0].decode()])
                except Exception as err:
                    raise tornado.web.HTTPError(409, str(err))
            self.write({'status': 'success'})


class GroupExportHandler(BaseHandler):
    """
    There is method for export one or more group object with dashs in '.json' format files.
    Json files returns in 'eva.group' package with datetime-name.
    """

    def initialize(self, **kwargs):
        super().initialize(kwargs['db_conn_pool'])
        self.static_conf = kwargs['static_conf']
        self.static_dir_name = 'storage'

    async def get(self):
        group_ids = self.get_argument('ids', None)
        if not group_ids:
            raise tornado.web.HTTPError(400, "param 'ids' is needed")
        group_ids = group_ids.split(',')
        group_ids = [int(_) for _ in group_ids]

        with tempfile.TemporaryDirectory() as tmp_dir:
            archive_name = f"{datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')}.eva.group"
            _dirname = str(uuid.uuid4())
            _base_path = os.path.join(self.static_conf['static_path'], self.static_dir_name, _dirname)
            if not os.path.exists(_base_path):
                os.makedirs(_base_path)

            archive_path = os.path.join(_base_path, archive_name)
            archive = tarfile.open(archive_path, mode='x:gz')

            for gid in group_ids:
                try:
                    dashs_data = self.db.get_dashs_data(group_id=gid)
                except Exception as err:
                    raise tornado.web.HTTPError(409, str(err))

                group_dir = os.path.join(tmp_dir, str(gid))
                if not os.path.exists(group_dir):
                    os.makedirs(group_dir)
                path_template = os.path.join(group_dir, '{}.json')

                for dash in dashs_data:
                    filename = make_unique_name(path_template, dash['name'])
                    filepath = path_template.format(filename)

                    if not os.path.exists(filepath):
                        with open(filepath, 'w+') as f:
                            f.write(dash['body'])
                    archive.add(filepath, os.path.join(str(gid), filename))

                # adds group metadata for future import
                meta_filemath = path_template.format('_META')
                with open(meta_filemath, 'w+') as f:
                    group_metadata = self.db.get_group_data(group_id=gid)
                    f.write(json.dumps(group_metadata))
                archive.add(meta_filemath, os.path.join(str(gid), '_META'))

            archive.close()
        self.write(f'{self.static_dir_name}/{_dirname}/{archive_name}')


class GroupImportHandler(BaseHandler):
    """
    That handler allows to import groups, exported with GroupExportHandler.
    Or you can put your own 'eva.group' file with dirs named group_id
    with inner dashs json files.
    """

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

        if not files or not files.get('body'):
            return self.write({'status': 'no file in payload'})
        tar_file = files['body'][0]

        # wraps bytes to work with it like with file
        file_like_object = io.BytesIO(tar_file['body'])
        with tarfile.open(mode='r:gz', fileobj=file_like_object) as tar:
            inner_objects = tar.getnames()
            meta_list = [s for s in inner_objects if '_META' in s]
            dtn = datetime.now().strftime('%Y%m%d%H%M%S')

            for meta in meta_list:
                meta_data = tar.extractfile(meta)
                meta_dict = json.loads(meta_data.read())
                group_name = f"{meta_dict['name']}_imported_{dtn}"
                self.db.add_group(name=group_name,
                                  color=meta_dict['color'])
                dashs = [s for s in inner_objects if s.startswith(f"{meta_dict['id']}/")
                         and '_META' not in s]
                for dash in dashs:
                    dash_name = dash.split('/', 1)[-1]
                    dash_data = tar.extractfile(dash)
                    self.db.add_dash(name=f'{dash_name}_imported_{dtn}',
                                     body=dash_data.read().decode(),
                                     groups=[group_name])
        self.write({'status': 'success'})
