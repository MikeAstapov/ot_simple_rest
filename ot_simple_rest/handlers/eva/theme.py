import json
import logging
import tornado.web
import tornado.httputil

from handlers.eva.base import BaseHandler

__author__ = "Alexander Matiakubov"
__copyright__ = "Copyright 2021, ISG Neuro"
__license__ = "LICENSE.md"
__version__ = "0.0.1"
__maintainer__ = "Alexander Matiakubov"
__email__ = "amatiakubov@isgneuro.com"
__status__ = "Development"


class ThemeListHandler(BaseHandler):
    def initialize(self, **kwargs):
        super().initialize(kwargs['db_conn_pool'])
        self.logger = logging.getLogger('osr')

    async def get(self):
        _offset = self.get_argument('offset', 0)
        _limit = self.get_argument('limit', 100)
        try:
            themes_list = self.db.get_themes_data(limit=_limit, offset=_offset)
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.logger.debug(f'ThemeListHandler RESPONSE: {themes_list}')
        self.write(json.dumps(themes_list))


class ThemeGetHandler(BaseHandler):
    def initialize(self, **kwargs):
        super().initialize(kwargs['db_conn_pool'])
        self.logger = logging.getLogger('osr')

    async def get(self):
        theme_name = self.get_argument('themeName', None)
        if not theme_name:
            raise tornado.web.HTTPError(400, "param 'themeName' is needed")
        self.logger.debug(f'ThemeGetHandler Request theme name: {theme_name}')
        try:
            # theme = self.db.get_dash_data(dash_id=dash_id)
            theme = self.db.get_theme(theme_name=theme_name)
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))
        self.logger.debug(f'ThemeGetHandler RESPONSE: {theme}')
        self.write(json.dumps(theme))


class ThemeHandler(BaseHandler):
    def initialize(self, **kwargs):
        super().initialize(kwargs['db_conn_pool'])
        self.logger = logging.getLogger('osr')

    async def post(self):
        theme_name = self.data.get('themeName', None)
        if not theme_name:
            raise tornado.web.HTTPError(400, "param 'themeName' is needed for creating theme")

        # 1. create theme in DB
        try:
            self.logger.debug(f'ThemeHandler create theme, name: {theme_name} with body: {json.dumps(self.data)}')
            theme = self.db.add_theme(theme_name=theme_name,
                                      content=json.dumps(self.data))
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))

        # 2. return saved theme
        self.write(json.dumps(theme))

    async def delete(self):
        theme_name = self.data.get('themeName', None)
        if not theme_name:
            raise tornado.web.HTTPError(400, "param 'themeName' is needed for deleting")
        # 1. get theme
        try:
            # theme = self.db.get_dash_data(dash_id=dash_id)
            theme = self.db.get_theme(theme_name=theme_name)
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))

        # 2. delete theme
        try:
            deleted_theme_name = self.db.delete_theme(theme_name=theme_name)
            self.logger.debug(f'ThemeHandler Deleted theme name: {deleted_theme_name} with body: {theme}')
        except Exception as err:
            raise tornado.web.HTTPError(409, str(err))

        # 3. return deleted theme
        self.write(json.dumps(theme))
