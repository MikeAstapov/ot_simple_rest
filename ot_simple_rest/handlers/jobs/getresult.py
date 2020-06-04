import os
import logging
import json

import tornado.web

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.2"
__maintainer__ = "Andrey Starchenkov"
__email__ = "akhromov@ot.ru"
__status__ = "Production"


class GetResult(tornado.web.RequestHandler):
    """
    Returns result from disk or make data urls list.

    1-st use-case: get result directly from disk with Tornado tools;
    2-nd use-case: get list of data urls for download (with nginx or something else);

    :method generate_data_links
    """

    def initialize(self, mem_conf, static_conf):
        self.mem_conf = mem_conf
        self.static_conf = static_conf
        self.data_path = self.mem_conf['path']
        self.base_url = self.static_conf['base_url']
        self.with_nginx = False if static_conf['use_nginx'] == 'False' else True

        self.logger = logging.getLogger('osr')
        self._cache_name_template = 'search_{}.cache/data'

    async def get(self):
        """
        It writes response to remote side.

        :return:
        """
        params = self.request.query_arguments
        cid = params.get('cid')[0].decode()
        if self.with_nginx:
            self.generate_data_links(cid)
        else:
            self.load_and_send_from_memcache(cid)

    def generate_data_links(self, cid):
        """
        Makes listing of directory with cache data and generate links
        for that data with url pattern.

        :param cid:         OT_Dispatcher's job cid
        :return:
        """
        cache_dir = self._cache_name_template.format(cid)
        cache_full_path = os.path.join(self.data_path, cache_dir)

        if not os.path.exists(cache_full_path):
            self.logger.error('No cache with id={}'.format(cid))
            return self.write({'status': 'failed', 'error': 'No cache with id={}'.format(cid)})
            # raise tornado.web.HTTPError(405, f'No cache with id={cid}')

        self.logger.debug('Cache with id={} exists'.format(cid))
        listing = os.listdir(cache_full_path)
        cache_list = [f for f in listing if f.endswith('.json') or 'SCHEMA' in f]
        cache_list = [os.path.join(cache_dir, f) for f in cache_list]
        urls_list = [self.base_url.format(f) for f in cache_list]
        response = {"status": "success", "data_urls": urls_list}
        self.logger.debug(response)
        self.write(response)

    def load_and_send_from_memcache(self, cid):
        """
        Makes listing of directory with cache data and read files
        content. Then join chunks together and writes data in socket.

        :param cid:         OT_Dispatcher's job cid
        :return:
        """
        self.logger.debug(f'Started loading cache {cid}.')
        path_to_cache_dir = os.path.join(self.data_path, f'search_{cid}.cache')
        self.logger.debug(f'Path to cache {path_to_cache_dir}.')
        file_names = [file_name for file_name in os.listdir(path_to_cache_dir) if file_name[-5:] == '.json']
        with open(os.path.join(path_to_cache_dir, "_SCHEMA")) as fr:
            df_schema = fr.read()
        self.write('{"status": "success", "schema": "%s", "events": {' % df_schema.strip())
        length = len(file_names)
        for i in range(length):
            file_name = file_names[i]
            self.logger.debug(f'Reading part: {file_name}')
            self.write(f'"{file_name}": ')
            with open(os.path.join(path_to_cache_dir, file_name)) as fr:
                body = fr.read()
            self.write(json.dumps(body))
            if i != length - 1:
                self.write(', ')
        self.write('}}')
