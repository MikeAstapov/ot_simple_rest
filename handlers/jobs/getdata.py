import os
import logging
import json

import tornado.web

__author__ = "Anton Khromov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.8.0"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


class GetResult(tornado.web.RequestHandler):
    """
    Returns result from disk or make data urls list.

    1-st use-case: get result directly from disk with Tornado tools;
    2-nd use-case: get list of data urls for download (with nginx or something else);

    :method generate_data_links
    """

    def initialize(self, mem_conf, static_conf):
        self.mem_conf = dict(mem_conf)
        self.static_conf = dict(static_conf)
        self.data_path = self.mem_conf['path']
        self.base_url = self.static_conf['base_url']
        self.with_nginx = static_conf.getboolean('use_nginx')

        self.logger = logging.getLogger('osr')
        self._cache_name_template = 'search_{}.cache'

    async def get(self):
        """
        It writes response to remote side.

        :return:
        """
        params = self.request.query_arguments
        task_id = params.get('task_id')[0].decode()
        if self.with_nginx:
            self.generate_data_links(task_id)
        else:
            self.load_and_send_from_memcache(task_id)

    def generate_data_links(self, task_id):
        """
        Makes listing of directory with cache data and generate links
        for that data with url pattern.

        :param task_id:         OT_Dispatcher's job cid
        :return:
        """
        cache_dir = self._cache_name_template.format(task_id)
        cache_full_path = os.path.join(self.data_path, cache_dir)

        if not os.path.exists(cache_full_path):
            self.logger.debug('No cache for task with id={}'.format(task_id))
            self.write({'status': 'No cache for task with id={}'.format(task_id)})

        self.logger.debug('Task id={} cache exists'.format(task_id))
        listing = os.listdir(cache_full_path)
        cache_list = [f for f in listing if f.endswith('.json') or 'SCHEMA' in f]
        cache_list = [os.path.join(cache_dir, f) for f in cache_list]
        urls_list = [self.base_url.format(f) for f in cache_list]
        response = {"data_urls": urls_list}
        self.logger.debug(response)
        self.write(response)

    def load_and_send_from_memcache(self, task_id):
        """
        Makes listing of directory with cache data and read files
        content. Then join chunks together and writes data in socket.

        :param task_id:         OT_Dispatcher's job cid
        :return:
        """
        self.logger.debug('Started loading cache %s.' % task_id)
        path_to_cache_dir = '%s/search_%s.cache/' % (self.data_path, task_id)
        self.logger.debug('Path to cache %s.' % path_to_cache_dir)
        file_names = [file_name for file_name in os.listdir(path_to_cache_dir) if file_name[-5:] == '.json']
        with open(path_to_cache_dir + "_SCHEMA") as fr:
            df_schema = fr.read()
        self.write('{"status": "success", "schema": "%s", "events": {' % df_schema.strip())
        length = len(file_names)
        for i in range(length):
            file_name = file_names[i]
            self.logger.debug('Reading part: %s' % file_name)
            self.write('"%s": ' % file_name)
            with open(path_to_cache_dir + file_name) as fr:
                body = fr.read()
            self.write(json.dumps(body))
            if i != length - 1:
                self.write(', ')
        self.write('}}')
