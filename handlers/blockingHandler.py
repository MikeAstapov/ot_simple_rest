import os
import time

import tornado.web
import tornado.gen
from tornado.ioloop import IOLoop


class BlockingHandler(tornado.web.RequestHandler):

    async def get(self):
        future = IOLoop.current().run_in_executor(None, self.blocking_read)
        await future

    def blocking_read(self):
        events = self.load_from_memcache()
        self.write(events)

    @staticmethod
    def load_from_memcache():
        events = {}
        path_to_cache_dir = '/home/andrey/tmp/search_424.cache/'
        file_names = os.listdir(path_to_cache_dir)
        for file_name in file_names:
            if file_name[-4:] == '.csv':
                events[file_name] = open(path_to_cache_dir + file_name).read()
        return events
