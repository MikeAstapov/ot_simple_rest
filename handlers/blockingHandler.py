import json
import os
import tornado.web
import tornado.gen
from tornado.ioloop import IOLoop

# For testing purpose ONLY!!!


class BlockingHandler(tornado.web.RequestHandler):

    async def get(self):
        future = IOLoop.current().run_in_executor(None, self.load_from_memcache)
        await future

    def load_from_memcache(self):
        path_to_cache_dir = '/home/andrey/tmp/search_424.cache/'
        file_names = os.listdir(path_to_cache_dir)
        self.write('{"status": "success", "events": {')
        length = len(file_names)
        for i in range(length):
            file_name = file_names[i]
            if file_name[-4:] == '.csv':
                self.write('"%s": ' % file_name)
                body = open(path_to_cache_dir + file_name).read()
                self.write(json.dumps(body))
                if i != length-1:
                    self.write(", ")
        self.write('}}')

        # return events
