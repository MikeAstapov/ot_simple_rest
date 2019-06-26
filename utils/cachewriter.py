import csv
import json


class CacheWriter:

    def __init__(self, cache, cache_id, mem_conf):
        self.cache = cache if type(cache) is dict else json.loads(cache)
        self.file_name = '%ssearch_%s.cache' % (mem_conf['path'], cache_id)

    def write(self):
        with open(self.file_name, 'wb') as fw:
            writer = csv.DictWriter(fw, self.cache[0].keys())
            writer.writeheader()
            writer.writerows(self.cache)
