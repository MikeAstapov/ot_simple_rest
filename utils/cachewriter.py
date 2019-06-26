import csv
import json
import logging


class CacheWriter:

    def __init__(self, cache, cache_id, mem_conf):

        self.logger = logging.getLogger('osr')
        self.logger.debug('Initialized.')

        self.cache = cache if type(cache) is dict else json.loads(cache)
        self.file_name = '%ssearch_%s.cache' % (mem_conf['path'], cache_id)

        self.logger.debug('Cache %s:%s with header will be written.' % (self.file_name, type(self.cache)))

    def get_fieldnames(self):
        fieldnames = []
        for line in self.cache:
            fieldnames += list(line.keys())
        fieldnames = list(set(fieldnames))
        return fieldnames

    def write(self):
        with open(self.file_name, 'w') as fw:
            fieldnames = self.get_fieldnames()
            writer = csv.DictWriter(fw, fieldnames=fieldnames)
            writer.writeheader()
            for line in self.cache:
                self.logger.debug('Line %s:%s.' % (line, type(line)))
                writer.writerow(line)
