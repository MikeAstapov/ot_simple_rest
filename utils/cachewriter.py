import csv
import json
import logging

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.2.0"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


class CacheWriter:
    """
    Writes JSON or Dict to CSV file as a table. Header is taken from keys.
    """

    def __init__(self, cache, cache_id, mem_conf):
        """
        Checks type of incoming cache. If it is JSON this parses it to dictionary.
        :param cache: results of Job for saving to RAM cache.
        :param cache_id: the same id as Job's one.
        :param mem_conf: config of RAM cache.
        """
        self.logger = logging.getLogger('osr')
        self.logger.debug('Initialized.')

        self.cache = cache if type(cache) is dict else json.loads(cache)
        self.file_name = '%ssearch_%s.cache' % (mem_conf['path'], cache_id)

        self.logger.debug('Cache %s:%s with header will be written.' % (self.file_name, type(self.cache)))

    def get_fieldnames(self):
        """
        Iterates the list getting unique key names.
        :return: list of unique keys.
        """
        fieldnames = []
        for line in self.cache:
            fieldnames += list(line.keys())
        fieldnames = set(fieldnames)
        return fieldnames

    def write_csv(self):
        """
        Writes the CSV file with cache data.
        :return:
        """
        with open(self.file_name, 'w') as fw:
            fieldnames = list(self.get_fieldnames())
            writer = csv.DictWriter(fw, fieldnames=fieldnames)
            writer.writeheader()
            for line in self.cache:
                writer.writerow(line)

    def write_json(self):
        """
        Writes the JSON Lines file with cache data.
        :return:
        """
        with open(self.file_name, 'w') as fw:
            fieldnames = self.get_fieldnames()
            for line in self.cache:
                keys = set(line.keys())
                empty_keys = fieldnames - keys
                for empty_key in empty_keys:
                    line[empty_key] = ''
                fw.write('%s\n' % json.dumps(line))

    def write(self):
        self.write_json()
