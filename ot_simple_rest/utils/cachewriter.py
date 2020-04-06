import csv
import json
import logging
import os

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.3.0"
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
        _path = mem_conf['path']
        self.cache_dir = f'{_path}ssearch_{cache_id}.cache'

        self.logger.debug(f'Cache {self.cache_dir}:{type(self.cache)} with header will be written.')

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

    def generate_schema(self, fields):
        """
        Generates DDL schema of data frame.
        :type fields: set of fields
        :return: DDL schema string
        """
        schema = [f"`{field}` STRING" for field in fields]
        schema = ",".join(schema)
        self.logger.debug(f"Schema: {schema}")
        return schema + '\n'

    def write_csv(self):
        """
        Writes the CSV file with cache data.
        :return:
        """
        with open(self.cache_dir, 'w') as fw:
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
        part_file = os.path.join(self.cache_dir, "part-1.json")
        schema_file = os.path.join(self.cache_dir, "_SCHEMA")
        fieldnames = self.get_fieldnames()
        schema = self.generate_schema(fieldnames)
        os.makedirs(os.path.dirname(schema_file), exist_ok=True)
        with open(schema_file, 'w') as fw:
            fw.write(schema)
        with open(part_file, 'w') as fw:
            for line in self.cache:
                keys = set(line.keys())
                empty_keys = fieldnames - keys
                for empty_key in empty_keys:
                    line[empty_key] = ''
                fw.write('{}\n'.format(json.dumps(line)))

    def write(self):
        self.write_json()
