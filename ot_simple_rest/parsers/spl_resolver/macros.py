import re
import logging
import os.path
from datetime import datetime

from parsers.spl_resolver.fieldalias import FieldAlias


class Macros:

    logger = logging.getLogger('osr')

    macros_pattern = r'__(?P<macros_name>\S+)__ (?P<macros_body>[^$|]+)'
    macros_args_pairs = r'((?P<token>\S+)=(?P<value>\S+))'
    macros_fields = r'(?<= )([^=]+( |$))'

    def __init__(self, name, body, directory):

        self.directory = directory
        self.name = name
        self.body = body

    def read(self):
        try:
            path = os.path.join(self.directory, '%s.macros' % self.name)
            with open(path) as fr:
                return fr.read()
        except FileNotFoundError:
            raise Exception("Macros not found")

    @staticmethod
    def get_epoch(date_string):
        utc_time = None
        try:
            utc_time = datetime.strptime(date_string, "%Y-%m-%d")
        except ValueError:
            pass
        try:
            utc_time = datetime.strptime(date_string, "%Y-%m-%d:%H:%M:%S")
        except ValueError:
            pass
        try:
            utc_time = datetime.strptime(date_string, "%Y-%m-%d:%H:%M:%S.%fZ")
        except ValueError:
            pass
        try:
            utc_time = datetime.strptime(date_string, "%Y/%m/%d")
        except ValueError:
            pass
        try:
            utc_time = datetime.strptime(date_string, "%Y/%m/%d:%H:%M:%S")
        except ValueError:
            pass
        try:
            utc_time = datetime.strptime(date_string, "%Y/%m/%d:%H:%M:%S.%fZ")
        except ValueError:
            pass
        try:
            utc_time = float(date_string)
        except ValueError:
            pass

        if utc_time is None:
            raise Exception("Time format is invalid")
        elif utc_time is float:
            epoch_time = utc_time
        else:
            epoch_time = utc_time.timestamp()

        return epoch_time

    @property
    def otl(self):
        macros_fields = re.findall(self.macros_fields, self.body)
        macros_fields = list(map(lambda x: x[0].split(" "), macros_fields))
        macros_fields = [field for sublist in macros_fields for field in sublist]
        macros_fields = list(filter(lambda x: x, macros_fields))

        self.logger.debug('Macros fields: %s.' % macros_fields)

        field_alias = FieldAlias(os.path.join(self.directory, 'names.csv'))
        aliases = []
        for field in macros_fields:
            field_aliases = field_alias.get_aliases(field)
            self.logger.debug('Field: %s, Aliases: %s' % (field, field_aliases))
            for fa in field_aliases:
                aliases.append(fa)
        # aliases = [field_alias.get_aliases(field) for field in macros_fields]
        # aliases = [alias for sublist in aliases for alias in sublist]

        macros_args_pairs = re.finditer(self.macros_args_pairs, self.body)
        pairs = {}
        for pair in macros_args_pairs:
            pairs[pair.group("token")] = pair.group("value")
        self.logger.debug('Macros args: %s.' % pairs)

        for pair in list(pairs.items()):
            token, value = pair
            values = value.split(',')
            if token in ['earliest', 'latest']:
                value = str(self.get_epoch(values[0]))
            else:
                value = ' OR '.join(['%s=%s' % (token, value) for value in values])
            pairs[token] = value

        otl_pattern = self.read()
        for token in pairs:
            otl_pattern = re.sub(r'\$%s\$' % token, pairs[token], otl_pattern)
        if 'table' not in otl_pattern.split('\n')[-1] and 'fields' not in otl_pattern.split('\n')[-1]:
            table_string = '\n| table _time, %s, %s' % (', '.join(pairs.keys()), ', '.join(aliases))
            otl_pattern = otl_pattern + table_string
        return otl_pattern
