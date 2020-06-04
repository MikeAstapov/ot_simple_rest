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
    token_pattern = r'(\$.+?\$)'

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
        elif type(utc_time) is float:
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
        if 'earliest' not in pairs:
            pairs['earliest'] = '0'
        if 'latest' not in pairs:
            pairs['latest'] = str(datetime.now().timestamp())
        self.logger.debug('Macros tokens: %s.' % pairs)

        otl_pattern = self.read()
        self.logger.debug('Macros %s. Body: %s. Pattern: %s.' % (self.name, self.body, otl_pattern))
        for token in pairs:
            otl_pattern = re.sub(r'\$%s\$' % token, pairs[token], otl_pattern)

        unused_tokens = re.findall(self.token_pattern, otl_pattern)
        for unused_token in unused_tokens:
            otl_pattern = re.sub(unused_token, '%s=*' % unused_token.replace('$', ''), otl_pattern)

        if aliases:
            del pairs['earliest']
            del pairs['latest']
            table_string = '\n| table _time, %s, %s' % (', '.join(pairs.keys()), ', '.join(aliases))
            otl_pattern = otl_pattern + table_string
        self.logger.debug('Macros %s. Body: %s. OTL: %s.' % (self.name, self.body, otl_pattern))
        return otl_pattern
