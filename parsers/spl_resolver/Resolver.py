#!/usr/bin/env python
# -*- coding: utf-8 -*-
# resolver.py
import json
import re
from hashlib import sha256
from parsers.spl_to_sparksql.splunk_parser import SPLtoSQL

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = ["Sergei Ermilov"]
__license__ = ""
__version__ = "0.3.10"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


class Resolver:
    """
    Gets service SPL string from original one. Transforms next commands:

    1. search -> | read "{__fts_json__}"
    2. | otrest endpoint=/any/path/to/api/ -> | otrest subsearch=subsearch_id
    3. any_command [subsearch] -> any_command subsearch=subsearch_id

    This is needed for calculation part of Dispatcher.
    """
    # Patterns for transformation.
    quoted_hide_pattern = r'"(.+?)"'
    quoted_return_pattern = r'_quoted_text_(\w+)'
    subsearch_pattern = r'.+\[(.+?)\]'
    read_pattern_middle = r'\[\s*search (.+?)[\|\]]'
    read_pattern_start = r'^ *search ([^|]+)'
    otrest_pattern = r'otrest[^|]+url\s*?=\s*?([^\|\] ]+)'
    filter_pattern = r'\|\s*search ([^\|$]+)'
    otfrom_pattern = r'otfrom datamodel:?\s*([^\|$]+)'
    otloadjob_id_pattern = r'otloadjob\s+(\d+\.\d+)'
    otloadjob_spl_pattern = r'otloadjob\s+spl="(.+?[^\\])"'

    def __init__(self, indexes, tws, twf, cur=None, sid=None, src_ip=None):
        """
        Init with default available indexes, time window and cursor to DB for DataModels.

        :param indexes: list of default available indexes.
        :param tws: Time Window Start.
        :param twf: Time Window Finish.
        :param cur: Cursor to Postgres DB.
        """
        self.indexes = indexes
        self.tws = tws
        self.twf = twf
        self.cur = cur
        self.sid = sid
        self.src_ip = src_ip
        self.subsearches = {}
        self.hidden_rex = {}
        self.hidden_quoted_text = {}

    def create_subsearch(self, match_object):
        """
        Finds subsearches and transforms original SPL with subsearch id.
        any_command [subsearch] -> any_command subsearch=subsearch_id

        :param match_object: Re match object with original SPL.
        :return: String with replaces of subsearches.
        """
        subsearch_query = match_object.group(1)

        subsearch_query_service = re.sub(self.read_pattern_middle, self.create_read_graph, subsearch_query)
        subsearch_query_service = re.sub(self.read_pattern_start, self.create_read_graph, subsearch_query_service)

        _subsearch_query = re.sub(self.quoted_return_pattern, self.return_quoted, subsearch_query)
        _subsearch_query_service = re.sub(self.quoted_return_pattern, self.return_quoted, subsearch_query_service)

        subsearch_sha256 = sha256(_subsearch_query.strip().encode('utf-8')).hexdigest()

        self.subsearches['subsearch_%s' % subsearch_sha256] = (_subsearch_query, _subsearch_query_service)
        return match_object.group(0).replace(
            '[%s]' % subsearch_query, 'subsearch=subsearch_%s' % subsearch_sha256
        )

    def create_otrest(self, match_object):
        """
        Finds "| otrest endpoint=/any/path/to/api/" command and transforms it to service form.
        | otrest endpoint=/any/path/to/api/-> | otrest subsearch=subsearch_id

        :param match_object: Re match object with original SPL.
        :return: String with replaces of subsearches.
        """
        otrest_sha256 = sha256(match_object.group(0).strip().encode('utf-8')).hexdigest()
        otrest_service = '| otrest subsearch=subsearch_%s' % otrest_sha256
        self.subsearches['subsearch_%s' % otrest_sha256] = ('| %s' % match_object.group(0), otrest_service)
        return otrest_service

    @staticmethod
    def hide_subsearch_before_read(query):

        subsearch = re.findall(r' subsearch=subsearch_\w+', query)
        if subsearch:
            subsearch = subsearch[0]
        else:
            subsearch = ''

        query = query.replace(subsearch, "")
        return query, subsearch

    def create_read_graph(self, match_object):
        """
        Finds "search __fts_query__" and transforms it to service form.
        search -> | read "{__fts_json__}"

        :param match_object: Re match object with original SPL.
        :return: String with replaces of FTS part.
        """
        query = match_object.group(1)

        query, subsearch = self.hide_subsearch_before_read(query)
        graph = SPLtoSQL.parse_read(query, av_indexes=self.indexes, tws=self.tws, twf=self.twf)
        return ('| read %s%s' % (json.dumps(graph), subsearch)) #.replace('\\"\\"','\\"')

    @staticmethod
    def create_filter_graph(match_object):
        """
        Finds "| search __filter_query__" and transforms it to service form.
        | search -> | filter "{__filter_json__}"

        :param match_object: Re match object with original SPL.
        :return: String with replaces of filter part.
        """
        query = match_object.group(1)
        graph = SPLtoSQL.parse_filter(query)
        return '| filter %s' % json.dumps(graph)

    def create_datamodels(self, match_object):
        """
        Transforms "| otfrom datamodel __NAME__" to "| search (index=__INDEX__ source=__SOURCE__)" or something like.

        :param match_object: Re match object with original SPL.
        :return: String with replaces of datamodel part.
        """
        datamodel_name = match_object.group(1)
        get_datamodel_stm = """SELECT search FROM DataModels WHERE name = '%s';"""
        self.cur.execute(get_datamodel_stm % (datamodel_name,))
        fetch = self.cur.fetchone()
        if fetch:
            query = fetch[0]
        else:
            raise Exception('Can\'t find DATAMODEL. Update DATAMODELS DB or fix the name.')
        return query[1:] if query[0] == '|' else query

    def create_otloadjob_id(self, match_object):
        """
        Transforms "| otloadjob __SID__" to "| otloadjob subsearch="subsearch___sha256__" ".

        :param match_object: Re match object with original SPL.
        :return: String with replaces of datamodel part.
        """
        sid = match_object.group(1)
        get_spl_stm = """SELECT spl FROM SplunkSIDs WHERE sid=%s AND src_ip='%s';"""
        self.cur.execute(get_spl_stm % (sid, self.src_ip))
        fetch = self.cur.fetchone()
        if fetch:
            spl = fetch[0]
            otloadjob_sha256 = sha256(spl.strip().encode('utf-8')).hexdigest()
            otloadjob_service = '| otloadjob subsearch=subsearch_%s' % otloadjob_sha256
            self.subsearches['subsearch_%s' % otloadjob_sha256] = (spl, otloadjob_service)
            return otloadjob_service
        else:
            raise Exception('Job sid is not found.')

    def create_otloadjob_spl(self, match_object):
        """
        Transforms '| otloadjob spl="__SPL__"' to '| otloadjob subsearch="subsearch___sha256__"'.

        :param match_object: Re match object with original SPL.
        :return: String with replaces of datamodel part.
        """
        spl = match_object.group(1)
        spl = spl.replace('\\"', '"')
        otloadjob_sha256 = sha256(spl.strip().encode('utf-8')).hexdigest()
        otloadjob_service = '| otloadjob subsearch=subsearch_%s' % otloadjob_sha256
        self.subsearches['subsearch_%s' % otloadjob_sha256] = (spl, otloadjob_service)
        return otloadjob_service

    def hide_quoted(self, match_object):

        quoted_text = match_object.group(1)
        quoted_text_sha256 = sha256(quoted_text.encode('utf-8')).hexdigest()
        self.hidden_quoted_text[quoted_text_sha256] = quoted_text

        return match_object.group(0).replace(quoted_text, '_quoted_text_%s' % quoted_text_sha256)

    def return_quoted(self, match_object):
        quoted_text_sha256 = match_object.group(1)
        return match_object.group(0).replace(
            '_quoted_text_%s' % quoted_text_sha256,
            self.hidden_quoted_text[quoted_text_sha256]
        )

    def resolve(self, spl):
        """
        Finds and replaces service patterns of original SPL.

        :param spl: original SPL.
        :return: dict with search query params.
        """

        _spl = re.sub(self.quoted_hide_pattern, self.hide_quoted, spl)
        _spl = (_spl, 1)
        while _spl[1]:
            _spl = re.subn(self.subsearch_pattern, self.create_subsearch, _spl[0])

        _spl = re.sub(self.quoted_return_pattern, self.return_quoted, _spl[0])

        _spl = re.sub(self.otfrom_pattern, self.create_datamodels, _spl)

        _spl = re.sub(self.read_pattern_middle, self.create_read_graph, _spl)
        _spl = re.sub(self.read_pattern_start, self.create_read_graph, _spl)

        _spl = re.sub(self.otrest_pattern, self.create_otrest, _spl)
        _spl = re.sub(self.filter_pattern, self.create_filter_graph, _spl)
        _spl = re.sub(self.otloadjob_id_pattern, self.create_otloadjob_id, _spl)
        _spl = re.sub(self.otloadjob_spl_pattern, self.create_otloadjob_spl, _spl)
        return {'search': (spl, _spl), 'subsearches': self.subsearches}
