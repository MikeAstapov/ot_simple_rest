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
__version__ = "0.3.0"
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
    subsearch_pattern = r'.+\[(.+?)\]'
    read_pattern_middle = r'\[\s*search (.+?)[\|\]]'
    read_pattern_start = r'^ *search ([^|]+)'
    otrest_pattern = r'otrest\s+endpoint\s*?=\s*?"(\S+?)"'
    filter_pattern = r'\|\s*search ([^\|$]+)'
    otfrom_pattern = r'otfrom datamodel:?\s*([^\|$]+)'
    otloadjob_pattern = r'otloadjob\s+(\d+\.\d+)'
    otrestdo_pattern = r'otrestdo\s+endpoint\s*?=\s*?"(\S+?)"'

    # Service structures for escaping special symbols in original SPL queries.
    query_replacements = {
        "\\[": "&&OPENSQUAREBRACKET",
        "\\]": "&&CLOSESQUAREBRACKET"
    }

    inverted_query_replacements = {v: k for (k, v) in query_replacements.items()}

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

    def create_subsearch(self, match_object):
        """
        Finds subsearches and transforms original SPL with subsearch id.
        any_command [subsearch] -> any_command subsearch=subsearch_id

        :param match_object: Re match object with original SPL.
        :return: String with replaces of subsearches.
        """
        subsearch_sha256 = sha256(match_object.group(1).strip().encode('utf-8')).hexdigest()
        subsearch_query = match_object.group(1)
        for replacement in self.inverted_query_replacements:
            subsearch_query = subsearch_query.replace(replacement, self.inverted_query_replacements[replacement])

        subsearch_query_service = re.sub(self.read_pattern_middle, self.create_read_graph, subsearch_query)
        subsearch_query_service = re.sub(self.read_pattern_start, self.create_read_graph, subsearch_query_service)

        self.subsearches['subsearch_%s' % subsearch_sha256] = (subsearch_query, subsearch_query_service)
        return match_object.group(0).replace('[%s]' % match_object.group(1), 'subsearch=subsearch_%s' % subsearch_sha256)

    def create_otrest(self, match_object):
        """
        Finds "| otrest endpoint=/any/path/to/api/" command and transforms it to service form.
        | otrest endpoint=/any/path/to/api/-> | otrest subsearch=subsearch_id

        :param match_object: Re match object with original SPL.
        :return: String with replaces of subsearches.
        """
        otrest_sha256 = sha256(match_object.group(1).strip().encode('utf-8')).hexdigest()
        otrest_service = '| otrest subsearch=subsearch_%s' % otrest_sha256
        self.subsearches['subsearch_%s' % otrest_sha256] = ('| %s' % match_object.group(0), otrest_service)
        return otrest_service

    def create_read_graph(self, match_object):
        """
        Finds "search __fts_query__" and transforms it to service form.
        search -> | read "{__fts_json__}"

        :param match_object: Re match object with original SPL.
        :return: String with replaces of FTS part.
        """
        query = match_object.group(1)
        graph = SPLtoSQL.parse_read(query, av_indexes=self.indexes, tws=self.tws, twf=self.twf)
        return '| read %s' % json.dumps(graph)

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

    def create_otloadjob(self, match_object):
        """
        Transforms "| otloadjob __SID__" to "| otloadjob spl="__SPL__" ".

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

    def resolve(self, spl):
        """
        Finds and replaces service patterns of original SPL.

        :param spl: original SPL.
        :return: dict with search query params.
        """
        spl = spl.replace('\n', ' ')
        for replacement in self.query_replacements:
            spl = spl.replace(replacement, self.query_replacements[replacement])

        _spl = (spl, 1)
        while _spl[1]:
            _spl = re.subn(self.subsearch_pattern, self.create_subsearch, _spl[0])

        _spl = _spl[0]

        for replacement in self.inverted_query_replacements:
            _spl = _spl.replace(replacement, self.inverted_query_replacements[replacement])

        _spl = re.sub(self.otfrom_pattern, self.create_datamodels, _spl)

        _spl = re.sub(self.read_pattern_middle, self.create_read_graph, _spl)
        _spl = re.sub(self.read_pattern_start, self.create_read_graph, _spl)

        _spl = re.sub(self.otrest_pattern, self.create_otrest, _spl)
        _spl = re.sub(self.otrestdo_pattern, self.create_otrest, _spl)
        _spl = re.sub(self.filter_pattern, self.create_filter_graph, _spl)
        _spl = re.sub(self.otloadjob_pattern, self.create_otloadjob, _spl)

        return {'search': (spl, _spl), 'subsearches': self.subsearches}


if __name__ == '__main__':

    spl1 = """search index=main (GET AND 200) OR (POST AND 404) "asd   ertert xbfert"| timechart span=1w max(val)
     as val | 
    join val [search index=notmain 200 | table _time, val, _span] | join host [search index=second 400] | table _raw]"""

    spl2 = """search index=1st | search hua|  join type=left val [ search index=2nd | stats count by val |
    join type=left val [ search index=3rd | stats max(val) as val, min(val) as minval ] | table 2] | table end"""

    spl3 = """search index=asd"""

    spl4 = """|makeresults |search index=main (GET AND 200) OR (POST AND 404) "asd   ertert xbfert"| timechart span=1w
     max(val) as val | 
    join val [search index=notmain 200 | table _time, val, _span] | join host [search index=second 400] | table _raw]"""

    spl5 = """otrest endpoint="/services/search/jobs/" | stats count """

    spl6 = """index = main | search 404 AND POST | table * """

    spl7 = """index = main | foreach GBL_CPU_TOTAL_UTIL [eval &lt;&lt;FIELD&gt;&gt;=round('&lt;&lt;FIELD&gt;&gt;',2)]
     | stats count"""

    spl8 = """otfrom datamodel: NAME | stats count"""

    spl9 = """otloadjob 123.25 | stats count"""

    default_indexes = ['main', '_internal']

    resolver = Resolver(default_indexes, 0, 0)
    print(resolver.resolve(spl1))

    resolver = Resolver(default_indexes, 0, 0)
    print(resolver.resolve(spl2))

    resolver = Resolver(default_indexes, 0, 0)
    print(resolver.resolve(spl3))

    resolver = Resolver(default_indexes, 0, 0)
    print(resolver.resolve(spl4))

    resolver = Resolver(default_indexes, 0, 0)
    print(resolver.resolve(spl5))

    resolver = Resolver(default_indexes, 0, 0)
    print(resolver.resolve(spl6))

    resolver = Resolver(default_indexes, 0, 0)
    print(resolver.resolve(spl7))

    resolver = Resolver(default_indexes, 0, 0)
    print(resolver.resolve(spl8))

    resolver = Resolver(default_indexes, 0, 0)
    print(resolver.resolve(spl9))
