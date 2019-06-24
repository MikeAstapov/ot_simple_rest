#!/usr/bin/env python
# -*- coding: utf-8 -*-
# resolver.py
import json
import re
from hashlib import sha256
from parsers.spl_to_sparksql.splunk_parser import SPLtoSQL


class Resolver:

    subsearch_pattern = r'.+\[(.+?)\]'
    read_pattern_middle = r'\[\s*search (.+?)[\|\]]'
    read_pattern_start = r'^ *search ([^|]+)'

    query_replacements = {
        "\\[": "&&OPENSQUAREBRACKET",
        "\\]": "&&CLOSESQUAREBRACKET"
    }

    inverted_query_replacements = {v: k for (k, v) in query_replacements.items()}

    def __init__(self):

        self.subsearches = {}

    def create_subsearch(self, match_object):
        subsearch_sha256 = sha256(match_object.group(1).strip().encode('utf-8')).hexdigest()
        subsearch_query = match_object.group(1)
        for replacement in self.inverted_query_replacements:
            subsearch_query = subsearch_query.replace(replacement, self.inverted_query_replacements[replacement])

        subsearch_query_service = re.sub(self.read_pattern_middle, self.create_read_graph, subsearch_query)
        subsearch_query_service = re.sub(self.read_pattern_start, self.create_read_graph, subsearch_query_service)

        self.subsearches['subsearch_%s' % subsearch_sha256] = (subsearch_query, subsearch_query_service)
        return match_object.group(0).replace('[%s]' % match_object.group(1), 'subsearch=subsearch_%s' % subsearch_sha256)

    @staticmethod
    def create_read_graph(match_object):
        query = match_object.group(1)
        graph = SPLtoSQL.parse(query)
        return '| read %s' % json.dumps(graph)

    def resolve(self, spl):
        spl = spl.replace('\n', ' ')
        for replacement in self.query_replacements:
            spl = spl.replace(replacement, self.query_replacements[replacement])

        _spl = (spl, 1)
        while _spl[1]:
            _spl = re.subn(self.subsearch_pattern, self.create_subsearch, _spl[0])

        _spl = _spl[0]

        for replacement in self.inverted_query_replacements:
            _spl = _spl.replace(replacement, self.inverted_query_replacements[replacement])

        _spl = re.sub(self.read_pattern_middle, self.create_read_graph, _spl)
        _spl = re.sub(self.read_pattern_start, self.create_read_graph, _spl)

        return {'search': (spl, _spl), 'subsearches': self.subsearches}


if __name__ == '__main__':

    spl1 = """search index=main (GET AND 200) OR (POST AND 404) "asd   ertert xbfert"| timechart span=1w max(val) as val | 
    join val [search index=notmain 200 | table _time, val, _span] | join host [search index=second 400] | table _raw]"""

    spl2 = """search index=1st | search hua|  join type=left val [ search index=2nd | stats count by val |
    join type=left val [ search index=3rd | stats max(val) as val, min(val) as minval ] | table 2] | table end"""

    spl3 = """search index=asd"""

    spl4 = """|makeresults |search index=main (GET AND 200) OR (POST AND 404) "asd   ertert xbfert"| timechart span=1w max(val) as val | 
    join val [search index=notmain 200 | table _time, val, _span] | join host [search index=second 400] | table _raw]"""

    resolver = Resolver()
    print(resolver.resolve(spl1))

    resolver = Resolver()
    print(resolver.resolve(spl2))

    resolver = Resolver()
    print(resolver.resolve(spl3))

    resolver = Resolver()
    print(resolver.resolve(spl4))
