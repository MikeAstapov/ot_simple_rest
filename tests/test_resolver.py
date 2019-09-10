import unittest
import parsers.spl_resolver.Resolver as Resolver


class TestResolver(unittest.TestCase):

    def setUp(self) -> None:
        self.maxDiff = None
        self.resolver = Resolver.Resolver(['index1', 'index2'], 0, 0)

    def test_rex(self):
        spl = """search index=main2 SUCCESS | rex field=host "^(?<host>[^\.\:]+).*\:[0-9]" | stats count by host"""
        target = {'search': ('search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host', '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host'), 'subsearches': {}}
        self.assertDictEqual(self.resolver.resolve(spl), target)

    def test_subsearche_general(self):
        spl = """search index=main FAIL | join host [search index=main2 SUCCESS | stats count by host]"""
        target = {'search': ('search index=main FAIL | join host [search index=main2 SUCCESS | stats count by host]', '| read {"main": {"query": "(_raw like \'%FAIL%\')", "tws": 0, "twf": 0}}| join host subsearch=subsearch_1ae3636e1888e78d8be6893c1e7d569b9289d145bdc8e5d33cdc50aa5bf097e7'), 'subsearches': {'subsearch_1ae3636e1888e78d8be6893c1e7d569b9289d145bdc8e5d33cdc50aa5bf097e7': ('search index=main2 SUCCESS | stats count by host', '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| stats count by host')}}
        self.assertDictEqual(self.resolver.resolve(spl), target)

    def test_subsearch_with_rex(self):
        spl = """search index=main FAIL | join host [search index=main2 SUCCESS | rex field=host "^(?<host>[^\.\:]+).*\:[0-9]" | stats count by host]"""
        target = {'search': ('search index=main FAIL | join host [search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host]', '| read {"main": {"query": "(_raw like \'%FAIL%\')", "tws": 0, "twf": 0}}| join host subsearch=subsearch_f4aa5cedb27113c2ef55dad8a8bc09ff3712d3ff801baa6f7bfcdd1e3bea8d58'), 'subsearches': {'subsearch_f4aa5cedb27113c2ef55dad8a8bc09ff3712d3ff801baa6f7bfcdd1e3bea8d58': ('search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host', '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host')}}
        self.assertDictEqual(self.resolver.resolve(spl), target)

    def test_susbearch_with_return(self):
        spl = """search index=main [search index=main2 | return random_Field]"""
        target = {'search': ('search index=main [search index=main2 | return random_Field]', '| read {"main": {"query": "", "tws": 0, "twf": 0}} subsearch=subsearch_f185051077b589c430cff82cd0156cc3da0e2399b8189a21fe2bd626eeb0467a'), 'subsearches': {'subsearch_f185051077b589c430cff82cd0156cc3da0e2399b8189a21fe2bd626eeb0467a': ('search index=main2 | return random_Field', '| read {"main2": {"query": "", "tws": 0, "twf": 0}}| return random_Field')}}
        self.assertDictEqual(self.resolver.resolve(spl), target)
