import unittest
import parsers.spl_resolver.Resolver as Resolver


class TestResolver(unittest.TestCase):

    def setUp(self) -> None:
        self.maxDiff = None
        self.resolver = Resolver.Resolver(['index1', 'index2'], 0,0)

    def test_subsearche_general(self):
        spl = """search index=main FAIL | join host [| search index=main2 SUCCESS | stats count by host]"""
        target = {'search': ('search index=main FAIL | join host [| search index=main2 SUCCESS | stats count by host]', '| read {"main": {"query": "(_raw like \'%FAIL%\')", "tws": 0, "twf": 0}}| join host subsearch=subsearch_f7dab1a6e843ef52348db7d572fd1694fc44029e1e370e7da7b89d8cc7092846'), 'subsearches': {'subsearch_f7dab1a6e843ef52348db7d572fd1694fc44029e1e370e7da7b89d8cc7092846': ('| search index=main2 SUCCESS | stats count by host', '| search index=main2 SUCCESS | stats count by host')}}
        self.assertDictEqual(self.resolver.resolve(spl), target)

    def test_subsearch_with_rex(self):
        spl = """search index=main FAIL | join host [| search index=main2 SUCCESS | rex field=host "^(?<host>[^\.\:]+).*\:[0-9]" | stats count by host]"""
        target = {'search': ('search index=main FAIL | join host [| search index=main2 SUCCESS | stats count by host]', '| read {"main": {"query": "(_raw like \'%FAIL%\')", "tws": 0, "twf": 0}}| join host subsearch=subsearch_f7dab1a6e843ef52348db7d572fd1694fc44029e1e370e7da7b89d8cc7092846'), 'subsearches': {'subsearch_f7dab1a6e843ef52348db7d572fd1694fc44029e1e370e7da7b89d8cc7092846': ('| search index=main2 SUCCESS | stats count by host', '| search index=main2 SUCCESS | stats count by host')}}
        self.assertDictEqual(self.resolver.resolve(spl), target)
