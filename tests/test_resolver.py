import unittest
import parsers.spl_resolver.Resolver as Resolver


class TestResolver(unittest.TestCase):

    def setUp(self) -> None:
        self.maxDiff = None
        self.resolver = Resolver.Resolver(['main1', 'main2'], 0, 0)

    def test_read_some_empty(self):
        spl = """search index=main2 SUCCESS host="h1 bla" OR host="" OR host=h3"""
        target = {'search': ('search index=main2 SUCCESS host="h1 bla" OR host="" OR host=h3', '| read {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1 bla\\" OR host=\\"\\" OR host=\\"h3\\"", "tws": 0, "twf": 0}}'), 'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_read_some_or(self):
        spl = """search index=main2 SUCCESS host="h1" OR host="h2" OR host=h3"""
        target = {'search': ('search index=main2 SUCCESS host="h1" OR host="h2" OR host=h3', '| read {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1\\" OR host=\\"h2\\" OR host=\\"h3\\"", "tws": 0, "twf": 0}}'), 'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_read_many_or(self):
        spl = """search index=main2 SUCCESS host="h1" OR host="h2" OR host=h3 OR host=h4 OR host=h5 OR host=h6 OR host=h7 OR host=h8 OR host=h9 OR host=h10"""
        target = {'search': ('search index=main2 SUCCESS host="h1" OR host="h2" OR host=h3 OR host=h4 OR host=h5 OR host=h6 OR host=h7 OR host=h8 OR host=h9 OR host=h10', '| read {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1\\" OR host=\\"h2\\" OR host=\\"h3\\" OR host=\\"h4\\" OR host=\\"h5\\" OR host=\\"h6\\" OR host=\\"h7\\" OR host=\\"h8\\" OR host=\\"h9\\" OR host=\\"h10\\"", "tws": 0, "twf": 0}}'), 'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_rex(self):
        spl = """search index=main2 SUCCESS | rex field=host "^(?<host>[^\.\:]+).*\:[0-9]" | stats count by host"""
        target = {'search': ('search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host', '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host'), 'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_subsearche_general(self):
        spl = """search index=main FAIL | join host [search index=main2 SUCCESS | stats count by host]"""
        target = {'search': ('search index=main FAIL | join host [search index=main2 SUCCESS | stats count by host]', '| read {"main": {"query": "(_raw like \'%FAIL%\')", "tws": 0, "twf": 0}}| join host subsearch=subsearch_1ae3636e1888e78d8be6893c1e7d569b9289d145bdc8e5d33cdc50aa5bf097e7'), 'subsearches': {'subsearch_1ae3636e1888e78d8be6893c1e7d569b9289d145bdc8e5d33cdc50aa5bf097e7': ('search index=main2 SUCCESS | stats count by host', '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| stats count by host')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_subsearch_with_rex(self):
        spl = """search index=main FAIL | join host [search index=main2 SUCCESS | rex field=host "^(?<host>[^\.\:]+).*\:[0-9]" | stats count by host]"""
        target = {'search': ('search index=main FAIL | join host [search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host]', '| read {"main": {"query": "(_raw like \'%FAIL%\')", "tws": 0, "twf": 0}}| join host subsearch=subsearch_44519941c88171a3bdcf5ffe6ae363eadd3e2969d0209252490e76933ed2af26'), 'subsearches': {'subsearch_44519941c88171a3bdcf5ffe6ae363eadd3e2969d0209252490e76933ed2af26': ('search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host', '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_susbearch_with_return(self):
        spl = """search index=main [search index=main2 | return random_Field]"""
        target = {'search': ('search index=main [search index=main2 | return random_Field]', '| read {"main": {"query": "", "tws": 0, "twf": 0}} subsearch=subsearch_f185051077b589c430cff82cd0156cc3da0e2399b8189a21fe2bd626eeb0467a'), 'subsearches': {'subsearch_f185051077b589c430cff82cd0156cc3da0e2399b8189a21fe2bd626eeb0467a': ('search index=main2 | return random_Field', '| read {"main2": {"query": "", "tws": 0, "twf": 0}}| return random_Field')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_subsearch_with_otoutputlookup(self):
        spl = """search index=test_index | otoutputlookup testoutputlookup.csv"""
        target = {'search': ('search index=test_index | otoutputlookup testoutputlookup.csv', '| read {"test_index": {"query": "", "tws": 0, "twf": 0}}| otoutputlookup testoutputlookup.csv'), 'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_subsearch_with_append(self):
        spl = """search index=test_index | table _time, serialField, random_Field, WordField, junkField| append [search index=test_index junkField="word"]"""
        target = {'search': ('search index=test_index | table _time, serialField, random_Field, WordField, junkField| append [search index=test_index junkField="word"]', '| read {"test_index": {"query": "", "tws": 0, "twf": 0}}| table _time, serialField, random_Field, WordField, junkField| append subsearch=subsearch_110c2b2fb62279fbc26900d477d7a9ca460a566213c797503e701020e366134f'), 'subsearches': {'subsearch_110c2b2fb62279fbc26900d477d7a9ca460a566213c797503e701020e366134f': ('search index=test_index junkField="word"', '| read {"test_index": {"query": "junkField=\\"word\\"", "tws": 0, "twf": 0}}')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_read_several_indexes(self):
        spl = """search index=main1 OR index=main2 SUCCESS"""
        target = {'search': ('search index=main1 OR index=main2 SUCCESS', '| read {"main1": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}, "main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}'), 'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_read_indexes_with_wildcards(self):
        spl = """search index=main* SUCCESS"""
        target = {'search': ('search index=main* SUCCESS', '| read {"main1": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}, "main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}'), 'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)
