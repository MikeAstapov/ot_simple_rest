import unittest
import parsers.spl_resolver.Resolver as Resolver


class TestResolver(unittest.TestCase):

    def setUp(self) -> None:
        self.maxDiff = None
        self.resolver = Resolver.Resolver(['index1', 'index2'], 0, 0)

    def test_filter_with_wildcards(self):
        spl = """search index=main | search alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*" summary="*kb.main*" """
        target = {'search': ('search index=main | search alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*" summary="*kb.main*" ', '| read {"main": {"query": "", "tws": 0, "twf": 0}}| filter {}" '), 'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_filter_without_wildcards(self):
        spl = """search index=main | search alert="main" | stats count"""
        target = {'search': ('search index=main | search alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*" summary="*kb.main*" ', '| read {"main": {"query": "", "tws": 0, "twf": 0}}| filter {}" '), 'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_rex(self):
        spl = """search index=main2 SUCCESS | rex field=host "^(?<host>[^\.\:]+).*\:[0-9]" | stats count by host"""
        target = {'search': ('search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host', '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host'), 'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_subsearch_general(self):
        spl = """search index=main FAIL | join host [search index=main2 SUCCESS | stats count by host]"""
        target = {'search': ('search index=main FAIL | join host [search index=main2 SUCCESS | stats count by host]', '| read {"main": {"query": "(_raw like \'%FAIL%\')", "tws": 0, "twf": 0}}| join host subsearch=subsearch_1ae3636e1888e78d8be6893c1e7d569b9289d145bdc8e5d33cdc50aa5bf097e7'), 'subsearches': {'subsearch_1ae3636e1888e78d8be6893c1e7d569b9289d145bdc8e5d33cdc50aa5bf097e7': ('search index=main2 SUCCESS | stats count by host', '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| stats count by host')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_subsearch_with_rex(self):
        spl = """search index=main FAIL | join host [search index=main2 SUCCESS | rex field=host "^(?<host>[^\.\:]+).*\:[0-9]" | stats count by host]"""
        target = {'search': ('search index=main FAIL | join host [search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host]', '| read {"main": {"query": "(_raw like \'%FAIL%\')", "tws": 0, "twf": 0}}| join host subsearch=subsearch_44519941c88171a3bdcf5ffe6ae363eadd3e2969d0209252490e76933ed2af26'), 'subsearches': {'subsearch_44519941c88171a3bdcf5ffe6ae363eadd3e2969d0209252490e76933ed2af26': ('search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host', '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_subsearch_with_return(self):
        spl = """search index=main [search index=main2 | return random_Field]"""
        target = {'search': ('search index=main [search index=main2 | return random_Field]', '| read {"main": {"query": "", "tws": 0, "twf": 0}} subsearch=subsearch_f185051077b589c430cff82cd0156cc3da0e2399b8189a21fe2bd626eeb0467a'), 'subsearches': {'subsearch_f185051077b589c430cff82cd0156cc3da0e2399b8189a21fe2bd626eeb0467a': ('search index=main2 | return random_Field', '| read {"main2": {"query": "", "tws": 0, "twf": 0}}| return random_Field')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_subsearch_with_otoutputlookup(self):
        spl = """search index=test_index | otoutputlookup testoutputlookup.csv"""
        target = {'search': ('search index=test_index | otoutputlookup testoutputlookup.csv', '| read {"test_index": {"query": "", "tws": 0, "twf": 0}}| otoutputlookup testoutputlookup.csv'), 'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_subsearch_with_append(self):
        spl = """search index=test_index | table _time, serialField, random_Field, WordField, junkField| append [search index=test_index junkField="word"]"""
        target = {'search': ('search index=test_index | table _time, serialField, random_Field, WordField, junkField| append [search index=test_index junkField="word"]', '| read {"test_index": {"query": "", "tws": 0, "twf": 0}}| table _time, serialField, random_Field, WordField, junkField| append subsearch=subsearch_110c2b2fb62279fbc26900d477d7a9ca460a566213c797503e701020e366134f'), 'subsearches': {'subsearch_110c2b2fb62279fbc26900d477d7a9ca460a566213c797503e701020e366134f': ('search index=test_index junkField="word"', '| read {"test_index": {"query": "junkField=\\"word\\"", "tws": 0, "twf": 0}}')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_otloadjob_spl(self):
        spl = """| ot ttl=60 | otloadjob spl="search index=alerts sourcetype!=alert_metadata | fields - _raw | dedup full_id | where alert=\\"pprb_*\\" status!=\\"*resolved\\" status!=\\"suppressed\\" app=\\"*\\" urgency=\\"*\\" summary=\\"*kb.main*\\"| stats count(alert) by alert" | simple"""
        target = {'search': ('| ot ttl=60 | otloadjob spl="search index=alerts sourcetype!=alert_metadata | fields - _raw | dedup full_id | where alert=\\"pprb_*\\" status!=\\"*resolved\\" status!=\\"suppressed\\" app=\\"*\\" urgency=\\"*\\" summary=\\"*kb.main*\\"| stats count(alert) by alert" | simple', '| ot ttl=60 | otloadjob subsearch=subsearch_e08d9facf3d89bc4a22b743303e6aedaf983debb8ebcda68338cd09b0744047a | simple'), 'subsearches': {'subsearch_e08d9facf3d89bc4a22b743303e6aedaf983debb8ebcda68338cd09b0744047a': ('search index=alerts sourcetype!=alert_metadata | fields - _raw | dedup full_id | where alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*" summary="*kb.main*"| stats count(alert) by alert', '| read {"alerts": {"query": "sourcetype!=\\"alert_metadata\\"", "tws": 0, "twf": 0}}| fields - _raw | dedup full_id | where alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*" summary="*kb.main*"| stats count(alert) by alert')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_pain_subsearch_1(self):
        spl = """| ot ttl=60 | search index=pprbcore_business audit.state.name=* 
| bucket _time span=1m 
| dedup _time,host 
| fields _time, host 
| join host 
    [| otinputlookup pprb_hosts_list.csv 
    | where module_name="audit2" and stend="prom" and like(kontur,"%") 
    | eval host=mvzip(host_name,p_custodian, ":")] 
| stats distinct_count(host) as prc by _time 
| eval 
    [| otinputlookup pprb_hosts_list.csv 
    | where module_name="audit2" and stend="prom" and like(kontur,"%") 
    | stats distinct_count(host_name) as total_hosts 
    | return 1 total_hosts] 
| eval prc=round(100*prc /total_hosts, 2) 
| timechart span=1m min(prc) as "% доступности"
| eval baseline = 100 | simple"""
        target = {}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

