import ast
from splunk_parser import SPLtoSQL

def test_pprb_dashboard():
    av_indexes = ['hpsm_db', 'incidents_local', 'hpsm_db_stat', 'ezsm_events_hist', 'kssh_db_esbpslog', 'scom', 'scomab','scomcd', 'scomfe', 'tivoliab', 'tivolicd', 'tivoli']

    f = open("tests.txt", "r")
    for test in f:
        #print(test.split("\t"))
        test_case = test.split("\t")[0]
        result = test.split("\t")[1]
        assert  SPLtoSQL.parse_read(test_case, av_indexes) == ast.literal_eval(result)
