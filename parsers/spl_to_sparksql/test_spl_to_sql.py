import ast
from splunk_parser import SPLtoSQL

def test_pprb_dashboard():
    f = open("tests.txt", "r")
    for test in f:
        print(test.split("\t"))
        test_case = test.split("\t")[0]
        result = test.split("\t")[1]
        assert  SPLtoSQL.parse(test_case) == ast.literal_eval(result)
