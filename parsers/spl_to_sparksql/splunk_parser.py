import re
import json
from lark import Lark, Transformer, Tree
from datetime import timedelta
from datetime import datetime

def inverted(s):
    return "!" + s

class SPLtoSQL:
    class BaseEvalExpressions(Transformer):
        def __init__(self):
            super().__init__()

        def le_not(self, args):
            return "NOT"

        def le_or(self, args):
            return 'OR'

        def le_and(self, args):
            return 'AND'

        def indexexpression(self, args):
            if args[0] == "NOT":
                return "NOT"
            if args[0][0] == '"' and args[0][-1] == '"':
                return "(_raw rlike '" + args[0][1:-1] + "')"
            if "*" in args[0]:
                replaced = re.escape(args[0]).replace("\*", ".*")
                return "(_raw rlike '" + replaced + "')"
            return "(_raw like '%" + args[0] + "%')"

        def logicalexpression(self, args):
            pass

        def comparisonexpression(self, args):
            striped = str(args[2]).strip("\"\'")
            if "*" in striped:
                replaced = striped.replace('*', '[a-zA-Z0-9а-яА-Я_*-. ]*')
                return args[0] + " rlike '" + replaced + "'"
            return args[0] + args[1] + striped

        def leftb(self, args):
            return "("

        def rightb(self, args):
            return ")"


    class FilterEvalExpression(BaseEvalExpressions):
        def logicalexpression(self, args):
            if len(args) >= 2:
                if args[0] == "NOT":
                    return inverted(args[1])
                if (args[0]) == "(" and args[-1] == ")":
                    flag = False
                    return "(" + args[1] + ")"
                if (args[1]) == "OR":
                    return args[0] + " OR " + args[2]
                if (args[1]) == "AND":
                    return args[0] + " AND " + args[2]
                return args[0] + " AND " + args[1]
            return args[0]

    class SearchEvalExpression(BaseEvalExpressions):
        def __init__(self):
            self.indexes = {}
            super().__init__()

        def indexspecifier(self, args):
            index_name = str(args[0]).strip("\"\'")
            print(index_name)
            self.indexes[index_name] = ""
            return "index=" + index_name

        def logicalexpression(self, args):
            if len(args) >= 2:
                if args[0] == "NOT":
                    return inverted(args[1])
                if (args[0]) == "(" and args[-1] == ")":
                    flag = False
                    for index in self.indexes.keys():
                        temp = "index="+str(index)
                        if temp in args[1]:
                            self.indexes[index] += args[1]
                            flag = True
                    if flag:
                        return ""
                    return "(" + args[1] + ")"
                if (args[1]) == "OR":
                    return args[0] + " OR " + args[2]
                if (args[1]) == "AND":
                    return args[0] + " AND " + args[2]
                return args[0] + " AND " + args[1]
            if args[0] == None:
                 return ""
            return args[0]

    @staticmethod
    def get_timestamp(time):
        if time == "now":
            return int(datetime.now().timestamp())
        regex = r"(-|\+|^)(\d+)(s|m|h|d|w|M|y)"
        result =  re.match(regex, time)
        if result is not None:
            diff_num = int(result.group(2))
            dict_delta = {
            's': timedelta(seconds=diff_num),
            'm': timedelta(minutes=diff_num),
            'h': timedelta(hours=diff_num),
            'd': timedelta(days=diff_num),
            'w': timedelta(weeks=diff_num),
            'M': timedelta(weeks=4*diff_num),
            'y': timedelta(weeks=52*diff_num)
			}
            now = datetime.now()
            delta = dict_delta[result.group(3)]
            print(delta)
            if result.group(1) == "-":
                 res_time = now - delta
            else:
                 res_time = now + delta
            return int(res_time.timestamp())
        return None

    @staticmethod
    def removetime(spl, tws, twf):
         _tws = tws
         _twf = twf
         regex = r"(earliest|latest)=([a-zA-Z0-9_*-]+)"
         for (time_modifier, time) in re.findall(regex, spl):
             print(time_modifier, time) 
             if (time_modifier == "earliest"):
                 _tws = SPLtoSQL.get_timestamp(time)
             if (time_modifier == "latest"):
                 _twf = SPLtoSQL.get_timestamp(time)
         service_spl = re.sub(regex, "", spl)
         return (service_spl, _tws, _twf)

    @staticmethod
    def parse_read(spl, av_indexes, tws , twf):
        lark = Lark('''start:_le
             _le:  logicalexpression
             logicalexpression: leftb _le rightb
             | le_not logicalexpression
             | _le [le_and|le_or] _le
             | _searchmodifier
             | indexexpression
             | comparisonexpression
             _searchmodifier.2:  indexspecifier
             indexspecifier.2: "index" "=" STRING_INDEX
             indexexpression.3:  FIELD
             comparisonexpression: STRING_INDEX CMP VALUE
             TIME_MODIFIER: "earliest" | "latest"
             FIELD: /(?:\"(.*?|[^\\"])\")|[a-zA-Z0-9_*-.]+/
             STRING_INDEX:/[a-zA-Z0-9_*-."']+/
             CMP:"="|"!="|"<"|"<="|">"|">="
             VALUE: /(?:\"(.*?)\")/ | NUM |  TERM
             TERM: /[a-zA-Z0-9_*-]+/
             NUM: /-?\d+(?:\.\d+)*/
             le_or.4: "OR"
             le_and.4: "AND"
             le_not.4: "NOT"
             //EQUAL: "="
             leftb.5: "("
             rightb.5: ")"
             %import common.WORD   // imports from terminal library
             %ignore " "           // Disregard spaces in text
         ''', parser='earley', debug=True)
        (spl_time, _tws, _twf) = SPLtoSQL.removetime(spl, tws, twf)
        tree = lark.parse(spl_time)
        evalexpr = SPLtoSQL.SearchEvalExpression()
        tree2 = evalexpr.transform(tree)
        st2 = tree2.children[0]
        indexes = evalexpr.indexes
        for index in indexes.keys():
            temp = "index="+str(index)
            if temp in st2:
                indexes[index] += st2
        for key in indexes:
            regex = r'(AND|OR)*\s*index=[\w\*]*\s*(AND|OR)?'
            indexes[key] = re.sub(regex, '', indexes[key])
            indexes[key] = indexes[key].strip()
        result = {} 
        for key in indexes:
            if '*' in key:
                regex = key.replace('*',r"(\w)*")
                pattern = re.compile(regex)
                for index in av_indexes:
                    if pattern.match(index):
                       result[index] = indexes[key]
            else:
                result[key] = indexes[key]
        map_with_time = {}
        for key in result:
            map_with_time[key] = {"query": result[key], "tws": _tws, "twf": _twf}
        return map_with_time

    @staticmethod
    def parse_filter(spl):
        lark = Lark('''start:_le
             _le:  logicalexpression
             logicalexpression: leftb _le rightb
             | le_not logicalexpression
             | _le [le_and|le_or] _le
             |  indexexpression
             |  comparisonexpression
             indexexpression.3:  FIELD
             comparisonexpression: STRING_INDEX CMP VALUE
             FIELD: /(?:\"(.*|[^\\"])\")|[a-zA-Z0-9_*-.]+/
             STRING_INDEX:/[a-zA-Z0-9_*-.]+/
             CMP:"="|"!="|"<"|"<="|">"|">="
             VALUE: /(?:\"(.*?)\")/ |TERM | NUM
             TERM: /[a-zA-Z0-9_*-]+/
             NUM: /-?\d+(?:\.\d+)*/
             le_or.4: "OR"
             le_and.4: "AND"
             le_not.4: "NOT"
             //EQUAL: "="
             leftb.5: "("
             rightb.5: ")"
             %import common.WORD   // imports from terminal library
             %ignore " "           // Disregard spaces in text
         ''', parser='earley', debug=True)

        tree = lark.parse(spl)
        evalexpr = SPLtoSQL.FilterEvalExpression()
        tree2 = evalexpr.transform(tree)
        st2 = tree2.children[0]
        result = {}
        result["query"] = st2
        return result
