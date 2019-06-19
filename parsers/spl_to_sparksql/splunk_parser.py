import json
import re
from lark import Lark, Transformer


def inverted(s):
    return "!" + s


class SPLtoSQL:

    class EvalExpressions(Transformer):
        def __init__(self):
            self.indexes = {}
            super().__init__()

        def nott(self, args):
            return "NOT"

        def indexspecifier(self, args):
            self.indexes[str(args[0])] = ""
            return "index=" + args[0]

        def indexexpression(self, args):
            if args[0] == "NOT":
                return "NOT"
            if args[0][0] == '"' and args[0][-1] == '"':
                return "(_raw like '%" + args[0][1:-1] + "%')"
            return "(_raw like '%" + args[0] + "%')"

        def comparisonexpression(self, args):
            if "*" in args[2]:
                return args[0] + " rlike " + args[2]
            return args[0] + args[1] + args[2]

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
            return args[0]

        def leftb(self, args):
            return "("

        def rightb(self, args):
            return ")"

        def orrr(self, args):
            return 'OR'

        def andd(self, args):
            return 'AND'

    @staticmethod
    def parse(spl):
        lark = Lark(r'''start:_le
             _le:  logicalexpression
             logicalexpression: leftb _le rightb
             | nott logicalexpression
             | _le [andd|orrr] _le
             | _searchmodifier
             |  indexexpression
             |  comparisonexpression
             _searchmodifier.2:  indexspecifier
             indexspecifier.2: "index=" STRING_INDEX
             indexexpression.3:  FIELD
             comparisonexpression: STRING_INDEX CMP VALUE
             FIELD: /(?:\"(.*?)\")|[a-zA-Z0-9_*-]+/
             //STRING: /.+/
             STRING_INDEX:/[a-zA-Z0-9_*-.]+/
             CMP:"="|"!="|"<"|"<="|">"|">="
             VALUE: /(?:\"(.*?)\")/ | NUM |  TERM
             TERM: /[a-zA-Z_*-]+/
             NUM: /-?\d+(?:\.\d+)*/
             orrr.4: "OR"
             andd.4: "AND"
             nott.4: "NOT"
             //EQUAL: "="
             leftb.5: "("
             rightb.5: ")"
             %import common.WORD   // imports from terminal library
             %ignore " "           // Disregard spaces in text
         ''', parser='earley', debug=True)

        tree = lark.parse(spl)
#        print(tree.pretty())
        evalexpr = SPLtoSQL.EvalExpressions()
        tree2 = evalexpr.transform(tree)
        st2 = tree2.children[0]
        indexes = evalexpr.indexes
        for index in indexes.keys():
            temp = "index="+str(index)
            if temp in st2:
                indexes[index] += st2
        for key in indexes:
            regex = r'(AND|OR)*\s*index=\w+\s*(AND|OR)'
            indexes[key] = re.sub(regex, '', indexes[key])
        return indexes

if __name__ == "__main__":
    import sys
    t = sys.argv[1]
    print(json.dumps(SPLtoSQL.parse(t)))
