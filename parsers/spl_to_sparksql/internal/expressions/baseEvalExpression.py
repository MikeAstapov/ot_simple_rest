import re
from lark import Transformer


class BaseEvalExpressions(Transformer):
    def __init__(self):
        super().__init__()

    def le_not(self, args):
        return "NOT"

    def inverted(s):
        return "!" + s

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
            replaced = re.escape(args[0]).replace(r"\*", ".*")
            return "(_raw rlike '" + replaced + "')"
        return "(_raw like '%" + args[0] + "%')"

    def logicalexpression(self, args):
        pass

    def comparisonexpression(self, args):
        striped = str(args[2]).strip("\"\'")
        if "*" in striped:
            replaced = striped.replace('*', '.*')
            return args[0] + " rlike '" + replaced + "'"
        return args[0] + args[1] + '"' + striped + '"'

    def leftb(self, args):
        return "("

    def rightb(self, args):
        return ")"
