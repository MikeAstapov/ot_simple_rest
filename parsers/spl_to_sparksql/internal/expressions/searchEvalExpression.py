from parsers.spl_to_sparksql.internal.expressions.baseEvalExpression import BaseEvalExpressions


class SearchEvalExpression(BaseEvalExpressions):
    def __init__(self):
        self.indexes = {}
        super().__init__()

    def indexspecifier(self, args):
        index_name = str(args[0]).strip("\"\'")
        self.indexes[index_name] = ""
        return "index=" + index_name

    def logicalexpression(self, args):
        if len(args) >= 2:
            if args[0] == "NOT":
                return self.inverted(args[1])
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
        if args[0] is None:
            return ""
        return args[0]
