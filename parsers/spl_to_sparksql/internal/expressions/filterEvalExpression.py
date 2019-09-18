from parsers.spl_to_sparksql.internal.expressions.baseEvalExpression import BaseEvalExpressions


class FilterEvalExpression(BaseEvalExpressions):

    def logicalexpression(self, args):
        if len(args) >= 2:
            if args[0] == "NOT":
                return self.inverted(args[1])
            if (args[0]) == "(" and args[-1] == ")":
                return "(" + args[1] + ")"
            if (args[1]) == "OR":
                return args[0] + " OR " + args[2]
            if (args[1]) == "AND":
                return args[0] + " AND " + args[2]
            return args[0] + " AND " + args[1]
        return args[0]
