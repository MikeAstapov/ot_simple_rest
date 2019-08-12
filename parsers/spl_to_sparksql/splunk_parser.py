import re
import internal.grammar
from lark import Lark
from internal.timerange import Timerange
from internal.expressions.searchEvalExpression import SearchEvalExpression
from internal.expressions.filterEvalExpression import FilterEvalExpression


class SPLtoSQL:
    @staticmethod
    def parse_read(spl, av_indexes, tws, twf):
        lark = Lark(internal.grammar.read, parser='earley', debug=True)
        (spl_time, _tws, _twf) = Timerange.removetime(spl, tws, twf)
        tree = lark.parse(spl_time)
        evalexpr = SearchEvalExpression()
        tree2 = evalexpr.transform(tree)
        st2 = tree2.children[0]
        indexes = evalexpr.indexes
        for index in indexes.keys():
            temp = "index="+str(index)
            if temp in st2:
                indexes[index] += st2
        if (not indexes):
            indexes = {k: st2 for k in av_indexes}
        for key in indexes:
            regex = r'(AND|OR)*\s*index=[\w\*_"\']*\s*(AND|OR)?'
            indexes[key] = re.sub(regex, '', indexes[key])
            indexes[key] = indexes[key].strip()
        result = {}
        for key in indexes:
            if '*' in key:
                regex = key.replace('*', r"(\w)*")
                pattern = re.compile(regex)
                for index in av_indexes:
                    if pattern.match(index):
                        result[index] = indexes[key]
            else:
                result[key] = indexes[key]
        map_with_time = {}
        for key in result:
            map_with_time[key] = {"query": result[key],
                                  "tws": _tws, "twf": _twf}
        return map_with_time

    @staticmethod
    def parse_filter(spl):
        lark = Lark(internal.grammar.filter, parser='earley', debug=True)
        tree = lark.parse(spl)
        evalexpr = FilterEvalExpression()
        tree2 = evalexpr.transform(tree)
        st2 = tree2.children[0]
        result = {}
        result["query"] = st2
        return result
