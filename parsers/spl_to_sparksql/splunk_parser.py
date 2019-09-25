import re

from parsers.spl_to_sparksql.internal import grammar

from parglare import Parser, Grammar
from parsers.spl_to_sparksql.internal.timerange import Timerange
from parsers.spl_to_sparksql.internal.expressions.filterEvalExpression import FilterEvalExpression
from parsers.spl_to_sparksql.internal.expressions.baseEvalExpression import BaseEvalExpressions


class SPLtoSQL:
    
    @staticmethod
    def parse_read(spl, av_indexes, tws, twf):
        #print ('ARGS ', spl, av_indexes, tws, twf)
        (spl_time, _tws, _twf) = Timerange.removetime(spl, tws, twf)
        #print ('SPL', spl)
        #print('SPL TIME', spl_time)
        #return
        indexString = ''
        evalExpr = BaseEvalExpressions(indexString)
        spl = evalExpr.splPreprocessing(spl)
        #print (grammar.smlGrammar)
        #return 
        lalrGrammar = Grammar.from_string(grammar.smlGrammar)
        lalrParser = Parser(lalrGrammar, debug=False, build_tree=True, actions={ 'I' : evalExpr.indexParse,
                                                                           	 'Q' : evalExpr.equalParse,
                                                                           	 'A' : evalExpr.andParse,
                                                                           	 'O' : evalExpr.orParse,
                                                                           	 'N' : evalExpr.notParse,
                                                                           	 'C' : evalExpr.compareParse,
                                                                           	 'B' : evalExpr.bracketsParse,
                                                                           	 'S' : evalExpr.stringParse })
        tree = lalrParser.parse(spl)
        #print(tree.tree_str())
        queryString = lalrParser.call_actions(tree)
        if (queryString == None): queryString = ''
        #print (queryString.replace('""','"'))
        indexString = evalExpr.indexString
        map_with_time = {indexString : {'query' : queryString, 'tws' : tws, 'twf' : twf}}
        #print(map_with_time)
        return map_with_time

    @staticmethod
    def parse_filter(spl):
        lark = Lark(grammar.filter, parser='earley', debug=True)
        tree = lark.parse(spl)
        evalexpr = FilterEvalExpression()
        tree2 = evalexpr.transform(tree)
        st2 = tree2.children[0]
        result = {}
        result["query"] = st2
        return result
