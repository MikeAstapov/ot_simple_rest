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
        indicesList = []
        evalExpr = BaseEvalExpressions(indicesList)
        spl = evalExpr.splPreprocessing(spl)
        #print (grammar.smlGrammar)
        #return 
        lalrGrammar = Grammar.from_string(grammar.smlGrammar)
        lalrParser = Parser(lalrGrammar, debug=False, build_tree=True, actions={ 'I' : evalExpr.indexParse,
                                                                           	 'Q' : evalExpr.equalParse,
                                                                           	 'A' : evalExpr.andParse,
                                                                           	 'O' : evalExpr.orParse,
                                                                           	 'N' : evalExpr.notParse,
                                                                                 'G' : evalExpr.commaParse,
                                                                           	 'C' : evalExpr.compareParse,
                                                                           	 'B' : evalExpr.bracketsParse,
                                                                                 'V' : evalExpr.valueParse,
                                                                           	 'S' : evalExpr.stringParse })
        tree = lalrParser.parse(spl)
#        print(tree) #.tree_str())
        queryString = lalrParser.call_actions(tree)
        if (queryString == None): queryString = ''
        #print (queryString)
        indicesList = evalExpr.indicesList
        map_with_time = {}
        fullIndicesList = []
        for indexString in indicesList:
            if (indexString.find('*') >= 0):
                av_string = indexString[:indexString.find('*')]
                for av in av_indexes:
                    if (av.find(av_string) == 0):
                        fullIndicesList.append(av)
            else:
                fullIndicesList.append(indexString)

        for indexString in fullIndicesList:
            map_with_time[indexString] = {'query' : queryString, 'tws' : tws, 'twf' : twf}
        #print(map_with_time)
        return map_with_time

    @staticmethod
    def parse_filter(spl):
        indicesList = []
        evalExpr = BaseEvalExpressions(indicesList)
        spl = evalExpr.splPreprocessing(spl)
        #print (grammar.smlGrammar)
        #return 
        lalrGrammar = Grammar.from_string(grammar.smlGrammar)
        lalrParser = Parser(lalrGrammar, debug=False, build_tree=True, actions={ 'I' : evalExpr.indexParse,
                                                                           	 'Q' : evalExpr.equalParse,
                                                                           	 'A' : evalExpr.andParse,
                                                                           	 'O' : evalExpr.orParse,
                                                                           	 'N' : evalExpr.notParse,
                                                                                 'G' : evalExpr.commaParse,
                                                                           	 'C' : evalExpr.compareParse,
                                                                           	 'B' : evalExpr.bracketsParse,
                                                                                 'V' : evalExpr.valueParse,
                                                                           	 'S' : evalExpr.stringParse })
        tree = lalrParser.parse(spl)
#        print(tree) #.tree_str())
        queryString = lalrParser.call_actions(tree)
        if (queryString == None): queryString = ''
        result = {'query' : queryString}
        return result
