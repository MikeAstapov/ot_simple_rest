import re
from parglare import Parser, Grammar

from parsers.spl_to_sparksql.internal import grammar
from parsers.spl_to_sparksql.internal.timerange import Timerange
from parsers.spl_to_sparksql.internal.expressions.baseEvalExpression import BaseEvalExpressions


class SPLtoSQL:
    @staticmethod
    def parse_read(spl, av_indexes, tws, twf):
        '''Returns JSON dictionary with indexes keys and SQL query string

        Arguments:
        spl(str): Input SPL request
        av_indexes(list): List of all possible indexes
        tws(int): Time to search from
        twf(int): Time to search to
        
        '''
        (spl_time, _tws, _twf) = Timerange.removetime(spl, tws, twf)
        indices_list = []
        expressions = BaseEvalExpressions(indices_list)
        spl = expressions.spl_preprocessing(spl)
        LALR_grammar = Grammar.from_string(grammar.SPLGrammar)
        
        LALR_parser = Parser(LALR_grammar, debug=False, build_tree=True,
                                 actions={'I' : expressions.remove_index,
                                          'Q' : expressions.transform_equal,
                                          'A' : expressions.transform_and,
                                          'O' : expressions.transform_or,
                                          'N' : expressions.transform_not,
                                          'G' : expressions.transform_comma,
                                          'C' : expressions.transform_comparison,
                                          'B' : expressions.transform_brackets,
                                          'V' : expressions.return_value,
                                          'S' : expressions.return_string,
                                          'NQ' : expressions.transform_not_equal
                                          })
        
        tree = LALR_parser.parse(spl)
        query_string = LALR_parser.call_actions(tree)
        if (query_string == None): query_string = ''
        indices_list = expressions.indices_list
        map_with_time = {}
        full_indices_list = []
        for index_string in indices_list:
            if (index_string.find('*') >= 0):
                string = index_string[:index_string.find('*')]
                for index_name in av_indexes:
                    if (index_name.find(string) == 0):
                        full_indices_list.append(index_name)
            else:
                full_indices_list.append(index_string)

        for index_string in full_indices_list:
            map_with_time[index_string] = {'query' : query_string, 'tws' : tws, 'twf' : twf}
        return map_with_time

    @staticmethod
    def parse_filter(spl):
        '''Returns JSON dictionary with SQL query string

        Arguments:
        spl(str): Input SPL request
        
        '''
        indices_list = []
        expressions = BaseEvalExpressions(indices_list)
        spl = expressions.spl_preprocessing(spl)
        LALR_grammar = Grammar.from_string(grammar.SPLGrammar)
        
        LALR_parser = Parser(LALR_grammar, debug=False, build_tree=True,
                                 actions={'I' : expressions.remove_index,
                                          'Q' : expressions.transform_equal,
                                          'A' : expressions.transform_and,
                                          'O' : expressions.transform_or,
                                          'N' : expressions.transform_not,
                                          'G' : expressions.transform_comma,
                                          'C' : expressions.transform_comparison,
                                          'B' : expressions.transform_brackets,
                                          'V' : expressions.return_value,
                                          'S' : expressions.return_string,
                                          'NQ' : expressions.transform_not_equal
                                          })
        
        tree = LALR_parser.parse(spl)
        #print (tree.tree_str())
        query_string = LALR_parser.call_actions(tree)
        if (query_string == None): query_string = ''
        result = {'query' : query_string}
        return result
