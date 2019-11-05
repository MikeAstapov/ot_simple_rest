from parglare import Parser, Grammar

from parsers.spl_to_sparksql.internal import grammar
from parsers.spl_to_sparksql.internal.timerange import Timerange
from parsers.spl_to_sparksql.internal.expressions.baseEvalExpression import BaseEvalExpressions


class SPLtoSQL:
    @staticmethod
    def parse_read(spl, av_indexes, tws, twf):
        """ Function for parsing read request.
        Returns JSON dictionary with index keys and SQL query string

        Arguments:
        spl(str): Input SPL request
        av_indexes(list): List of all possible indexes
        tws(int): Time to search from
        twf(int): Time to search to

        """

        # Remove time from SPL and save start time and end time values in tws and twf
        (_, _tws, _twf) = Timerange.removetime(spl, tws, twf)
        indices_list = []
        fields_list = []

        # Create BaseEvalExpressions class instance
        expressions = BaseEvalExpressions(indices_list, fields_list)

        # Preprocess SPL string
        spl = expressions.spl_preprocess_request(spl)

        # Create parglare grammar from SPLGrammar string
        lalr_grammar = Grammar.from_string(grammar.SPLGrammar)

        # Create parglare parser with lalr_grammar
        # ARGS: Build tree
        # No debug logging
        # Select action in BaseEvalExpressions class for tree node
        # according to expression symbol in SPLGrammar in grammar.py
        lalr_parser = Parser(lalr_grammar, debug=False, build_tree=True,
                             actions={'I': expressions.remove_index,
                                      'Q': expressions.transform_equal,
                                      'A': expressions.transform_and,
                                      'O': expressions.transform_or,
                                      'N': expressions.transform_not,
                                      'G': expressions.transform_comma,
                                      'C': expressions.transform_comparison,
                                      'B': expressions.transform_brackets,
                                      'V': expressions.return_value,
                                      'S': expressions.return_string,
                                      'NQ': expressions.transform_not_equal
                                      })

        # Build tree
        tree = lalr_parser.parse(spl)

        # Transform tree to query string
        query_string = lalr_parser.call_actions(tree)

        if query_string is None:
            query_string = ''

        # Save indices to indices_list from SPL string
        indices_list = expressions.indices_list

        map_with_time = {}
        full_indices_list = []

        # If index is regular expression,
        # find all indices in av_indexes list
        for index_string in indices_list:
            if index_string.find('*') >= 0:
                string = index_string[:index_string.find('*')]
                for index_name in av_indexes:
                    if index_name.find(string) == 0:
                        full_indices_list.append(index_name)
            else:
                full_indices_list.append(index_string)

        # Add indices and query strings to map_with_time dictionary
        for index_string in full_indices_list:
            map_with_time[index_string] = {'query': query_string, 'tws': tws, 'twf': twf}

        return map_with_time

    @staticmethod
    def parse_filter(spl):
        """ Function for parsing filter request.
        Returns JSON dictionary with SQL query string

        Arguments:
        spl(str): Input SPL request

        """
        indices_list = []
        fields_list = []

        # Create BaseEvalExpressions class instance
        expressions = BaseEvalExpressions(indices_list, fields_list)

        # Preprocess SPL string
        spl = expressions.spl_preprocess_request(spl)

        # Create parglare grammar from SPLGrammar string
        lalr_grammar = Grammar.from_string(grammar.SPLGrammar)

        # Create parglare parser with lalr_grammar
        # ARGS: Build tree
        # No debug logging
        # Select action in BaseEvalExpressions class for tree node
        # according to expression symbol in SPLGrammar in grammar.py
        lalr_parser = Parser(lalr_grammar, debug=False, build_tree=True,
                             actions={'I': expressions.remove_index,
                                      'Q': expressions.transform_equal,
                                      'A': expressions.transform_and,
                                      'O': expressions.transform_or,
                                      'N': expressions.transform_not,
                                      'G': expressions.transform_comma,
                                      'C': expressions.transform_comparison,
                                      'B': expressions.transform_brackets,
                                      'V': expressions.return_value,
                                      'S': expressions.return_string,
                                      'NQ': expressions.transform_not_equal
                                      })

        # Build tree
        tree = lalr_parser.parse(spl)

        # Transform tree to query string
        query_string = lalr_parser.call_actions(tree)
        if query_string is None:
            query_string = ''

        # Create dictionary with key 'query' and value query_string
        result = {'query': query_string, 'fields': fields_list}
        return result
