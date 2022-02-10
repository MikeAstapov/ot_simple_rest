from parglare import Parser, Grammar

from parsers.otl_to_sparksql.internal import grammar
from parsers.otl_to_sparksql.internal.timerange import OTLTimeRangeExtractor
from parsers.otl_to_sparksql.internal.expressions.baseEvalExpression import BaseEvalExpressions


class OTLtoSQL:
    @staticmethod
    def parse_read(otl, av_indexes, tws, twf):
        """ Function for parsing read request.
        Returns JSON dictionary with index keys and SQL query string

        Arguments:
        otl(str): Input OTL request
        av_indexes(list): List of all possible indexes
        tws(int): Time to search from
        twf(int): Time to search to

        """

        # Remove time from OTL and save start time and end time values in tws and twf
        otl, _tws, _twf = OTLTimeRangeExtractor().extract_timerange(otl, tws, twf)
        indices_list = []
        fields_list = []

        # Create BaseEvalExpressions class instance
        expressions = BaseEvalExpressions(indices_list, fields_list)

        # Preprocess OTL string
        otl = expressions.otl_preprocess_request(otl)

        # Create parglare grammar from OTLGrammar string
        lalr_grammar = Grammar.from_string(grammar.OTLGrammar)

        # Create parglare parser with lalr_grammar
        # ARGS: Build tree
        # No debug logging
        # Select action in BaseEvalExpressions class for tree node
        # according to expression symbol in OTLGrammar in grammar.py
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
        tree = lalr_parser.parse(otl)

        # Transform tree to query string
        query_string = lalr_parser.call_actions(tree)

        if query_string is None:
            query_string = ''

        # Save indices to indices_list from OTL string
        indices_list = expressions.indices_list

        map_with_time = {}
        full_indices_list = []

        # If index is regular expression,
        # find all indices in av_indexes list
        for index_string in indices_list:

            if index_string.find('*') >= 0:
                unstarred_string = index_string[:index_string.find('*')]

                for index_name in av_indexes:
                    if index_name.find(unstarred_string) == 0:
                        full_indices_list.append(index_name)

            else:
                if index_string in av_indexes:
                    full_indices_list.append(index_string)

        # Add indices and query strings to map_with_time dictionary
        for index_string in full_indices_list:
            map_with_time[index_string] = {'query': query_string, 'tws': _tws, 'twf': _twf}

        return map_with_time

    @staticmethod
    def parse_filter(otl):
        """ Function for parsing filter request.
        Returns JSON dictionary with SQL query string

        Arguments:
        otl(str): Input OTL request

        """
        indices_list = []
        fields_list = []

        # Create BaseEvalExpressions class instance
        expressions = BaseEvalExpressions(indices_list, fields_list)

        # Preprocess OTL string
        otl = expressions.otl_preprocess_request(otl)

        # Create parglare grammar from OTLGrammar string
        lalr_grammar = Grammar.from_string(grammar.OTLGrammar)

        # Create parglare parser with lalr_grammar
        # ARGS: Build tree
        # No debug logging
        # Select action in BaseEvalExpressions class for tree node
        # according to expression symbol in OTLGrammar in grammar.py
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
        tree = lalr_parser.parse(otl)

        # Transform tree to query string
        query_string = lalr_parser.call_actions(tree)
        if query_string is None:
            query_string = ''

        # Create dictionary with key 'query' and value query_string
        result = {'query': query_string, 'fields': fields_list}
        return result
