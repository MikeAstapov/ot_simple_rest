import re

# Class with actions for parsing SPL expression to SQL

class BaseEvalExpressions():
    def __init__(self, indices_list):
        self.indices_list = indices_list

    def spl_preprocessing(self, spl):
        '''Transforms all logical expressions to upper case.
        Replaces whitespaces with logical AND.

        :param spl: input SPL string
        :return: preprocessed SPL string

        '''

        spl = self.spl_replace_case(spl)
        spl = self.spl_replace_ws_with_and(spl)
        return spl

    def spl_replace_case(self, spl):
        '''Transforms all logical expressions to upper case

        :param spl: input SPL string
        :return: preprocessed SPL string

        '''
        operators = ["NOT", "OR", "AND"]
        result = ''
        spl_list = re.findall(r'(?:[^\s,"]|"(?:\\.|[^"])*")+', spl)
        for index in range(0, len(spl_list)):
            if (spl_list[index].upper() in operators):
                result = result + spl_list[index].upper() + ' '
            else:
                result = result + spl_list[index] + ' '
        return result

    def spl_replace_ws_with_and(self, spl):
        ''' Replaces whitespaces with logical AND

        :param spl: input SPL string
        :return: preprocessed SPL string

        '''
        
        operators = ["NOT", "OR", "AND"]
        result = ''
        spl_list = re.findall(r'(?:[^\s,"]|"(?:\\.|[^"])*")+', spl)
        for index in range(0, len(spl_list) - 1):
            if (spl_list[index][-1:] == ')') and (spl_list[index+1][:1] == '('):
                result = result + spl_list[index] + ' AND '
            elif ((spl_list[index].replace('(','').replace(')','').upper() not in operators) and
                     (spl_list[index + 1].replace('(','').replace(')','').upper() not in operators)):
                result = result + spl_list[index] + ' AND '
            else:
                result = result + spl_list[index] + ' '
        result = result + spl_list[-1]
        return result

    def remove_index(self, context, nodes):
        '''Removes indices from SPL request and save index contents in self.indices_list

        :param context: An object used to keep parser context info
        :param nodes: Nodes of the parse tree on this iteration
        :return: None

        '''
        
        if (len(nodes) == 2):
            index_string = nodes[1].replace('"', '').replace("'", '')
            self.indices_list.insert(0, index_string)
        if (len(nodes) == 4):
            index_string = nodes[2].replace('"', '').replace("'", '')
            self.indices_list.insert(0, index_string)
        return

    def transform_equal(self, context, nodes):
        '''Transforms equal expressions from SPL format to SQL format

        :param context: An object used to keep parser context info
        :param nodes: Nodes of the parse tree on this iteration
        :return: Transformed equal expression

        '''

        result = ''
        if (len(nodes) == 5):
            result = nodes[0] + '=' + nodes[2] + nodes[3] + nodes[4]
            return result
            
        if (nodes[2].find('*') >= 0):
            result = "(" + nodes[0] +  ' rlike \'' + nodes[2] + '\')'
            pos = result.rfind('*')
            while (pos >= 0):
                result = result[:pos] + '.' + result[pos:]
                pos = result[:pos].rfind('*')
        elif (nodes[2] == ''):
            result = nodes[0] + '=""'
        elif (nodes[2][:1] == '"') and (nodes[2][-1:] == '"'):
            result = nodes[0] + '=' + nodes[2]
        else:
            result = nodes[0] + "=\"" + nodes[2] + "\""
        return result

    def transform_not_equal(self, context, nodes):
        '''Transforms not equal expressions from SPL format to SQL format

        :param context: An object used to keep parser context info
        :param nodes: Nodes of the parse tree on this iteration
        :return: Transformed not equal expression

        '''

        result = ''
        if (len(nodes) == 5):
            result = '!(' + nodes[0] + '=' + nodes[2] + nodes[3] + nodes[4] + ')'
            return result
            
        if (nodes[2].find('*') >= 0):
            result = "!(" + nodes[0] +  ' rlike \'' + nodes[2] + '\')'
            pos = result.rfind('*')
            while (pos >= 0):
                result = result[:pos] + '.' + result[pos:]
                pos = result[:pos].rfind('*')
        elif (nodes[2] == ''):
            result = '!(' + nodes[0] + '="")'
        elif (nodes[2][:1] == '"') and (nodes[2][-1:] == '"'):
            result = '!(' + nodes[0] + '=' + nodes[2] + ')'
        else:
            result = '!(' + nodes[0] + "=\"" + nodes[2] + "\"" + ')'
        return result

    def transform_and(self, context, nodes):
        '''Transforms AND expressions from SPL format to SQL format

        :param context: An object used to keep parser context info
        :param nodes: Nodes of the parse tree on this iteration
        :return: Transformed AND expression
        
        '''
        
        if nodes[0] == None:
            return nodes[2]
        elif nodes[2] == None:
            return nodes[0]
        else:
            return nodes[0] + " AND " + nodes[2]

    def transform_or(self, context, nodes):
        '''Transforms OR expressions from SPL format to SQL format

        :param context: An object used to keep parser context info
        :param nodes: Nodes of the parse tree on this iteration
        :return: Transformed OR expression
        
        '''
        
        if nodes[0] == None:
            return nodes[2]
        elif nodes[2] == None:
            return nodes[0]
        else:
            return nodes[0] + " OR " + nodes[2]
      
    def transform_not(self, context, nodes):
        '''Transforms NOT expressions from SPL format to SQL format

        :param context: An object used to keep parser context info
        :param nodes: Nodes of the parse tree on this iteration
        :return: Transformed NOT expression
        
        '''
        
        if nodes[1][:1] == '!':
            return nodes[1][1:]
        else:
            return "!(" + nodes[1] + ")"

    def transform_comparison(self, context, nodes):
        '''Transforms compare expressions from SPL format to SQL format

        :param context: An object used to keep parser context info
        :param nodes: Nodes of the parse tree on this iteration
        :return: Transformed compare expression
        
        '''
        
        return nodes[0] + nodes[1] + nodes[2]

    def transform_quotes(self, context, nodes):
        '''Transforms quoted expressions from SPL format to SQL format

        :param context: An object used to keep parser context info
        :param nodes: Nodes of the parse tree on this iteration
        :return: Transformed quoted expression
        
        '''
        
        return '(_raw rlike \'' + nodes[1] + '\')'

    def transform_brackets(self, context, nodes):
        '''Transforms expressions with brackets from SPL format to SQL format

        :param context: An object used to keep parser context info
        :param nodes: Nodes of the parse tree on this iteration
        :return: Transformed brackets expression
        
        '''
        
        return "(" + nodes[1] + ")"

    def transform_comma(self, context, nodes):
        '''Transforms expressions with comma from SPL format to SQL format

        :param context: An object used to keep parser context info
        :param nodes: Nodes of the parse tree on this iteration
        :return: Transformed comma expression
        
        '''
        
        if nodes[0] == None:
            return nodes[2]
        elif nodes[2] == None:
            return nodes[0]
        else:
            return nodes[0] + " AND " + nodes[2]

    def return_value(self, context, nodes):
        '''Transforms value with optional regular expression to SPL format

        :param context: An object used to keep parser context info
        :param nodes: Nodes of the parse tree on this iteration
        :return: Terminal value, optionally with quotes
        
        '''

        if (len(nodes) == 0):
            return ''
        elif (type(nodes[0]) == list):
            return nodes[0][0]
        elif (len(nodes) == 1):
            return nodes[0]
        elif (len(nodes) == 3):
            if (nodes[1].find('*') >= 0):
                return nodes[1]
            else:
                return nodes[0] + nodes[1] + nodes[2]

    def return_string(self, context, nodes):
        '''Transforms string to SPL format with optional '_raw like' or '_raw rlike' strings

        :param context: An object used to keep parser context info
        :param nodes: Nodes of the parse tree on this iteration
        :return: Terminal string, optionally with '_raw like' or '_raw rlike'
        
        '''

        if len(nodes) == 0:
            return ''
        if (nodes[0] == '"') and (nodes[len(nodes)-1] == '"'):
            if (len(nodes) == 3):
                return '(_raw rlike \'' + nodes[1] + '\')'
            else: return '""'
        else:
            return '(_raw like \'%' + nodes[0][0] + '%\')'
