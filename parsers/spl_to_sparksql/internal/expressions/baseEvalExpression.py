import re
from parglare import Parser, Grammar


class BaseEvalExpressions():
    def __init__(self, indexString):
        self.indexString = indexString
#        super().__init__()

    def splPreprocessing(self, spl):
    	spl = self.splAddANDOnEmptyWS(spl)
    	return spl  

    def splAddANDOnEmptyWS(self, spl):
        operators = ["NOT", "OR", "AND"]
        result = ""
        splList = spl.split()
        for index in range(0,len(splList)-1):
            if (splList[index][-1:] == ')') and (splList[index+1][:1] == '('):
                result = result + splList[index] + " AND "
            elif (splList[index].replace('(','').replace(')','').upper() not in operators) and (splList[index + 1].replace('(','').replace(')','').upper() not in operators):
                result = result + splList[index] + " AND "
            else:
                result = result + splList[index] + ' '
        result = result + splList[-1]
        return result

    def indexParse(self, context, nodes):
        self.indexString = nodes[1]
        print(self.indexString)
        return

    def equalParse(self, context, nodes):
        result = ''
        if (result.find('*') >= 0):
            result = "(" + nodes[0] +  ' rlike \'' + nodes[2] + '\')'
            result = result[:result.find('*')] + '.' + result[result.find('*'):]
        else:
            result = nodes[0] + "=\"" + nodes[2] + "\""
        return result

    def andParse(self, context, nodes):
        if nodes[0] == None:
            return nodes[2]
        elif nodes[2] == None:
            return nodes[0]
        else:
            return nodes[0] + " AND " + nodes[2]

    def orParse(self, context, nodes):
        if nodes[0] == None:
            return nodes[2]
        elif nodes[2] == None:
            return nodes[0]
        else:
            return nodes[0] + " OR " + nodes[2]
      
    def notParse(self, context, nodes):
        if nodes[1][:1] == '!':
            return nodes[1][1:]
        else:
            return "!" + nodes[1]

    def compareParse(self, context, nodes):
        return nodes[0] + nodes[1] + nodes[2]

    def quotesParse(self, context, nodes):
        return '(_raw rlike \'' + nodes[1] + '\')'

    def bracketsParse(self, context, nodes):
        return "(" + nodes[1] + ")"

    def stringParse(self, context, nodes):
        print(nodes[0])
##        if (nodes[0][0].find('_quoted_text_') >= 0):
##            return nodes[0][0]
        if (nodes[0][0][:1] == '"') and (nodes[0][0][-1:] == '"'):
            return '(_raw rlike \'' + nodes[0][0][1:-1] + '\')'
        else:
            return '(_raw like \'%' + nodes[0][0] + '%\')'
