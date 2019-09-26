import re
import shlex
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
        result = ''
        splList = re.findall(r'(?:[^\s,"]|"(?:\\.|[^"])*")+', spl) #shlex.split(spl)
        for index in range(0,len(splList)-1):
            #print (splList[index])
            if (splList[index][-1:] == ')') and (splList[index+1][:1] == '('):
                result = result + splList[index] + ' AND '
            elif (splList[index].replace('(','').replace(')','').upper() not in operators) and (splList[index + 1].replace('(','').replace(')','').upper() not in operators):
                result = result + splList[index] + ' AND '
            else:
                result = result + splList[index] + ' '
        result = result + splList[-1]
        #print ('SPLPRE ', result)
        return result

    def indexParse(self, context, nodes):
        #print ('Index ', nodes)
        self.indexString = nodes[1]
        return

    def equalParse(self, context, nodes):
        result = ''
        #print('EQUAL ', nodes)
        
        if (len(nodes) == 5):
            result = nodes[0] + '=' + nodes[2] + nodes[3] + nodes[4]
            return result
            
        if (nodes[2].find('*') >= 0):
            result = "(" + nodes[0] +  ' rlike \'' + nodes[2] + '\')'
            result = result[:result.find('*')] + '.' + result[result.find('*'):]
        elif (nodes[2] == ''):
            result = nodes[0] + '=""'
        elif (nodes[2][:1] == '"') and (nodes[2][-1:] == '"'):
            result = nodes[0] + '=' + nodes[2]
        else:
            result = nodes[0] + "=\"" + nodes[2] + "\""
        #print ('EQ ', result)
        return result

    def andParse(self, context, nodes):
        #print ('AND ', nodes)
        if nodes[0] == None:
            return nodes[2]
        elif nodes[2] == None:
            return nodes[0]
        else:
            return nodes[0] + " AND " + nodes[2]

    def orParse(self, context, nodes):
        #print ('OR ', nodes)
        if nodes[0] == None:
            return nodes[2]
        elif nodes[2] == None:
            return nodes[0]
        else:
            return nodes[0] + " OR " + nodes[2]
      
    def notParse(self, context, nodes):
        #print ('NOT ', nodes)
        if nodes[1][:1] == '!':
            return nodes[1][1:]
        else:
            return "!" + nodes[1]

    def compareParse(self, context, nodes):
        return nodes[0] + nodes[1] + nodes[2]

    def quotesParse(self, context, nodes):
        #print ('Quotes ', nodes)
        return '(_raw rlike \'' + nodes[1] + '\')'

    def bracketsParse(self, context, nodes):
        #print ('Brackets ', nodes)
        return "(" + nodes[1] + ")"

    def valueParse(self, context, nodes):
        #print ('Value ', nodes)
        #print (type(nodes[0]), type(nodes))
        if (type(nodes[0]) == list):
            return nodes[0][0]
        elif (len(nodes) == 1):
            return nodes[0]
        elif (len(nodes) == 3):
            return nodes[0] + nodes[1] + nodes[2]

    def stringParse(self, context, nodes):
        #print ('STR ', nodes)
        if len(nodes) == 0:
            return ''
        if (nodes[0] == '"') and (nodes[len(nodes)-1] == '"'):
            if (len(nodes) == 3):
                return '(_raw rlike \'' + nodes[2] + '\')'
            else: return '""'
        else:
            return '(_raw like \'%' + nodes[0][0] + '%\')'
