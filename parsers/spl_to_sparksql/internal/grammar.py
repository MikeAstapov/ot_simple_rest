read = r'''start:_le
         _le:  logicalexpression
          logicalexpression: leftb _le rightb
          | le_not logicalexpression
          | _le [le_and|le_or] _le
          | _searchmodifier
          | indexexpression
          | comparisonexpression
             _searchmodifier.2:  indexspecifier
             indexspecifier: "index" "=" VALUE
             indexexpression.3:  VALUE
             comparisonexpression: STRING_INDEX CMP VALUE
             TIME_MODIFIER: "earliest" | "latest"
             FIELD: ESCAPED_STRING | /[a-zA-Z0-9а-яА-Я_*-.]+/
             STRING_INDEX:/[a-zA-Z0-9а-яА-Я_*-."']+/
             CMP:"="|"!="|"<"|"<="|">"|">="
             VALUE: ESCAPED_STRING | NUM |  TERM
             TERM: /[a-zA-Z0-9_*.-:]+/
             NUM: /-?\d+(?:\.\d+)*/
             le_or.4: "OR"
             le_and.4: "AND"
             le_not.4: "NOT"
             leftb.5: "("
             rightb.5: ")"
             %import common.WORD
             %import common.NEWLINE
             %import common.ESCAPED_STRING
             %ignore " "
             %ignore NEWLINE
         '''

filter = r'''start:_le
             _le:  logicalexpression
             logicalexpression: leftb _le rightb
             | le_not logicalexpression
             | _le [le_and|le_or] _le
             |  indexexpression
             |  comparisonexpression
             indexexpression.3:  FIELD
             comparisonexpression: STRING_INDEX CMP VALUE
             FIELD: ESCAPED_STRING|/[a-zA-Z0-9_*-.]+/
             STRING_INDEX:/[a-zA-Z0-9а-яА-Я_*-."']+/  
             CMP:"="|"!="|"<"|"<="|">"|">="
             VALUE: ESCAPED_STRING |TERM | NUM
             TERM: /[a-zA-Z0-9_.*-]+/
             NUM: /-?\d+(?:\.\d+)*/
             le_or.4: "OR"
             le_and.4: "AND"
             le_not.4: "NOT"
             leftb.5: "("
             rightb.5: ")"
             %import common.NEWLINE
             %import common.ESCAPED_STRING
             %ignore " "
             %ignore NEWLINE
         '''
