smlGrammar = r'''
                E: B
                | N {left, 3}
                | O {left, 1}
                | A {left, 2}
                | Q {left, 3}
                | I
                | C {left, 3}
                | S;
                
                I: 'index=' VALUE;
                Q: STRING "=" STRING;
                A: E "AND" E;
                O: E "OR" E;
                N: "NOT" E;
                C: E ">" E
                | E ">=" E
                | E "<" E
                | E "<=" E
                | E "==" E
                | E "!=" E;
                B: "(" E ")";
                S: STRING+;

                terminals
                VALUE: /[a-zA-Z0-9а-яА-Я_*-.:]+/;
                STRING: /[a-zA-Z0-9а-яА-Я_*.-:"]+/;
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
             STRING_INDEX:/[a-zA-Z0-9_*-.]+/
             CMP:"="|"!="|"<"|"<="|">"|">="
             VALUE: ESCAPED_STRING |TERM | NUM
             TERM: /[a-zA-Z0-9_*-]+/
             NUM: /-?\d+(?:\.\d+)*/
             le_or.4: "OR"
             le_and.4: "AND"
             le_not.4: "NOT"
             leftb.5: "("
             rightb.5: ")"
             %import common.WORD
             %ignore " "
         '''
