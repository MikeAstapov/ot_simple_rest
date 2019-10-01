smlGrammar = r'''
                E: B
                | N {left, 3}
                | O {left, 1}
                | A {left, 2}
                | Q {left, 3}
                | I
                | G
                | C {left, 3}
                | S;
                
                I: 'index=' VALUE
                | 'index=' '"'VALUESPC'"';
                Q: STRING "=" V;
                G: E "," E;
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
                V: STRING+
                | '"'SPACESTR'"'
                | '""'
                | EMPTY;
                S: STRING+
                | '"'SPACESTR'"'
                | '""'
                | EMPTY;

                terminals
                VALUE: /[a-zA-Z0-9а-яА-Я_*-.:]+/;
                VALUESPC: /[a-zA-Z0-9а-яА-Я_*-.: ]+/;
                SPACESTR: /[a-zA-Z0-9а-яА-Я_*.,-: ]+/;
                STRING: /[a-zA-Z0-9а-яА-Я_*.-:]+/;
                '''



#EMPTY: /^(?![\s\S])/;

#                | STRING "=" '"'S'"';
