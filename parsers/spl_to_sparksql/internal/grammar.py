# LALR Grammar string for SPL to SQL Parser

SPLGrammar = r'''
                E: B
                | NQ {left, 3}
                | N {left, 3}
                | O {left, 1}
                | A {left, 2}
                | Q {left, 3}
                | I
                | G
                | C {left, 3}
                | S;
                
                I: 'index=' INDEX
                | 'index=' '"'SPACEDINDEX'"';
                Q: STRING "=" V;
                G: E "," E;
                A: E "AND" E;
                O: E "OR" E;
                N: "NOT" E;
                NQ: STRING "!=" V;
                C: STRING ">" V
                | STRING ">=" V
                | STRING "<" V
                | STRING "<=" V;
                B: "(" E ")";
                V: STRING+
                | '"'SPACEDSTR'"'
                | '""'
                | EMPTY;
                S: STRING+
                | '"'SPACEDSTR'"'
                | '""'
                | EMPTY;

                terminals
                INDEX: /[^ !"'=%()&\-\\\/,]+/;
                SPACEDINDEX: /[^!"'=]+/;
                SPACEDSTR: /[^!"'=\\]+/;
                STRING: /[^! <>"'=%()&\-\\\/,]+/;
                '''
