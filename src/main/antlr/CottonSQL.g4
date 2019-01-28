/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 by Ralph Gasser
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
grammar CottonSQL;

@header {
    package ch.unibas.dmi.dbis.cottontail.sql.antlr;
}

/* The root for CottonSQL parsing. */
root
 : ( sql_stmt_list | error ) EOF
 ;

error
 : UNEXPECTED_CHAR
   {
     throw new RuntimeException("UNEXPECTED_CHAR=" + $UNEXPECTED_CHAR.text);
   }
 ;

sql_stmt_list
 : sql_stmt ( ';'+ sql_stmt )* ';'*
 ;

sql_stmt
: ( K_EXPLAIN )? (statement)
;

statement
 : create_index_stmt
 | create_schema_stmt
 | create_entity_stmt
 | delete_stmt
 | drop_index_stmt
 | drop_schema_stmt
 | drop_entity_stmt
 | insert_stmt
 | reindex_stmt
 | select_stmt
 | compound_statement
 | update_stmt
 | vacuum_stmt
;

create_index_stmt
 : K_CREATE K_UNIQUE? K_INDEX ( K_IF K_NOT K_EXISTS )?
   ( schema_name '.' )? index_name K_ON entity_name '(' qualified_column_name ')'
   ( K_WHERE expr )?
 ;

create_schema_stmt
 : K_CREATE K_SCHEMA ( K_IF K_NOT K_EXISTS )? schema_name
 ;

create_entity_stmt
 : K_CREATE K_ENTITY ( K_IF K_NOT K_EXISTS )?
   qualified_entity_name
   ( '(' column_def ( ',' column_def )* ')'
   | K_AS select_stmt
   )
 ;

delete_stmt
 : with_clause? K_DELETE K_FROM qualified_entity_name
   ( K_WHERE expr )?
 ;

drop_index_stmt
 : K_DROP K_INDEX ( K_IF K_EXISTS )? ( schema_name '.' )? index_name
 ;

drop_schema_stmt
 : K_DROP K_SCHEMA ( K_IF K_EXISTS )? schema_name
 ;

drop_entity_stmt
 : K_DROP K_ENTITY ( K_IF K_EXISTS )? qualified_entity_name
 ;

insert_stmt
 : with_clause? ( K_INSERT
                | K_REPLACE
                | K_INSERT K_OR K_REPLACE
                | K_INSERT K_OR K_ROLLBACK
                | K_INSERT K_OR K_ABORT
                | K_INSERT K_OR K_FAIL
                | K_INSERT K_OR K_IGNORE ) K_INTO
   qualified_entity_name ( '(' column_name ( ',' column_name )* ')' )?
   ( K_VALUES '(' expr ( ',' expr )* ')' ( ',' '(' expr ( ',' expr )* ')' )*
   | select_stmt
   | K_DEFAULT K_VALUES
   )
 ;

reindex_stmt
 : K_REINDEX ( collation_name
             | ( schema_name '.' )? ( entity_name | index_name )
             )?
 ;

select_stmt
 : select_core ( K_ORDER K_BY order_clause ( ',' order_clause )* )? limit_clause?
 ;

limit_clause
 : K_LIMIT INTEGER_LITERAL ( ( K_OFFSET | ',' ) INTEGER_LITERAL )?
 ;

update_stmt
 : with_clause? K_UPDATE ( K_OR K_ROLLBACK
                         | K_OR K_ABORT
                         | K_OR K_REPLACE
                         | K_OR K_FAIL
                         | K_OR K_IGNORE )? qualified_entity_name
   K_SET column_name '=' expr ( ',' column_name '=' expr )* ( K_WHERE expr )?
 ;

vacuum_stmt
 : K_VACUUM
 ;

column_def
 : column_name type
 ;

type
 : type_name+ ( '(' signed_number ')' )? (column_constraint)?
 ;

column_constraint
 :   K_NOT? K_NULL
   | K_UNIQUE
 ;

/*
    Cottontail DB understands the following binary operators, in order from highest to lowest precedence:
    ||
    *    /    %
    +    -
    <<   >>   &    |
    <    <=   >    >=
    =    ==   !=   <>   IS   IS NOT   IN   LIKE   GLOB   MATCH   REGEXP
    AND
    OR
*/
expr
 : literal_value
 | BIND_PARAMETER
 | qualified_column_name;


qualified_column_name
 : ( ( schema_name '.' )? entity_name '.' )? column_name;

with_clause
 : K_WITH cte_table_name K_AS '(' select_stmt ')' ( ',' cte_table_name K_AS '(' select_stmt ')' )*
 ;

qualified_entity_name
 : ( schema_name '.' )? entity_name
 ;

order_clause
 : expr ( K_ASC | K_DESC )?
 ;

result_column
 : '*'
 | entity_name '.' '*'
 | expr ( K_AS? column_alias )?
 ;

from_clause
 : ( entity_or_subselect | join_clause )
 ;

entity_or_subselect
 : simple_entity_clause
 | subselect_clause
 ;

simple_entity_clause
 : ( schema_name '.' )? entity_name ( K_AS? table_alias )?
 ;

subselect_clause
 : '(' select_stmt ')' K_AS? table_alias
 ;

join_clause
 : entity_or_subselect ( join_operator entity_or_subselect join_constraint )*
 ;

join_operator
 : ','
 | K_NATURAL? ( K_LEFT K_OUTER? | K_INNER | K_CROSS )? K_JOIN
 ;

join_constraint
 : ( K_ON expr
   | K_USING '(' column_name ( ',' column_name )* ')' )?
 ;

select_core
 : K_SELECT ( K_DISTINCT | K_ALL )? (knn_expression | result_column) ( ',' result_column )*
   ( K_FROM from_clause )
   ( K_WHERE expr )?
 ;

knn_expression
 : K_KNN '(' column_name ',' distance_name ',' expr ')'
 ;

compound_statement
 : (select_stmt) (compound_operator compound_statement)+
 ;

compound_operator
 : K_UNION
 | K_UNION K_ALL
 | K_INTERSECT
 | K_EXCEPT
 ;

cte_table_name
 : entity_name ( '(' column_name ( ',' column_name )* ')' )?
 ;

signed_number
 : ( '+' | '-' )? NUMERIC_LITERAL
 ;

literal_value
 : NUMERIC_LITERAL
 | STRING_LITERAL
 | BLOB_LITERAL
 | VECTOR_LITERAL
 | K_NULL
 | K_CURRENT_TIME
 | K_CURRENT_DATE
 | K_CURRENT_TIMESTAMP
 ;

unary_operator
 : '-'
 | '+'
 | '~'
 | K_NOT
 ;

error_message
 : STRING_LITERAL
 ;

column_alias
 : IDENTIFIER
 | STRING_LITERAL
 ;

keyword
 : K_ABORT
 | K_ACTION
 | K_ADD
 | K_AFTER
 | K_ALL
 | K_ALTER
 | K_ANALYZE
 | K_AND
 | K_AS
 | K_ASC
 | K_ATTACH
 | K_AUTOINCREMENT
 | K_BEFORE
 | K_BEGIN
 | K_BETWEEN
 | K_BY
 | K_CASCADE
 | K_CASE
 | K_CAST
 | K_CHECK
 | K_COLLATE
 | K_COLUMN
 | K_COMMIT
 | K_CONFLICT
 | K_CONSTRAINT
 | K_CREATE
 | K_CROSS
 | K_CURRENT_DATE
 | K_CURRENT_TIME
 | K_CURRENT_TIMESTAMP
 | K_SCHEMA
 | K_DEFAULT
 | K_DEFERRABLE
 | K_DEFERRED
 | K_DELETE
 | K_DESC
 | K_DETACH
 | K_DISTINCT
 | K_DROP
 | K_EACH
 | K_ELSE
 | K_END
 | K_ESCAPE
 | K_EXCEPT
 | K_EXCLUSIVE
 | K_EXISTS
 | K_EXPLAIN
 | K_FAIL
 | K_FOR
 | K_FOREIGN
 | K_FROM
 | K_FULL
 | K_GLOB
 | K_HAVING
 | K_IF
 | K_IGNORE
 | K_IMMEDIATE
 | K_IN
 | K_INDEX
 | K_INDEXED
 | K_INITIALLY
 | K_INNER
 | K_INSERT
 | K_INSTEAD
 | K_INTERSECT
 | K_INTO
 | K_IS
 | K_ISNULL
 | K_JOIN
 | K_KEY
 | K_KNN
 | K_LEFT
 | K_LIKE
 | K_LIMIT
 | K_MATCH
 | K_NATURAL
 | K_NO
 | K_NOT
 | K_NOTNULL
 | K_NULL
 | K_OF
 | K_OFFSET
 | K_ON
 | K_OR
 | K_ORDER
 | K_OUTER
 | K_PRAGMA
 | K_PRIMARY
 | K_RAISE
 | K_REFERENCES
 | K_REGEXP
 | K_REINDEX
 | K_RELEASE
 | K_RENAME
 | K_REPLACE
 | K_RESTRICT
 | K_RIGHT
 | K_ROLLBACK
 | K_ROW
 | K_SAVEPOINT
 | K_SELECT
 | K_SET
 | K_ENTITY
 | K_TEMP
 | K_TEMPORARY
 | K_THEN
 | K_TO
 | K_TRANSACTION
 | K_TRIGGER
 | K_UNION
 | K_UNIQUE
 | K_UPDATE
 | K_USING
 | K_VACUUM
 | K_VALUES
 | K_VIEW
 | K_VIRTUAL
 | K_WHEN
 | K_WHERE
 | K_WITH
 | K_WITHOUT
 ;

function_name
 : any_name
 ;

schema_name
 : any_name
 ;

entity_name
 : any_name
 ;

column_name
 : any_name
 ;

collation_name
 : any_name
 ;

index_name
 : any_name
 ;

table_alias
 : any_name
 ;

any_name
 : IDENTIFIER
 | keyword
 | STRING_LITERAL
 | '(' any_name ')'
 ;


// The type supported by Cottontail DB
type_name
 :  T_BOOLEAN
  | T_BYTE
  | T_SHORT
  | T_INTEGER
  | T_LONG
  | T_FLOAT
  | T_DOUBLE
  | T_STRING
  | T_BOOL_VEC
  | T_BYTE_VEC
  | T_SHORT_VEC
  | T_INT_VEC
  | T_LONG_VEC
  | T_FLOAT_VEC
  | T_DOUBLE_VE
  ;

// The distances supported by Cottontail DB
distance_name
 : D_L1
 | D_L2
 ;


SCOL : ';';
DOT : '.';
OPEN_PAR : '(';
CLOSE_PAR : ')';
COMMA : ',';
ASSIGN : '=';
STAR : '*';
PLUS : '+';
MINUS : '-';
TILDE : '~';
PIPE2 : '||';
DIV : '/';
MOD : '%';
LT2 : '<<';
GT2 : '>>';
AMP : '&';
PIPE : '|';
LT : '<';
LT_EQ : '<=';
GT : '>';
GT_EQ : '>=';
EQ : '==';
NOT_EQ1 : '!=';
NOT_EQ2 : '<>';

// Definition of Cottontail DB types
T_BOOLEAN : (B O O L | B O O L E A N);
T_BYTE : B Y T E;
T_SHORT : S H O R T;
T_INTEGER : (I N T E G E R | I N T);
T_LONG : L O N G;
T_FLOAT : F L O A T;
T_DOUBLE : D O U B L E;
T_STRING : (S T R I N G | S T R);
T_BOOL_VEC : (B O O L V E C | B O O L E A N V E C);
T_BYTE_VEC : B Y T E V E C;
T_SHORT_VEC : S H O R T V E C;
T_INT_VEC : (I N T E G E R V E C | I N T V E C);
T_LONG_VEC : L O N G V E C;
T_FLOAT_VEC : F L O A T V E C;
T_DOUBLE_VE : (D O U B L E V E C | D B L V E C);

// Definition of Cottontail DB distance functions
D_L1 : (L '1' | M A N H A T T A N);
D_L2 : (L '2' | E U C L I D I A N);

// http://www.sqlite.org/lang_keywords.html
K_ABORT : A B O R T;
K_ACTION : A C T I O N;
K_ADD : A D D;
K_AFTER : A F T E R;
K_ALL : A L L;
K_ALTER : A L T E R;
K_ANALYZE : A N A L Y Z E;
K_AND : A N D;
K_AS : A S;
K_ASC : A S C;
K_ATTACH : A T T A C H;
K_AUTOINCREMENT : A U T O I N C R E M E N T;
K_BEFORE : B E F O R E;
K_BEGIN : B E G I N;
K_BETWEEN : B E T W E E N;
K_BY : B Y;
K_CASCADE : C A S C A D E;
K_CASE : C A S E;
K_CAST : C A S T;
K_CHECK : C H E C K;
K_COLLATE : C O L L A T E;
K_COLUMN : C O L U M N;
K_COMMIT : C O M M I T;
K_CONFLICT : C O N F L I C T;
K_CONSTRAINT : C O N S T R A I N T;
K_CREATE : C R E A T E;
K_CROSS : C R O S S;
K_CURRENT_DATE : C U R R E N T '_' D A T E;
K_CURRENT_TIME : C U R R E N T '_' T I M E;
K_CURRENT_TIMESTAMP : C U R R E N T '_' T I M E S T A M P;
K_SCHEMA : S C H E M A;
K_DEFAULT : D E F A U L T;
K_DEFERRABLE : D E F E R R A B L E;
K_DEFERRED : D E F E R R E D;
K_DELETE : D E L E T E;
K_DESC : D E S C;
K_DETACH : D E T A C H;
K_DISTINCT : D I S T I N C T;
K_DROP : D R O P;
K_EACH : E A C H;
K_ELSE : E L S E;
K_END : E N D;
K_ESCAPE : E S C A P E;
K_EXCEPT : E X C E P T;
K_EXCLUSIVE : E X C L U S I V E;
K_EXISTS : E X I S T S;
K_EXPLAIN : E X P L A I N;
K_FAIL : F A I L;
K_FOR : F O R;
K_FOREIGN : F O R E I G N;
K_FROM : F R O M;
K_FULL : F U L L;
K_GLOB : G L O B;
K_HAVING : H A V I N G;
K_IF : I F;
K_IGNORE : I G N O R E;
K_IMMEDIATE : I M M E D I A T E;
K_IN : I N;
K_INDEX : I N D E X;
K_INDEXED : I N D E X E D;
K_INITIALLY : I N I T I A L L Y;
K_INNER : I N N E R;
K_INSERT : I N S E R T;
K_INSTEAD : I N S T E A D;
K_INTERSECT : I N T E R S E C T;
K_INTO : I N T O;
K_IS : I S;
K_ISNULL : I S N U L L;
K_JOIN : J O I N;
K_KEY : K E Y;
K_KNN : K N N;
K_LEFT : L E F T;
K_LIKE : L I K E;
K_LIMIT : L I M I T;
K_MATCH : M A T C H;
K_NATURAL : N A T U R A L;
K_NO : N O;
K_NOT : N O T;
K_NOTNULL : N O T N U L L;
K_NULL : N U L L;
K_OF : O F;
K_OFFSET : O F F S E T;
K_ON : O N;
K_OR : O R;
K_ORDER : O R D E R;
K_OUTER : O U T E R;
K_PRAGMA : P R A G M A;
K_PRIMARY : P R I M A R Y;
K_RAISE : R A I S E;
K_REFERENCES : R E F E R E N C E S;
K_REGEXP : R E G E X P;
K_REINDEX : R E I N D E X;
K_RELEASE : R E L E A S E;
K_RENAME : R E N A M E;
K_REPLACE : R E P L A C E;
K_RESTRICT : R E S T R I C T;
K_RIGHT : R I G H T;
K_ROLLBACK : R O L L B A C K;
K_ROW : R O W;
K_SAVEPOINT : S A V E P O I N T;
K_SELECT : S E L E C T;
K_SET : S E T;
K_ENTITY : E N T I T Y;
K_TEMP : T E M P;
K_TEMPORARY : T E M P O R A R Y;
K_THEN : T H E N;
K_TO : T O;
K_TRANSACTION : T R A N S A C T I O N;
K_TRIGGER : T R I G G E R;
K_UNION : U N I O N;
K_UNIQUE : U N I Q U E;
K_UPDATE : U P D A T E;
K_USING : U S I N G;
K_VACUUM : V A C U U M;
K_VALUES : V A L U E S;
K_VIEW : V I E W;
K_VIRTUAL : V I R T U A L;
K_WHEN : W H E N;
K_WHERE : W H E R E;
K_WITH : W I T H;
K_WITHOUT : W I T H O U T;

IDENTIFIER
 : '"' (~'"' | '""')* '"'
 | '`' (~'`' | '``')* '`'
 | [a-zA-Z_] [a-zA-Z_0-9]*
 ;

INTEGER_LITERAL
 : DIGIT+
 ;

NUMERIC_LITERAL
 : DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
 | '.' DIGIT+ ( E [-+]? DIGIT+ )?
 ;

VECTOR_LITERAL
 : '[' NUMERIC_LITERAL (',' NUMERIC_LITERAL)+ ']'
 ;

BIND_PARAMETER
 : '?' DIGIT*
 | [:@$] IDENTIFIER
 ;

STRING_LITERAL
 : '\'' ( ~'\'' | '\'\'' )* '\''
 ;

BLOB_LITERAL
 : X STRING_LITERAL
 ;

SINGLE_LINE_COMMENT
 : '--' ~[\r\n]* -> channel(HIDDEN)
 ;

MULTILINE_COMMENT
 : '/*' .*? ( '*/' | EOF ) -> channel(HIDDEN)
 ;

SPACES
 : [ \u000B\t\r\n] -> channel(HIDDEN)
 ;

UNEXPECTED_CHAR
 : .
 ;

fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];