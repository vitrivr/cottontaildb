package ch.unibas.dmi.dbis.cottontail.sql

import ch.unibas.dmi.dbis.cottontail.sql.antlr.CottonSQLParser
import ch.unibas.dmi.dbis.cottontail.sql.metamodel.*


/**
 * Converts a [Sql_stmt_listContext] to its AST representation.
 *
 * @param context The [Context] required for AST conversion
 */
internal fun CottonSQLParser.Sql_stmt_listContext.toAst(context: Context) = StatementList(this.sql_stmt().map { it.toAst(context) }.toTypedArray())

/**
 * Converts a [Sql_stmtContext] to its AST representation.
 *
 * @param context The [Context] required for AST conversion
 */
internal fun CottonSQLParser.Sql_stmtContext.toAst(context: Context): Statement {
    val x = this.statement().children[0]
    return when (x) {
        is CottonSQLParser.Select_stmtContext -> x.toAst(context)
        else -> throw UnsupportedOperationException(this.javaClass.canonicalName)
    }
}

/**
 * Converts a [Limit_termContext] to its AST representation.
 *
 * @param context The [Context] required for AST conversion
 */
internal fun CottonSQLParser.Limit_clauseContext.toAst(): LimitClause = LimitClause(this.INTEGER_LITERAL(0)!!.text.toInt(), this.INTEGER_LITERAL(1)?.text?.toInt() ?: 0)

