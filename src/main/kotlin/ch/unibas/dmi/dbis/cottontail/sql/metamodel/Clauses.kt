package ch.unibas.dmi.dbis.cottontail.sql.metamodel

import ch.unibas.dmi.dbis.cottontail.sql.Context
import ch.unibas.dmi.dbis.cottontail.sql.QueryParsingException
import ch.unibas.dmi.dbis.cottontail.sql.antlr.CottonSQLParser

/** */
sealed class Clause

/** */
data class OrderClause(val clause: Expression, val order: Order = Order.ASC): Clause() {
    enum class Order { ASC, DESC }
}

/** */
data class LimitClause(val limit: Int, val offset: Int = 0) : Clause()

/** */
data class ProjectionClause(val type: ProjectionType, val expression: Expression?, val entity: String?, val alias: String?) : Clause() {
    enum class ProjectionType {
        STAR, EXPRESSION
    }
}


/**
 * A SQL 'FROM' clause.
 */
sealed class FromClause(val alias: String?) : Clause()

class SimpleEntityFromClause(val entity: String, val schema: String, alias: String?) : FromClause(alias)

class SubselectFromClause(val statement: Statement, alias: String?) : FromClause(null)

class JoinFromClause : FromClause(null)

internal fun CottonSQLParser.From_clauseContext.toAst(context: Context): FromClause = when {
    !this.entity_or_subselect().isEmpty -> this.entity_or_subselect().toAst(context)
    !this.join_clause().isEmpty -> this.join_clause().toAst(context)
    else -> throw QueryParsingException("Malformed FROM clause in query.")
}



internal fun CottonSQLParser.Entity_or_subselectContext.toAst(context: Context): FromClause = when {
    !this.simple_entity_clause().isEmpty -> this.simple_entity_clause().toAst(context)
    !this.subselect_clause().isEmpty -> this.subselect_clause().toAst(context)
    else -> throw QueryParsingException("Malformed FROM clause in query.")
}

internal fun CottonSQLParser.Join_clauseContext.toAst(context: Context): FromClause = JoinFromClause()

/**
 *
 */
internal fun CottonSQLParser.Simple_entity_clauseContext.toAst(context: Context): SimpleEntityFromClause {
    val schema = (this.schema_name()?.text ?: context.default) ?: throw QueryParsingException("Schema is not specified in simple FROM clause.")
    val entity = this.entity_name()?.text ?: throw QueryParsingException("Entity is not specified in simple FROM clause.")
    return SimpleEntityFromClause(entity, schema , this.table_alias()?.text)
}

/**
 *
 */
internal fun CottonSQLParser.Subselect_clauseContext.toAst(context: Context): SubselectFromClause {
    val alias = this.table_alias()?.text ?: throw QueryParsingException("No alias was specified for sub-select FROM clause.")
    return SubselectFromClause(this.select_stmt().toAst(context) , alias)
}


/**
 *
 */
internal fun CottonSQLParser.Result_columnContext.toAst(context: Context) = when {
    this.STAR() != null -> ProjectionClause(type = ProjectionClause.ProjectionType.STAR, entity = this.entity_name()?.text, alias = this.column_alias()?.text, expression = null)
    else -> ProjectionClause(type = ProjectionClause.ProjectionType.EXPRESSION, expression = this.expr().toAst(context), alias = this.column_alias()?.text, entity = null)
}

/**
 * Converts a [Ordering_termContext] to its AST representation.
 *
 * @param context The [Context] required for AST conversion
 */
internal fun CottonSQLParser.Order_clauseContext.toAst(context: Context): OrderClause = when {
    this.K_ASC() != null -> OrderClause(this.expr().toAst(context), OrderClause.Order.ASC)
    this.K_DESC() != null -> OrderClause(this.expr().toAst(context), OrderClause.Order.DESC)
    else -> OrderClause(this.expr().toAst(context), OrderClause.Order.ASC)
}