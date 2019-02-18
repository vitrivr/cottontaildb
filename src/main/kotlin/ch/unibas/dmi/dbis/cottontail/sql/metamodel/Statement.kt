package ch.unibas.dmi.dbis.cottontail.sql.metamodel

import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.Distance
import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.DistanceFunction
import ch.unibas.dmi.dbis.cottontail.sql.Context
import ch.unibas.dmi.dbis.cottontail.sql.QueryParsingException
import ch.unibas.dmi.dbis.cottontail.sql.antlr.CottonSQLParser
import ch.unibas.dmi.dbis.cottontail.sql.toAst


/** A simple CottonSQL [Statement] */
sealed class Statement

/** A [QueryStatement] */
sealed class QueryStatement(val from: FromClause, val projection: List<ProjectionClause>, val limit: LimitClause?, val order: List<OrderClause>?): Statement()

/** */
class KnnSelectStatement(val column: String, val distance : DistanceFunction, val query : Expression, from: FromClause, projection: List<ProjectionClause>, limit: LimitClause, order: List<OrderClause>): QueryStatement(from, projection, limit, order)

/**
 * Converts a [Sql_stmtContext] to its AST representation.
 *
 * @param context The [Context] required for AST conversion
 */
internal fun CottonSQLParser.Select_stmtContext.toAst(context: Context): Statement = when {
    !this.select_core().knn_expression().isEmpty -> KnnSelectStatement(
            column = this.select_core().knn_expression().column_name().text,
            distance = Distance.valueOf(this.select_core().knn_expression().distance_name().text),
            query = this.select_core().knn_expression().expr().toAst(context),
            from = this.select_core().from_clause().toAst(context),
            projection = this.select_core().result_column().map { it.toAst(context) },
            limit = this.limit_clause()?.toAst() ?: throw QueryParsingException("kNN query is missing a LIMIT clause."),
            order = this.order_clause().map { it.toAst(context) }
    )
    else -> throw UnsupportedOperationException(this.javaClass.canonicalName)
}