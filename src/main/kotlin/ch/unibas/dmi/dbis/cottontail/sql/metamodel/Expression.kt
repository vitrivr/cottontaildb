package ch.unibas.dmi.dbis.cottontail.sql.metamodel

import ch.unibas.dmi.dbis.cottontail.sql.Context
import ch.unibas.dmi.dbis.cottontail.sql.QueryParsingException
import ch.unibas.dmi.dbis.cottontail.sql.antlr.CottonSQLParser

/** A simple CottonSQL [Statement] */
sealed class Expression

sealed class LiteralExpression : Expression()

data class NumberLiteralExpression<T: Number>(val value: T) : LiteralExpression()

data class StringLiteralExpression(val value: String) : LiteralExpression()

data class VectorLiteralExpression<T : Number>(val value: Array<T>) : LiteralExpression()

object NullLiteralExpression : LiteralExpression()

class QualifiedColumnNameLiteralExpression(val column: String, val entity: String?, val schema: String?) : LiteralExpression()

/**
 *
 */
internal fun CottonSQLParser.ExprContext.toAst(context: Context): Expression = when {
    this.literal_value() != null -> this.literal_value().toAst()
    this.BIND_PARAMETER() != null -> context.getBoundParameter(this.BIND_PARAMETER().text.replace("?","").toInt())
    this.qualified_column_name() != null -> this.qualified_column_name().toAst()
    else -> throw QueryParsingException("Expression '${this}' does not contain any supported value.")
}

/**
 *
 */
internal fun CottonSQLParser.Qualified_column_nameContext.toAst() = QualifiedColumnNameLiteralExpression(this.column_name().text, this.entity_name()?.text, this.schema_name()?.text)

/**
 *
 */
internal fun CottonSQLParser.Literal_valueContext.toAst() = when {
    this.STRING_LITERAL() != null -> StringLiteralExpression(this.STRING_LITERAL().text)
    this.NUMERIC_LITERAL() != null -> NumberLiteralExpression(this.NUMERIC_LITERAL().text.toBigDecimal())
    this.VECTOR_LITERAL() != null -> VectorLiteralExpression(this.VECTOR_LITERAL().text.removePrefix("[").removeSuffix("]").split(",").map { it.toBigDecimal() }.toTypedArray())
    this.K_NULL() != null -> NullLiteralExpression
    else -> throw QueryParsingException("Literal expression '${this}' does not contain any supported value.")
}