package ch.unibas.dmi.dbis.cottontail.sql

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.sql.metamodel.LiteralExpression
import ch.unibas.dmi.dbis.cottontail.sql.metamodel.NumberLiteralExpression
import ch.unibas.dmi.dbis.cottontail.sql.metamodel.StringLiteralExpression
import ch.unibas.dmi.dbis.cottontail.sql.metamodel.VectorLiteralExpression


/**
 * A context class used for translating a raw CottonSQL query (string) into an executable, abstract syntax tree.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal data class Context(val catalogue: Catalogue, val parameters: List<Any>, val default: String? = null) {
    /**
     * Fetches and returns the parameter bound for the given index.
     *
     * @param index The index of the parameter.
     * @return LiteralExpression
     */
    fun getBoundParameter(index: Int) : LiteralExpression {
        try {
            val value = this.parameters[index]
            return when(value) {
                is String -> StringLiteralExpression(value)
                is FloatArray -> VectorLiteralExpression(value.toTypedArray())
                is DoubleArray -> VectorLiteralExpression(value.toTypedArray())
                is ByteArray -> VectorLiteralExpression(value.toTypedArray())
                is ShortArray -> VectorLiteralExpression(value.toTypedArray())
                is IntArray -> VectorLiteralExpression(value.toTypedArray())
                is LongArray -> VectorLiteralExpression(value.toTypedArray())
                is Float -> NumberLiteralExpression(value)
                is Double -> NumberLiteralExpression(value)
                is Byte -> NumberLiteralExpression(value)
                is Short -> NumberLiteralExpression(value)
                is Int -> NumberLiteralExpression(value)
                is Long -> NumberLiteralExpression(value)
                else -> throw QueryParsingException("Value for parameter binding '$index' is not supported.")
            }
        } catch (e: IndexOutOfBoundsException) {
            throw QueryParsingException("No value for parameter binding '$index'; index is out of bounds!")
        }
    }
}