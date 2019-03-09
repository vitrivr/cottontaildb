package ch.unibas.dmi.dbis.cottontail.database.queries

import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException

/**
 * List of query [ComparisonOperator]s.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
enum class ComparisonOperator {
    EQUAL, /* One entry on right-hand side required! */
    GREATER, /* One entry on right-hand side required! */
    LESS, /* One entry on right-hand side required! */
    GEQUAL, /* One entry on right-hand side required! */
    LEQUAL, /* One entry on right-hand side required! */
    LIKE, /* One entry on right-hand side required! */
    IN, /* One to n entries on right-hand side required! */
    BETWEEN, /* Two entries on right-hand side required! */
    ISNULL, /* No right-hand side required! */
    ISNOTNULL; /* No right-hand side required! */

    /**
     * Matches the left hand side to the right hand side given this [ComparisonOperator].
     *
     * @param left Left-hand side of the operator.
     * @param right Right-hand side of the operator.
     * @return True on match, false otherwise.
     */
    fun <T: Any> match(left: T?, right: Collection<T>) : Boolean = when {
        this == EQUAL && left != null -> matchEqual(left, right.first())
        this == GREATER && left is Number -> matchGreater(left, right.first() as Number)
        this == LESS && left is Number -> matchLess(left, right.first() as Number)
        this == GEQUAL && left is Number -> matchGreaterEqual(left, right.first() as Number)
        this == LEQUAL && left is Number -> matchLessEqual(left, right.first() as Number)
        this == IN && left != null -> matchIn(left,right)
        this == BETWEEN && left is Number -> matchBetween(left, right.first() as Number, right.last() as Number)
        this == ISNULL -> left == null
        this == ISNOTNULL -> left != null
        else -> throw QueryException("Unknown operator '$this' or incompatible type!")
    }

    /**
     * Performs an equality check between the left and the right value.
     *
     * @param left The left value.
     * @param right The right value.
     *
     * @return True if match, false otherwise.
     */
    private fun <T: Any> matchEqual(left: T?, right: T) : Boolean = (left == right)

    /**
     * Performs a greater than comparison between the left and the right value. The values must be numeric!
     *
     * @param left The left value.
     * @param right The right value.
     *
     * @return True if match, false otherwise.
     */
    private fun <T: Number> matchGreater(left: T, right: T) = when (left) {
        is Double -> left > right.toDouble()
        is Float -> left > right.toFloat()
        is Long -> left > right.toLong()
        is Int -> left > right.toInt()
        is Short -> left > right.toShort()
        is Byte -> left > right.toByte()
        else -> throw QueryException.TypeException()
    }

    /**
     * Performs a less than comparison between the left and the right value. The values must be numeric!
     *
     * @param left The left value.
     * @param right The right value.
     *
     * @return True if match, false otherwise.
     */
    private fun <T: Number> matchLess(left: T, right: T) = when (left) {
        is Double -> left < right.toDouble()
        is Float -> left < right.toFloat()
        is Long -> left < right.toLong()
        is Int -> left < right.toInt()
        is Short -> left < right.toShort()
        is Byte -> left < right.toByte()
        else -> throw QueryException.TypeException()
    }

    /**
     * Performs a greater OR equal than comparison between the left and the right value. The values must be numeric!
     *
     * @param left The left value.
     * @param right The right value.
     *
     * @return True if match, false otherwise.
     */
    private fun <T: Number> matchGreaterEqual(left: T, right: T) = when (left) {
        is Double -> left >= right.toDouble()
        is Float -> left >= right.toFloat()
        is Long -> left >= right.toLong()
        is Int -> left >= right.toInt()
        is Short -> left >= right.toShort()
        is Byte -> left >= right.toByte()
        else -> throw DatabaseException("Type error: The provided column value is not of a numeric type.")
    }

    /**
     * Performs a less OR equal than comparison between the left and the right value. The values must be numeric!
     *
     * @param left The left value.
     * @param right The right value.
     *
     * @return True if match, false otherwise.
     */
    private fun <T: Number> matchLessEqual(left: T, right: T) = when (left) {
        is Double -> left <= right.toDouble()
        is Float -> left <= right.toFloat()
        is Long -> left <= right.toLong()
        is Int -> left <= right.toInt()
        is Short -> left <= right.toShort()
        is Byte -> left <= right.toByte()
        else -> throw DatabaseException("Type error: The provided column value is not of a numeric type.")
    }

    /**
     *
     */
    private fun <T: Any> matchIn(value: T, values: Collection<T>) : Boolean = (values.contains(value))

    /**
     *
     */
    private fun <T: Number> matchBetween(left: T, rightLower: T, rightUpper: T) = when (left) {
        is Double -> left >= rightLower.toDouble() && left <= rightUpper.toDouble()
        is Float -> left >= rightLower.toFloat() && left <= rightUpper.toFloat()
        is Long -> left >= rightLower.toLong() && left <= rightUpper.toLong()
        is Int -> left >= rightLower.toInt() && left <= rightUpper.toInt()
        is Short -> left >= rightLower.toShort() && left <= rightUpper.toShort()
        is Byte -> left >= rightLower.toByte() && left <= rightUpper.toByte()
        else -> throw DatabaseException("Type error: The provided column value is not of a numeric type.")
    }
}