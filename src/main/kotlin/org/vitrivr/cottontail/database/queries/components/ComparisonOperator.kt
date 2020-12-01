package org.vitrivr.cottontail.database.queries.components

import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.values.PatternValue
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * Enumeration of query [ComparisonOperator]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
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
    fun match(left: Value?, right: Collection<Value>): Boolean {
        if (left == null) {
            return when (this) {
                ISNULL -> true
                ISNOTNULL -> false
                else -> throw QueryException("Incompatible operands for operator $this: Left operand cannot be null!")
            }
        } else {
            return when (this) {
                EQUAL -> left.isEqual(right.firstOrNull()
                        ?: throw QueryException("Incompatible operands for operator $this: Right operand cannot be null!"))
                GREATER -> left > right.firstOrNull() ?: throw QueryException("Incompatible operands for operator $this: Right operand cannot be null!")
                LESS -> left < right.firstOrNull() ?: throw QueryException("Incompatible operands for operator $this: Right operand cannot be null!")
                GEQUAL -> left >= right.firstOrNull() ?: throw QueryException("Incompatible operands for operator $this: Right operand cannot be null!")
                LEQUAL -> left <= right.firstOrNull() ?: throw QueryException("Incompatible operands for operator $this: Right operand cannot be null!")
                IN -> right.contains(left)
                BETWEEN -> left >= (right.firstOrNull()
                        ?: throw QueryException("Incompatible operands for operator $this: Right operand cannot be null!"))
                        && left <= (right.lastOrNull()
                        ?: throw QueryException("Incompatible operands for operator $this: Right operand cannot be null!"))
                LIKE -> {
                    val check = right.first()
                    if (left is PatternValue && check is StringValue) {
                        left.matches(check)
                    } else {
                        throw QueryException("Incompatible operands for operator $this (left = ${left::class.simpleName}, left = ${check::class.simpleName})!")
                    }
                }
                ISNULL -> false
                ISNOTNULL -> true
            }
        }
    }
}