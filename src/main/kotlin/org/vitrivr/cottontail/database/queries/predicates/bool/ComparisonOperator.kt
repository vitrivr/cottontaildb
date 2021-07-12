package org.vitrivr.cottontail.database.queries.predicates.bool

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.pattern.LikePatternValue
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*


/**
 * The sealed [ComparisonOperator]s class.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class ComparisonOperator {

    /** The atomic CPU cost of matching this [ComparisonOperator] with a [Value]. */
    abstract val atomicCpuCost: Float

    /**
     * Matches the given [Value] to this [ComparisonOperator] and returns true,
     * if there is a match (i.e. value satisfies operation) and false otherwise.
     *
     * @return True on match, false otherwise.
     */
    abstract fun match(): Boolean

    /**
     * A [ComparisonOperator] that checks if a value is NULL.
     */
    class IsNull(val left: Binding<*>) : ComparisonOperator() {
        override val atomicCpuCost: Float = Cost.COST_MEMORY_ACCESS
        override fun match() = (this.left.value == null)
    }

    /**
     * A [ComparisonOperator] that expresses an equality (==) comparison.
     */
    sealed class Binary(val left: Binding<*>, val right: Binding<*>) : ComparisonOperator() {

        override val atomicCpuCost: Float = 2 * Cost.COST_MEMORY_ACCESS

        class Equal(left: Binding<*>, right: Binding<*>) : Binary(left, right) {
            override fun match() = this.left.value.isEqual(this.right.value)
            override fun toString(): String = "= $right"
        }

        /**
         * A [ComparisonOperator] that expresses greater (>) comparison.
         */
        class Greater(left: Binding<*>, right: Binding<*>) : Binary(left, right) {
            override fun match(): Boolean = this.left.value > this.right.value
            override fun toString(): String = "> $right"

        }

        /**
         * A [ComparisonOperator] that expresses less (<) comparison.
         */
        class Less(left: Binding<*>, right: Binding<*>) : Binary(left, right) {
            override fun match() = this.left.value < this.right.value
            override fun toString(): String = "< $right"
        }

        /**
         * A [ComparisonOperator] that expresses greater or equal (>=) comparison.
         */
        class GreaterEqual(left: Binding<*>, right: Binding<*>) : Binary(left, right) {
            override fun match() = this.left.value >= this.right.value
            override fun toString(): String = ">= $right"
        }

        /**
         * A [ComparisonOperator] that expresses less or equal (<=) comparison.
         */
        class LessEqual(left: Binding<*>, right: Binding<*>) : Binary(left, right) {
            override fun match() = this.left.value <= this.right.value
            override fun toString(): String = "<= $right"
        }

        /**
         * A [ComparisonOperator] that expresses a LIKE comparison. I.e. left LIKE right.
         */
        class Like(left: Binding<*>, right: Binding<*>) : Binary(left, right) {
            override fun match() = (this.right.value as LikePatternValue).matches(left as StringValue)
            override fun toString(): String = "LIKE $right"
        }

        /**
         * A [ComparisonOperator] that expresses a MATCH comparison. Can only be evaluated through a lucene index.
         */
        class Match(left: Binding<*>, right: Binding<*>) : Binary(left, right) {
            override fun match() = throw UnsupportedOperationException("A MATCH comparison operator cannot be evaluated directly.")
            override fun toString(): String = "MATCH $right"
        }
    }

    /**
     * A [ComparisonOperator] that expresses a BETWEEN comparison (i.e. lower <= left <= upper).
     */
    class Between(val left: Binding<*>, val rightLower: Binding<*>, val rightUpper: Binding<*>) : ComparisonOperator() {
        override val atomicCpuCost: Float = 4.0f * Cost.COST_MEMORY_ACCESS
        override fun match() = this.left.value in this.rightLower.value..this.rightUpper.value
        override fun toString(): String = "BETWEEN $rightLower, $rightUpper"
    }

    /**
     * A [ComparisonOperator] that expresses a IN comparison (i.e. left IN right).
     */
    class In(val left: Binding<*>, right: MutableList<Binding<*>>) : ComparisonOperator() {
        val right: MutableList<Binding<*>> = LinkedList()
        private var rightSet: ObjectOpenHashSet<Value>? = null /* To speed-up IN operation. */
        override val atomicCpuCost: Float = 4.0f * Cost.COST_MEMORY_ACCESS
        override fun match(): Boolean {
            if (this.rightSet == null) {
                this.rightSet = ObjectOpenHashSet()
                this.right.forEach { this.rightSet!!.add(it.value) }
            }
            return this.left.value in this.rightSet!!
        }

        init {
            right.forEach { this.addRef(it) }
        }

        /**
         * Adds a [Binding] to this [In] operator.
         *
         * @param ref [Binding] to add.
         */
        fun addRef(ref: Binding<*>) {
            this.right.add(ref)
            this.rightSet = null
        }

        override fun toString(): String = "IN [${this.right.joinToString(",")}]"
    }
}