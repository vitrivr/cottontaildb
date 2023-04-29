package org.vitrivr.cottontail.core.queries.predicates

import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.NodeWithCost
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.pattern.LikePatternValue
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A [ComparisonOperator] is used as part of a [BooleanPredicate] to
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
sealed interface ComparisonOperator: NodeWithCost {
    /** The [Binding] that acts as left operand for this [ComparisonOperator]. */
    val left: Binding

    /**
     * Matches the given [Value] to this [ComparisonOperator] and returns true,
     * if there is a match (i.e. value satisfies operation) and false otherwise.
     *
     * @return True on match, false otherwise.
     */
    context(BindingContext,Record)
    fun match(): Boolean

    /**
     * A [ComparisonOperator] that checks if a value is NULL.
     */
    data class IsNull(override val left: Binding) : ComparisonOperator {
        override val cost: Cost
            get() = Cost.MEMORY_ACCESS
        context(BindingContext,Record)
        override fun match() = (this.left.getValue() == null)
        override fun digest(): Digest = this.hashCode().toLong()
    }

    /**
     * A [ComparisonOperator] that expresses an equality (==) comparison.
     */
    sealed interface Binary : ComparisonOperator {
        /** The [Binding] that acts as right operand for this [ComparisonOperator]. */
        val right: Binding

        /** The cost of evaluating a [Binary]. */
        override val cost: Cost
            get() = Cost.MEMORY_ACCESS * 5

        data class Equal(override val left: Binding, override val right: Binding): Binary {
            context(BindingContext,Record)
            override fun match() = this.left.getValue() != null && this.right.getValue() != null && this.left.getValue()!!.isEqual(this.right.getValue()!!)
            override fun toString(): String = "$left = $right"
            override fun digest(): Digest = this.hashCode().toLong()
        }

        /**
         * A [ComparisonOperator] that expresses greater (>) comparison.
         */
        data class Greater(override val left: Binding, override val right: Binding): Binary {
            context(BindingContext,Record)
            override fun match(): Boolean = this.left.getValue() != null && this.right.getValue() != null && this.left.getValue()!! > this.right.getValue()!!
            override fun toString(): String = "$left > $right"
            override fun digest(): Digest = this.hashCode().toLong()
        }

        /**
         * A [ComparisonOperator] that expresses less (<) comparison.
         */
        data class Less(override val left: Binding, override val right: Binding) : Binary {
            context(BindingContext,Record)
            override fun match() = this.left.getValue() != null && this.right.getValue() != null && this.left.getValue()!! < this.right.getValue()!!
            override fun toString(): String = "$left < $right"
            override fun digest(): Digest = this.hashCode().toLong()
        }

        /**
         * A [ComparisonOperator] that expresses greater or equal (>=) comparison.
         */
        data class GreaterEqual(override val left: Binding, override val right: Binding) : Binary {
            context(BindingContext,Record)
            override fun match() = this.left.getValue() != null && this.right.getValue() != null && this.left.getValue()!! >= this.right.getValue()!!
            override fun toString(): String = "$left >= $right"
            override fun digest(): Digest = this.hashCode().toLong()
        }

        /**
         * A [ComparisonOperator] that expresses less or equal (<=) comparison.
         */
        data class LessEqual(override val left: Binding, override val right: Binding) : Binary {
            context(BindingContext,Record)
            override fun match() = this.left.getValue() != null && this.right.getValue() != null && this.left.getValue()!! <= this.right.getValue()!!
            override fun toString(): String = "$left <= $right"
            override fun digest(): Digest = this.hashCode().toLong()
        }

        /**
         * A [ComparisonOperator] that expresses a LIKE comparison, i.e., left LIKE right.
         */
        class Like(override val left: Binding, override val right: Binding) : Binary {
            context(BindingContext,Record)
            override fun match() = this.left.getValue() is StringValue && this.right.getValue() is LikePatternValue && (this.right.getValue() as LikePatternValue).matches(this.left.getValue() as StringValue)
            override fun toString(): String = "$left LIKE $right"
            override fun digest(): Digest = this.hashCode().toLong()
        }

        /**
         * A [ComparisonOperator] that expresses a MATCH comparison. Can only be evaluated through a lucene index.
         */
        class Match(override val left: Binding, override val right: Binding) : Binary {
            context(BindingContext,Record)
            override fun match() = throw UnsupportedOperationException("A MATCH comparison operator cannot be evaluated directly.")
            override fun toString(): String = "$left MATCH $right"
            override fun digest(): Digest = this.hashCode().toLong()
        }
    }

    /**
     * A [ComparisonOperator] that expresses a BETWEEN comparison (i.e. lower <= left <= upper).
     */
    data class Between(override val left: Binding, val rightLower: Binding, val rightUpper: Binding) : ComparisonOperator {
        override val cost: Cost
            get() = Cost.MEMORY_ACCESS * 4
        context(BindingContext,Record)
        override fun match() = this.left.getValue() != null && this.rightLower.getValue() != null && this.rightLower.getValue() != null && this.left.getValue()!! in this.rightLower.getValue()!!..this.rightUpper.getValue()!!
        override fun digest(): Digest = this.hashCode().toLong()
        override fun toString(): String = "$left BETWEEN $rightLower, $rightUpper"
    }

    /**
     * A [ComparisonOperator] that expresses an IN comparison (i.e. left IN right).
     */
    class In(override val left: Binding, val right: List<Binding>) : ComparisonOperator {

        /** Cost of executing this [ComparisonOperator.In]*/
        override val cost: Cost
            get() = Cost.MEMORY_ACCESS * 2 * this.right.size

        /** Internal set to facilitate lookup. */
        private val lookupSet = ObjectLinkedOpenHashSet<Value>()

        init {
            /* Sanity check + initialization of values list. */
            require(this.right.all { it !is Binding.Column }) { "Right-hand side of IN operator cannot be a column reference." }
        }

        /**
         * Compares [left] to [right] for this [ComparisonOperator.In] and returns true on match and false otherwise,
         *
         * @return True on match, false otherwise.
         */
        context(BindingContext,Record)
        override fun match(): Boolean {
            if (this.lookupSet.isEmpty()) {
                this.initialize()
            }

            val value = this.left.getValue()
            return if (value is VectorValue<*>) {
                matchVector(value)
            } else {
                value in this.lookupSet
            }
        }
        override fun digest(): Digest = this.hashCode().toLong()
        override fun toString(): String = "$left IN [${this.right.joinToString(",")}]"

        /**
         * Performs the IN matching with vector values. Resorts to brute-force comparison.
         *
         * @param v1 The [VectorValue] to compare.
         * TODO: Remove, once isEqual() / hashCode() can be overriden in value classes.
         */
        context(BindingContext,Record)
        private fun matchVector(v1: VectorValue<*>): Boolean {
            for (v2 in this.lookupSet) {
                if (v1.isEqual(v2)) return true
            }
            return false
        }

        context(BindingContext,Record)
        private fun initialize() {
            for (r in this.right) {
                if (r is Binding.Subquery) {
                    this.lookupSet.addAll(r.getValues())
                } else {
                    this.lookupSet.add(r.getValue())
                }
            }
        }
    }
}