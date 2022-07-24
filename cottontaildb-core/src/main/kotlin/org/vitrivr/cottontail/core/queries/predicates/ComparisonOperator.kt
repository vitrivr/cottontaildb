package org.vitrivr.cottontail.core.queries.predicates

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.BindableNode
import org.vitrivr.cottontail.core.queries.nodes.NodeWithCost
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.pattern.LikePatternValue
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A [ComparisonOperator] is used as part of a [BooleanPredicate] to
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
sealed interface ComparisonOperator: BindableNode, NodeWithCost {
    /** The [Binding] that acts as left operand for this [ComparisonOperator]. */
    val left: Binding

    /**
     * Matches the given [Value] to this [ComparisonOperator] and returns true,
     * if there is a match (i.e. value satisfies operation) and false otherwise.
     *
     * @return True on match, false otherwise.
     */
    fun match(): Boolean

    /**
     * Copies this [ComparisonOperator], creating a new [ComparisonOperator] that is initially bound to the same [Binding]s.
     *
     * @return Copy of this [ComparisonOperator]
     */
    override fun copy(): ComparisonOperator

    /**
     * A [ComparisonOperator] that checks if a value is NULL.
     */
    data class IsNull(override val left: Binding) : ComparisonOperator {
        override val cost: Cost
            get() = Cost.MEMORY_ACCESS
        override fun match() = (this.left.value == null)
        override fun copy() = IsNull(this.left.copy())
        override fun digest(): Digest = this.hashCode().toLong()
        override fun bind(context: BindingContext) = this.left.bind(context)
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

        /**
         * Copies this [Binary], creating a new [Binary] that is initially bound to the same [Binding]s.
         *
         * @return Copy of this [Binary]
         */
        override fun copy(): Binary

        /**
         * Binds all [Binding]s contained in this [ComparisonOperator.Binary] to the new [BindingContext].
         *
         * @param context The new [BindingContext] to bind [Binding]s to.
         */
        override fun bind(context: BindingContext) {
            this.left.bind(context)
            this.right.bind(context)
        }

        data class Equal(override val left: Binding, override val right: Binding): Binary {
            override fun match() = this.left.value != null && this.right.value != null && this.left.value!!.isEqual(this.right.value!!)
            override fun toString(): String = "$left = $right"
            override fun copy() = Equal(this.left.copy(), this.right.copy())
            override fun digest(): Digest = this.hashCode().toLong()
        }

        /**
         * A [ComparisonOperator] that expresses greater (>) comparison.
         */
        data class Greater(override val left: Binding, override val right: Binding): Binary {
            override fun match(): Boolean = this.left.value != null && this.right.value != null && this.left.value!! > this.right.value!!
            override fun toString(): String = "$left > $right"
            override fun copy() = Greater(this.left.copy(), this.right.copy())
            override fun digest(): Digest = this.hashCode().toLong()
        }

        /**
         * A [ComparisonOperator] that expresses less (<) comparison.
         */
        data class Less(override val left: Binding, override val right: Binding) : Binary {
            override fun match() = this.left.value != null && this.right.value != null && this.left.value!! < this.right.value!!
            override fun toString(): String = "$left < $right"
            override fun copy() = Less(this.left.copy(), this.right.copy())
            override fun digest(): Digest = this.hashCode().toLong()
        }

        /**
         * A [ComparisonOperator] that expresses greater or equal (>=) comparison.
         */
        data class GreaterEqual(override val left: Binding, override val right: Binding) : Binary {
            override fun match() = this.left.value != null && this.right.value != null && this.left.value!! >= this.right.value!!
            override fun toString(): String = "$left >= $right"
            override fun copy() = GreaterEqual(this.left.copy(), this.right.copy())
            override fun digest(): Digest = this.hashCode().toLong()
        }

        /**
         * A [ComparisonOperator] that expresses less or equal (<=) comparison.
         */
        data class LessEqual(override val left: Binding, override val right: Binding) : Binary {
            override fun match() = this.left.value != null && this.right.value != null && this.left.value!! <= this.right.value!!
            override fun toString(): String = "$left <= $right"
            override fun copy() = LessEqual(this.left.copy(), this.right.copy())
            override fun digest(): Digest = this.hashCode().toLong()
        }

        /**
         * A [ComparisonOperator] that expresses a LIKE comparison, i.e., left LIKE right.
         */
        class Like(override val left: Binding, override val right: Binding) : Binary {
            override fun match() = this.left.value is StringValue && this.right.value is LikePatternValue && (this.right.value as LikePatternValue).matches(this.left.value as StringValue)
            override fun toString(): String = "$left LIKE $right"
            override fun copy() = Like(this.left.copy(), this.right.copy())
            override fun digest(): Digest = this.hashCode().toLong()
        }

        /**
         * A [ComparisonOperator] that expresses a MATCH comparison. Can only be evaluated through a lucene index.
         */
        class Match(override val left: Binding, override val right: Binding) : Binary {
            override fun match() = throw UnsupportedOperationException("A MATCH comparison operator cannot be evaluated directly.")
            override fun toString(): String = "$left MATCH $right"
            override fun copy() = Match(this.left.copy(), this.right.copy())
            override fun digest(): Digest = this.hashCode().toLong()
        }
    }

    /**
     * A [ComparisonOperator] that expresses a BETWEEN comparison (i.e. lower <= left <= upper).
     */
    data class Between(override val left: Binding, val rightLower: Binding, val rightUpper: Binding) : ComparisonOperator {
        override val cost: Cost
            get() = Cost.MEMORY_ACCESS * 4
        override fun match() = this.left.value != null && this.rightLower.value != null && this.rightLower.value != null && this.left.value!! in this.rightLower.value!!..this.rightUpper.value!!
        override fun copy(): ComparisonOperator = Between(this.left.copy(), this.rightLower.copy(), this.rightUpper.copy())
        override fun digest(): Digest = this.hashCode().toLong()
        override fun bind(context: BindingContext) {
            this.left.bind(context)
            this.rightLower.bind(context)
            this.rightUpper.bind(context)
        }
        override fun toString(): String = "$left BETWEEN $rightLower, $rightUpper"
    }

    /**
     * A [ComparisonOperator] that expresses an IN comparison (i.e. left IN right).
     */
    class In(override val left: Binding, val right: List<Binding>) : ComparisonOperator {

        /** Internal [ObjectOpenHashSet] used to speed-up actual comparison.*/
        private var lookupSet: ObjectOpenHashSet<Value>? = null

        /** Cost of executing this [ComparisonOperator.In]*/
        override val cost: Cost
            get() = Cost.MEMORY_ACCESS * 2 * this.right.size

        init {
            /* Sanity check + initialization of values list. */
            require(this.right.isNotEmpty()) { "Right-hand side of IN operator cannot be empty." }
            require(this.right.all { it !is Binding.Column }) { "Right-hand side of IN operator cannot be a column reference." }
        }

        /**
         * Compares [left] to [right] for this [ComparisonOperator.In] and returns true on match and false otherwise,
         *
         * @return True on match, false otherwise.
         */
        override fun match(): Boolean {
            if (this.lookupSet == null) {
                this.lookupSet = ObjectOpenHashSet()
                for (r in this.right) {
                    if (r is Binding.Subquery) {
                        this.lookupSet!!.addAll(r.values)
                    } else {
                        this.lookupSet!!.add(r.value)
                    }
                }
            }
            return this.left.value in this.lookupSet!!
        }

        /**
         * Binds all [Binding]s contained in this [ComparisonOperator.In] to the new [BindingContext].
         *
         * @param context The new [BindingContext] to bind [Binding]s to.
         */
        override fun bind(context: BindingContext) {
            this.left.bind(context)
            this.right.forEach { it.bind(context) }
            this.lookupSet = null
        }

        override fun copy() = In(this.left.copy(), this.right.map { it.copy() }.toMutableList())
        override fun digest(): Digest = this.hashCode().toLong()
        override fun toString(): String = "$left IN [${this.right.joinToString(",")}]"
    }
}