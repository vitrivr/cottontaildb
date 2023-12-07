package org.vitrivr.cottontail.core.queries.predicates

import it.unimi.dsi.fastutil.objects.ObjectRBTreeSet
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.nodes.NodeWithCost
import org.vitrivr.cottontail.core.queries.nodes.PreparableNode
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.generators.defaultValue
import org.vitrivr.cottontail.core.values.pattern.LikePatternValue

/**
 * A [ComparisonOperator] is used as part of a [BooleanPredicate] to
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
sealed interface ComparisonOperator: NodeWithCost, PreparableNode {
    /** The [Binding] that acts as left operand for this [ComparisonOperator]. */
    val left: Binding

    /** The [Binding] that acts as right operand for this [ComparisonOperator]. */
    val right: Binding

    /**
     * Matches the given [Value] to this [ComparisonOperator] and returns true,
     * if there is a match (i.e. value satisfies operation) and false otherwise.
     *
     * @return True on match, false otherwise.
     */
    context(BindingContext, Tuple)
    fun match(): Boolean

    /**
     * Default implementation of prepare does nothing.
     */
    context(BindingContext)
    override fun prepare() {
        /* No op. */
    }

    /**
     * Creates a cop of this [ComparisonOperator] and returns it.
     *
     * @return Copy of this [ComparisonOperator].
     */
    fun copy(): ComparisonOperator

    /** The cost of evaluating a [Binary]. */
    override val cost: Cost
        get() = Cost.MEMORY_ACCESS * 5

    data class Equal(override val left: Binding, override val right: Binding): ComparisonOperator {
        context(BindingContext, Tuple)
        override fun match() : Boolean {
            val left = this.left.getValue()
            val right = this.right.getValue()
            return left != null && right != null && left == right
        }
        override fun toString(): String = "$left = $right"
        override fun copy() = Equal(this.left, this.right)
        override fun digest(): Digest = this.hashCode().toLong()
    }

    data class NotEqual(override val left: Binding, override val right: Binding): ComparisonOperator {
        context(BindingContext, Tuple)
        override fun match() : Boolean {
            val left = this.left.getValue()
            val right = this.right.getValue()
            return left != null && right != null && left != right
        }
        override fun toString(): String = "$left != $right"
        override fun copy() = NotEqual(this.left, this.right)
        override fun digest(): Digest = this.hashCode().toLong()
    }

    /**
     * A [ComparisonOperator] that expresses greater (>) comparison.
     */
    data class Greater(override val left: Binding, override val right: Binding): ComparisonOperator {
        context(BindingContext, Tuple)
        override fun match(): Boolean {
            val left = this.left.getValue()
            val right = this.right.getValue()
            return left != null && right != null && left > right
        }
        override fun toString(): String = "$left > $right"
        override fun copy() = Greater(this.left, this.right)
        override fun digest(): Digest = this.hashCode().toLong()
    }

    /**
     * A [ComparisonOperator] that expresses less (<) comparison.
     */
    data class Less(override val left: Binding, override val right: Binding) : ComparisonOperator {
        context(BindingContext, Tuple)
        override fun match() : Boolean {
            val left = this.left.getValue()
            val right = this.right.getValue()
            return left != null && right != null && left < right
        }
        override fun toString(): String = "$left < $right"
        override fun copy() = Greater(this.left, this.right)
        override fun digest(): Digest = this.hashCode().toLong()
    }

    /**
     * A [ComparisonOperator] that expresses greater or equal (>=) comparison.
     */
    data class GreaterEqual(override val left: Binding, override val right: Binding) : ComparisonOperator {
        context(BindingContext, Tuple)
        override fun match(): Boolean {
            val left = this.left.getValue()
            val right = this.right.getValue()
            return left != null && right != null && left >= right
        }
        override fun toString(): String = "$left >= $right"
        override fun copy() = GreaterEqual(this.left, this.right)
        override fun digest(): Digest = this.hashCode().toLong()
    }

    /**
     * A [ComparisonOperator] that expresses less or equal (<=) comparison.
     */
    data class LessEqual(override val left: Binding, override val right: Binding) : ComparisonOperator {
        context(BindingContext, Tuple)
        override fun match(): Boolean {
            val left = this.left.getValue()
            val right = this.right.getValue()
            return left != null && right != null && left <= right
        }
        override fun copy() = LessEqual(this.left, this.right)
        override fun toString(): String = "$left <= $right"
        override fun digest(): Digest = this.hashCode().toLong()
    }

    /**
     * A [ComparisonOperator] that expresses a LIKE comparison, i.e., left LIKE right.
     */
    class Like(override val left: Binding, override val right: Binding) : ComparisonOperator {
        context(BindingContext, Tuple)
        override fun match() = this.left.getValue() is StringValue && this.right.getValue() is LikePatternValue && (this.right.getValue() as LikePatternValue).matches(this.left.getValue() as StringValue)
        override fun copy() = Like(this.left, this.right)
        override fun toString(): String = "$left LIKE $right"
        override fun digest(): Digest = this.hashCode().toLong()
    }

    /**
     * A [ComparisonOperator] that expresses a MATCH comparison. Can only be evaluated through a lucene index.
     */
    class Match(override val left: Binding, override val right: Binding) : ComparisonOperator {
        context(BindingContext, Tuple)
        override fun match() = throw UnsupportedOperationException("A MATCH comparison operator cannot be evaluated directly.")
        override fun copy() = LessEqual(this.left, this.right)
        override fun toString(): String = "$left MATCH $right"
        override fun digest(): Digest = this.hashCode().toLong()
    }

    /**
     * A [ComparisonOperator] that expresses a BETWEEN comparison (i.e. lower <= left <= upper).
     */
    data class Between(override val left: Binding, override val right: Binding) : ComparisonOperator {
        init {
            /* Sanity check + initialization of values list. */
            require(this.right is Binding.LiteralList && this.right.size() == 2L) {
                "Right-hand side of BETWEEN operator must be a literal list with two entries."
            }
        }

        /** The lower value of this [Between]. Since only literals can be used here, this can be generated during prepare(). */
        private var lower: Value = this.right.type.defaultValue()

        /** The upper value of this [Between]. Since only literals can be used here, this can be generated during prepare(). */
        private var upper: Value = this.right.type.defaultValue()

        override val cost: Cost
            get() = Cost.MEMORY_ACCESS * 4

        context(BindingContext, Tuple)
        override fun match(): Boolean {
            val left = this.left.getValue()
            return left != null && left >= this.lower && left <= this.upper
        }
        context(BindingContext)
        override fun prepare() {
            with(MissingTuple) {
                this@Between.lower = this@Between.right.getValues().getOrNull(0) ?: throw IllegalStateException("BETWEEN operator expects two non-null, literal values as right operands. This is a programmer's error!")
                this@Between.upper = this@Between.right.getValues().getOrNull(1) ?: throw IllegalStateException("BETWEEN operator expects two non-null, literal values as right operands. This is a programmer's error!")
                if (this@Between.lower > this@Between.upper) { /* Normalize order for literal bindings. */
                    val cache = this@Between.upper
                    this@Between.upper = this@Between.lower
                    this@Between.lower = cache
                }
            }
        }

        override fun copy() = Between(this.left, this.right)
        override fun digest(): Digest = this.hashCode().toLong()
        override fun toString(): String = "$left BETWEEN ${this.right}"
    }

    /**
     * A [ComparisonOperator] that expresses an IN comparison (i.e. left IN right).
     */
    class In(override val left: Binding, override val right: Binding) : ComparisonOperator {

        /** Cost of executing this [ComparisonOperator.In]*/
        override val cost: Cost
            get() = Cost.MEMORY_ACCESS * 2 * this.right.size()

        /** Internal set to facilitate lookup. */
        private val lookup = ObjectRBTreeSet<Value>()

        init {
            /* Sanity check + initialization of values list. */
            require(this.right is Binding.Literal || this.right is Binding.LiteralList || this.right is Binding.Subquery) {
                "Right-hand side of IN operator must be a literal or a sub-query."
            }
        }

        /**
         * Compares [left] to [right] for this [ComparisonOperator.In] and returns true on match and false otherwise,
         *
         * @return True on match, false otherwise.
         */
        context(BindingContext, Tuple)
        override fun match(): Boolean = this.left.getValue() in this.lookup

        /**
         * Prepares this [ComparisonOperator] for query execution by reading all the right-side values into the collection.
         */
        context(BindingContext)
        override fun prepare() {
            with (MissingTuple) {
                this@In.lookup.addAll(this@In.right.getValues())
            }
        }
        override fun copy() = In(this.left, this.right)
        override fun digest(): Digest = this.hashCode().toLong()
        override fun toString(): String = "$left IN ${this.right}"
    }
}