package org.vitrivr.cottontail.core.queries.predicates

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.PreparableNode
import org.vitrivr.cottontail.core.queries.nodes.StatefulNode
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.toDouble
import org.vitrivr.cottontail.core.tuple.Tuple

/**
 * A [Predicate] that can be used to match a [Tuple]s using boolean algebra.
 *
 * A [BooleanPredicate] either matches a [Tuple] or not, returning true or false respectively.
 *
 * @see Tuple
 *
 * @author Ralph Gasser
 * @version 3.1.0
 */
sealed interface BooleanPredicate : Predicate, StatefulNode, PreparableNode {
    /** The [BooleanPredicate]s that make up this [BooleanPredicate]. */
    val atomics: Set<BooleanPredicate>

    /**
     * Returns true, if this [BooleanPredicate] returns true in its current configuration, and false otherwise.
     */
    context(BindingContext, Tuple)
    fun isMatch(): Boolean

    /**
     * Returns the matching score, if the provided [Tuple]. Score of 0.0 equates to a non-match, while 1.0 equates to a full match.
     */
    context(BindingContext, Tuple)
    fun score(): Double = this.isMatch().toDouble()

    /**
     * Creates a copy of this [BooleanPredicate]
     *
     * @return Copy of this [BooleanPredicate].
     */
    override fun copy(): BooleanPredicate

    /**
     * A [Literal] [BooleanPredicate]. Can either be true or false.
     *
     * Often generated as a result of early evaluation.
     */
    data class Literal(val boolean: Boolean): BooleanPredicate {
        override val cost: Cost = Cost.ZERO
        context(BindingContext)
        override fun prepare() { /* No op */ }

        override val columns: Set<ColumnDef<*>> = emptySet()

        override val atomics: Set<Comparison> = emptySet()
        override fun isMatch(): Boolean = this.boolean
        override fun copy(): BooleanPredicate = Literal(this.boolean)
        override fun digest(): Digest = 7L * this.boolean.hashCode()
    }

    /**
     * A [IsNull] [BooleanPredicate] that evaluates if the given [Binding] is NULL.
     */
    data class IsNull(val binding: Binding): BooleanPredicate {
        override val cost: Cost = Cost.MEMORY_ACCESS
        override val atomics: Set<BooleanPredicate> = setOf(this)
        override val columns: Set<ColumnDef<*>> = ObjectOpenHashSet()
        init {
            if (this.binding is Binding.Column) {
                (this.columns as ObjectOpenHashSet).add(this.binding.column)
            }
        }
        context(BindingContext, Tuple)
        override fun isMatch() = this.binding.getValue() == null
        context(BindingContext, Tuple)
        override fun score(): Double = (this.binding.getValue() == null).toDouble()
        override fun copy() = IsNull(this.binding)
        override fun digest(): Digest = 5L * this.hashCode().toLong()
        context(BindingContext) override fun prepare() { /* No op. */ }
    }

    /**
     * An [Comparison] [BooleanPredicate] that compares two expressions using a [ComparisonOperator].
     */
    data class Comparison(val operator: ComparisonOperator) : BooleanPredicate {

        /** The [Cost] of evaluating this [Comparison]. */
        override val cost: Cost = this.operator.cost

        /** The non-reducible [BooleanPredicate]s that make up this [BooleanPredicate]. */
        override val atomics: Set<BooleanPredicate> = setOf(this)

        /** [Set] of [ColumnDef] this [Comparison] accesses. */
        override val columns: Set<ColumnDef<*>> = ObjectOpenHashSet()

        init {
            if (this.operator.left is Binding.Column) {
                (this.columns as ObjectOpenHashSet).add((this.operator.left as Binding.Column).column)
            }
            if (this.operator.right is Binding.Column) {
                (this.columns as ObjectOpenHashSet).add((this.operator.right as Binding.Column).column)
            }
        }

        /**
         * Checks if the provided [Tuple] matches this [Comparison] and returns true or false respectively.
         *
         * @return true if [Tuple] matches this [Comparison], false otherwise.
         */
        context(BindingContext, Tuple)
        override fun isMatch(): Boolean = this.operator.match()

        /**
         * Method that is being called directly before query execution starts.
         *
         * Propagated to [ComparisonOperator]
         */
        context(BindingContext)
        override fun prepare() = this.operator.prepare()

        /**
         * Calculates and returns the digest for this [BooleanPredicate.Comparison]
         *
         * @return [Digest]
         */
        override fun digest(): Digest = 33L * this.hashCode()

        /**
         * Creates a cop of this [Comparison] and returns it.
         *
         * @return Copy of this [Comparison].
         */
        override fun copy() = Comparison(this.operator.copy())

        /**
         * Generates a [String] representation of this [BooleanPredicate].
         *
         * @return [String]
         */
        override fun toString(): String  = this.operator.toString()
    }


    /**
     * A negating [Not] [BooleanPredicate].
     */
    data class Not(val p: BooleanPredicate): BooleanPredicate {
        override val atomics: Set<BooleanPredicate>
            get() = this.p.atomics
        override val columns: Set<ColumnDef<*>>
            get() = this.p.columns
        override val cost: Cost
            get() = this.p.cost

        /**
         * Checks if the provided [Tuple] matches this [Not] and returns true or false respectively.
         *
         * @return true if [Tuple] matches this [Not], false otherwise.
         */
        context(BindingContext, Tuple)
        override fun isMatch(): Boolean = !this.p.isMatch()

        /**
         * Method that is being called directly before query execution starts.
         *
         * Propagated to child [BooleanPredicate]s
         */
        context(BindingContext)
        override fun prepare() {
            this.p.prepare()
        }

        /**
         * Creates a copy of this [Not] and returns it.
         *
         * @return Copy of this [Not].
         */
        override fun copy() = Not(this.p.copy())

        override fun digest(): Digest  = 27L * this.hashCode()

        override fun toString(): String = "NOT $p"
    }

    /**
     * A compound [And] [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical AND operator.
     */
    data class And(val p1: BooleanPredicate, val p2: BooleanPredicate) : BooleanPredicate {

        /** The [Cost] of evaluating this [BooleanPredicate]. */
        override val cost: Cost
            get() = this.p1.cost + this.p2.cost

        /** The [Comparison]s that make up this [And]. */
        override val atomics: Set<BooleanPredicate>
            get() = this.p1.atomics + this.p2.atomics

        /** Set of [ColumnDef] that are affected by this [And]. */
        override val columns: Set<ColumnDef<*>>
            get() = this.p1.columns + this.p2.columns

        /**
         * Checks if the provided [Tuple] matches this [And] and returns true or false respectively.
         *
         * @return true if [Tuple] matches this [And], false otherwise.
         */
        context(BindingContext, Tuple)
        override fun isMatch(): Boolean = this.p1.isMatch() && this.p2.isMatch()

        /**
         * Method that is being called directly before query execution starts.
         *
         * Propagated to child [BooleanPredicate]s
         */
        context(BindingContext)
        override fun prepare() {
            this.p1.prepare()
            this.p2.prepare()
        }

        /**
         * Creates a copy of this [And] and returns it.
         *
         * @return Copy of this [And].
         */
        override fun copy() = And(this.p1.copy(), this.p2.copy())

        /**
         * Calculates and returns the digest for this [And]
         *
         * @return Digest as [Long]
         */
        override fun digest(): Long = 27L * this.hashCode()


        override fun toString(): String = "$p1 AND $p2"
    }

    /**
     * A compound [Or] [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical AND connection.
     */
    data class Or(val p1: BooleanPredicate, val p2: BooleanPredicate): BooleanPredicate {
        /** The [Cost] of evaluating this [BooleanPredicate]. */
        override val cost: Cost
            get() = this.p1.cost + this.p2.cost

        /** The [BooleanPredicate]s that make up this [And]. */
        override val atomics: Set<BooleanPredicate>
            get() = this.p1.atomics + this.p2.atomics

        /** Set of [ColumnDef] that are affected by this [And]. */
        override val columns: Set<ColumnDef<*>>
            get() = this.p1.columns + this.p2.columns

        /**
         * Checks if the provided [Tuple] matches this [Or] and returns true or false respectively.
         *
         * @return true if [Tuple] matches this [Or], false otherwise.
         */
        context(BindingContext, Tuple)
        override fun isMatch(): Boolean = this.p1.isMatch() || this.p2.isMatch()

        /**
         * Checks if the provided [Tuple] matches this [Or] and returns a score.
         *
         * @return true if [Tuple] matches this [Comparison], false otherwise.
         */
        context(BindingContext, Tuple)
        override fun score(): Double = ((p1.isMatch() || p2.isMatch()).toDouble() / 2.0)

        /**
         * Method that is being called directly before query execution starts.
         *
         * Propagated to child [BooleanPredicate]s
         */
        context(BindingContext)
        override fun prepare() {
            this.p1.prepare()
            this.p2.prepare()
        }

        /**
         * Creates a cop of this [And] and returns it.
         *
         * @return Copy of this [And].
         */
        override fun copy() = Or(this.p1.copy(), this.p2.copy())

        /**
         * Calculates and returns the digest for this [Or]
         *
         * @return Digest as [Long]
         */
        override fun digest(): Long = 31L * this.hashCode()
        override fun toString(): String = "$p1 OR $p2"
    }
}