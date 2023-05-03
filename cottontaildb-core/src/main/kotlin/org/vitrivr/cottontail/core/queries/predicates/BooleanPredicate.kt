package org.vitrivr.cottontail.core.queries.predicates

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.PreparableNode
import org.vitrivr.cottontail.core.queries.nodes.StatefulNode
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.utilities.extensions.toDouble

/**
 * A [Predicate] that can be used to match a [Record]s using boolean [ComparisonOperator]s and boolean algebra.
 *
 * A [BooleanPredicate] either matches a [Record] or not, returning true or false respectively.
 * All types of [BooleanPredicate] are constructed using conjunctive normal form (CNF).
 *
 * @see Record
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
    context(BindingContext,Record)
    fun isMatch(): Boolean

    /**
     * Returns the matching score, if the provided [Record]. Score of 0.0 equates to a non-match, while 1.0 equates to a full match.
     */
    context(BindingContext,Record)
    fun score(): Double

    /**
     * Creates a copy of this [BooleanPredicate]
     *
     * @return Copy of this [BooleanPredicate].
     */
    override fun copy(): BooleanPredicate

    /**
     *
     */
    data class Literal(val boolean: Boolean): BooleanPredicate {
        override val cost: Cost = Cost.ZERO
        context(BindingContext)
        override fun prepare() { /* No op */ }

        override val columns: Set<ColumnDef<*>> = emptySet()

        override val atomics: Set<Comparison> = emptySet()
        override fun isMatch(): Boolean = this.boolean
        override fun score(): Double = this.boolean.toDouble()
        override fun copy(): BooleanPredicate = Literal(this.boolean)
        override fun digest(): Digest = 7L * this.boolean.hashCode()
    }

    /**
     * An atomic [BooleanPredicate] that compares the column of a [Record] to a provided value, a set of provided values or another column.
     *
     * @author Ralph Gasser
     * @version 1.3.0
     */
    data class Comparison(val operator: ComparisonOperator, val not: Boolean) : BooleanPredicate {

        /** The [Cost] of evaluating this [Comparison]. */
        override val cost: Cost
            get() = this.operator.cost

        override val columns: Set<ColumnDef<*>>
            get() {
                val set = ObjectOpenHashSet<ColumnDef<*>>()
                when(this.operator) {
                    is ComparisonOperator.Binary -> {
                        if (this.operator.left is Binding.Column) {
                            set.add((this.operator.left as Binding.Column).column)
                        }
                        if (this.operator.right is Binding.Column) {
                            set.add((this.operator.right as Binding.Column).column)
                        }
                    }
                    is ComparisonOperator.Between -> {
                        if (this.operator.left is Binding.Column) {
                            set.add((this.operator.left as Binding.Column).column)
                        }
                        if (this.operator.rightLower is Binding.Column) {
                            set.add((this.operator.rightLower).column)
                        }
                        if (this.operator.rightUpper is Binding.Column) {
                            set.add((this.operator.rightUpper).column)
                        }
                    }
                    is ComparisonOperator.In,
                    is ComparisonOperator.IsNull -> {
                        val binding = this.operator.left
                        if (binding is Binding.Column) {
                            set.add(binding.column)
                        }
                    }
                }
                return set
            }


        /** The [Comparison]s that make up this [BooleanPredicate]. */
        override val atomics: Set<Comparison>
            get() = setOf(this)

        /**
         * Checks if the provided [Record] matches this [Comparison] and assigns a score if so.
         *
         * @return Matching score.
         */
        context(BindingContext,Record)
        override fun score(): Double = this.isMatch().toDouble()

        /**
         * Checks if the provided [Record] matches this [Comparison] and returns true or false respectively.
         *
         * @return true if [Record] matches this [Comparison], false otherwise.
         */
        context(BindingContext,Record)
        override fun isMatch(): Boolean =
            (!this.not && this.operator.match()) || (this.not && !this.operator.match())

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
        override fun copy() = Comparison(this.operator.copy(), this.not)

        /**
         * Generates a [String] representation of this [BooleanPredicate].
         *
         * @return [String]
         */
        override fun toString(): String {
            val builder = StringBuilder()
            if (this.not) builder.append("!(")
            builder.append(this.operator.toString())
            if (this.not) builder.append(")")
            return builder.toString()
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Comparison) return false
            if (this.operator != other.operator) return false
            if (this.not != other.not) return false

            return true
        }

        override fun hashCode(): Int {
            var result = this. operator.hashCode()
            result = 31 * result + not.hashCode()
            return result
        }
    }


    /**
     *
     */
    data class Not(val p: BooleanPredicate): BooleanPredicate {
        override val atomics: Set<BooleanPredicate>
            get() = this.p.atomics
        override val columns: Set<ColumnDef<*>>
            get() = this.p.columns
        override val cost: Cost
            get() = this.p.cost


        /**
         * Checks if the provided [Record] matches this [Not] and returns true or false respectively.
         *
         * @return true if [Record] matches this [Not], false otherwise.
         */
        context(BindingContext, Record)
        override fun isMatch(): Boolean = !this.p.isMatch()

        /**
         * Checks if the provided [Record] matches this [Not] and returns a score.
         *
         * @return true if [Record] matches this [Not], false otherwise.
         */
        context(BindingContext, Record)
        override fun score(): Double = (!this.p.isMatch()).toDouble()

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
     * A compound [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical OR connection.
     *
     * @author Ralph Gasser
     * @version 1.3.0
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
         * Checks if the provided [Record] matches this [And] and returns true or false respectively.
         *
         * @return true if [Record] matches this [And], false otherwise.
         */
        context(BindingContext,Record)
        override fun isMatch(): Boolean = this.p1.isMatch() && this.p2.isMatch()

        /**
         * Checks if the provided [Record] matches this [And] and returns a score.
         *
         * @return true if [Record] matches this [Comparison], false otherwise.
         */
        context(BindingContext,Record)
        override fun score(): Double = (p1.isMatch() && p2.isMatch()).toDouble()

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
     * A compound [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical AND connection.
     *
     * @author Ralph Gasser
     * @version 1.3.0
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
         * Checks if the provided [Record] matches this [Or] and returns true or false respectively.
         *
         * @return true if [Record] matches this [Or], false otherwise.
         */
        context(BindingContext,Record)
        override fun isMatch(): Boolean = this.p1.isMatch() || this.p2.isMatch()
        /**
         * Checks if the provided [Record] matches this [Or] and returns a score.
         *
         * @return true if [Record] matches this [Comparison], false otherwise.
         */
        context(BindingContext,Record)
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