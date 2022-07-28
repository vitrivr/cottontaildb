package org.vitrivr.cottontail.core.queries.predicates

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
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
 * @version 3.0.0
 */
sealed interface BooleanPredicate : Predicate {
    /** The [Atomic]s that make up this [BooleanPredicate]. */
    val atomics: Set<Atomic>

    /**
     * Returns true, if this [BooleanPredicate] returns true in its current configuration, and false otherwise.
     */
    context(Record, BindingContext)
    fun isMatch(): Boolean

    /**
     * Returns the matching score, if the provided [Record]. Score of 0.0 equates to a non-match, while 1.0 equates to a full match.
     */
    context(Record, BindingContext)
    fun score(): Double

    /**
     * An atomic [BooleanPredicate] that compares the column of a [Record] to a provided value, a set of provided values or another column.
     *
     * @author Ralph Gasser
     * @version 1.3.0
     */
    data class Atomic(val operator: ComparisonOperator, val not: Boolean) : BooleanPredicate {

        /** The [Cost] of evaluating this [Atomic]. */
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


        /** The [Atomic]s that make up this [BooleanPredicate]. */
        override val atomics: Set<Atomic>
            get() = setOf(this)

        /**
         * Checks if the provided [Record] matches this [Atomic] and assigns a score if so.
         *
         * @return Matching score.
         */
        context(Record, BindingContext)
        override fun score(): Double = this.isMatch().toDouble()

        /**
         * Checks if the provided [Record] matches this [Atomic] and returns true or false respectively.
         *
         * @return true if [Record] matches this [Atomic], false otherwise.
         */
        context(Record, BindingContext)
        override fun isMatch(): Boolean =
            (!this.not && this.operator.match()) || (this.not && !this.operator.match())

        /**
         * Calculates and returns the digest for this [BooleanPredicate.Atomic]
         *
         * @return [Digest]
         */
        override fun digest(): Digest = 33L * this.hashCode()

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
            if (other !is Atomic) return false
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
     * A compound [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical AND or OR connection.
     *
     * @author Ralph Gasser
     * @version 1.3.0
     */
    sealed interface Compound: BooleanPredicate {

        /** The left operand of this [Compound] boolean predicate. */
        val p1: BooleanPredicate

        /** The right operand of this [Compound] boolean predicate. */
        val p2: BooleanPredicate

        /** The [Cost] of evaluating this [BooleanPredicate]. */
        override val cost: Cost
            get() = this.p1.cost + this.p2.cost

        /** The [Atomic]s that make up this [Compound]. */
        override val atomics
            get() = this.p1.atomics + this.p2.atomics

        /** Set of [ColumnDef] that are affected by this [Compound]. */
        override val columns: Set<ColumnDef<*>>
            get() = this.p1.columns + this.p2.columns

        /**
         * A compound [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical AND or OR connection.
         *
         * @author Ralph Gasser
         * @version 1.3.0
         */
        data class And(override val p1: BooleanPredicate, override val p2: BooleanPredicate) : Compound {

            /**
             * Checks if the provided [Record] matches this [Compound] and returns true or false respectively.
             *
             * @return true if [Record] matches this [Compound], false otherwise.
             */
            context(Record, BindingContext)
            override fun isMatch(): Boolean = this.p1.isMatch() && this.p2.isMatch()

            /**
             * Checks if the provided [Record] matches this [Compound] and returns a score.
             *
             * @return true if [Record] matches this [Atomic], false otherwise.
             */
            context(Record, BindingContext)
            override fun score(): Double = (p1.isMatch() && p2.isMatch()).toDouble()

            /**
             * Calculates and returns the digest for this [BooleanPredicate.Compound]
             *
             * @return Digest as [Long]
             */
            override fun digest(): Long = 27L * this.hashCode()


            override fun toString(): String = "$p1 AND $p2"
        }

        /**
         * A compound [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical AND or OR connection.
         *
         * @author Ralph Gasser
         * @version 1.3.0
         */
        data class Or(override val p1: BooleanPredicate, override val p2: BooleanPredicate) : Compound {

            /**
             * Checks if the provided [Record] matches this [Compound] and returns true or false respectively.
             *
             * @return true if [Record] matches this [Compound], false otherwise.
             */
            context(Record, BindingContext)
            override fun isMatch(): Boolean = this.p1.isMatch() || this.p2.isMatch()
            /**
             * Checks if the provided [Record] matches this [Compound] and returns a score.
             *
             * @return true if [Record] matches this [Atomic], false otherwise.
             */
            context(Record, BindingContext)
            override fun score(): Double = ((p1.isMatch() || p2.isMatch()).toDouble() / 2.0)

            /**
             * Calculates and returns the digest for this [BooleanPredicate.Compound]
             *
             * @return Digest as [Long]
             */
            override fun digest(): Long = 31L * this.hashCode()
            override fun toString(): String = "$p1 OR $p2"
        }
    }
}