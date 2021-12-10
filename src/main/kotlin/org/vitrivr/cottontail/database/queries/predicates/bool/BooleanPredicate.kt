package org.vitrivr.cottontail.database.queries.predicates.bool

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.utilities.extensions.toDouble

/**
 * A [Predicate] that can be used to match a [Record]s using boolean [ComparisonOperator]s and [ConnectionOperator]s.
 *
 * A [BooleanPredicate] either matches a [Record] or not, returning true or false respectively.
 * All types of [BooleanPredicate] are constructed using conjunctive normal form (CNF).
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
sealed class BooleanPredicate : Predicate {
    /** The [Atomic]s that make up this [BooleanPredicate]. */
    abstract val atomics: Set<Atomic>

    /**
     * Returns true, if this [BooleanPredicate] returns true in its current configuration, and false otherwise.
     */
    abstract fun isMatch(): Boolean

    /**
     * Returns the matching score, if the provided [Record]. Score of 0.0 equates to a non-match,
     * while 1.0 equates to a full match.
     *
     * @param record The [Record] that should be checked against the predicate.
     */
    abstract fun score(record: Record): Double

    /**
     * An atomic [BooleanPredicate] that compares the column of a [Record] to a provided value, a set of provided values or another column.
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    class Atomic(val operator: ComparisonOperator, val not: Boolean, val dependsOn: GroupId = 0) : BooleanPredicate() {

        /** The number of operations required by this [Atomic]. */
        override val atomicCpuCost: Float
            get() = this.operator.atomicCpuCost

        /** */
        override val columns = ObjectOpenHashSet<ColumnDef<*>>()

        /** The [Atomic]s that make up this [BooleanPredicate]. */
        override val atomics: Set<Atomic>
            get() = setOf(this)


        init {
            when (this.operator) {
                is ComparisonOperator.Binary -> {
                    if (this.operator.left is Binding.Column) {
                        this.columns.add(this.operator.left.column)
                    }
                    if (this.operator.right is Binding.Column) {
                        this.columns.add(this.operator.right.column)
                    }
                }
                is ComparisonOperator.Between -> {
                    if (this.operator.left is Binding.Column) {
                        this.columns.add(this.operator.left.column)
                    }
                    if (this.operator.rightLower is Binding.Column) {
                        this.columns.add(this.operator.rightLower.column)
                    }
                    if (this.operator.rightUpper is Binding.Column) {
                        this.columns.add(this.operator.rightUpper.column)
                    }
                }
                is ComparisonOperator.In, -> {
                    if (this.operator.left is Binding.Column) {
                        this.columns.add(this.operator.left.column)
                    }
                }
                is ComparisonOperator.IsNull -> {
                    if (this.operator.left is Binding.Column) {
                        this.columns.add(this.operator.left.column)
                    }
                }
            }
        }

        /**
         * Checks if the provided [Record] matches this [Atomic] and assigns a score if so.
         *
         * @param record The [Record] to check.
         * @return Matching score.
         */
        override fun score(record: Record): Double = this.isMatch().toDouble()

        /**
         * Calculates and returns the digest for this [BooleanPredicate.Atomic]
         *
         * @return Digest as [Long]
         */
        override fun digest(): Long = 33L * this.hashCode()

        /**
         * Checks if the provided [Record] matches this [Atomic] and returns true or false respectively.
         *
         * @return true if [Record] matches this [Atomic], false otherwise.
         */
        override fun isMatch(): Boolean =
            (!this.not && this.operator.match()) || (this.not && !this.operator.match())

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
     * A compound [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical
     * AND or OR connection.
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    class Compound(val connector: ConnectionOperator, val p1: BooleanPredicate, val p2: BooleanPredicate) : BooleanPredicate() {

        /** The total number of operations required by this [Compound]. */
        override val atomicCpuCost
            get() = this.p1.atomicCpuCost + this.p2.atomicCpuCost

        /** The [Atomic]s that make up this [Compound]. */
        override val atomics
            get() = this.p1.atomics + this.p2.atomics

        /** Set of [ColumnDef] that are affected by this [Compound]. */
        override val columns: Set<ColumnDef<*>>
            get() = this.p1.columns + this.p2.columns

        /**
         * Checks if the provided [Record] matches this [Compound] and returns true or false respectively.
         *
         * @return true if [Record] matches this [Compound], false otherwise.
         */
        override fun isMatch(): Boolean = when (connector) {
            ConnectionOperator.AND -> this.p1.isMatch() && this.p2.isMatch()
            ConnectionOperator.OR -> this.p1.isMatch() || this.p2.isMatch()
        }

        /**
         * Checks if the provided [Record] matches this [Compound] and returns a score.
         *
         * @param record The [Record] to check.
         * @return true if [Record] matches this [Atomic], false otherwise.
         */
        override fun score(record: Record): Double = when {
            this.connector == ConnectionOperator.AND && p1.isMatch() && p2.isMatch() -> 1.0
            this.connector == ConnectionOperator.OR -> ((p1.score(record) + p2.score(record)) / 2.0)
            else -> 0.0
        }

        override fun toString(): String = "$p1 $connector $p2"

        /**
         * Calculates and returns the digest for this [BooleanPredicate.Compound]
         *
         * @return Digest as [Long]
         */
        override fun digest(): Long = 33L * this.hashCode()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Compound) return false

            if (connector != other.connector) return false
            if (p1 != other.p1) return false
            if (p2 != other.p2) return false

            return true
        }

        override fun hashCode(): Int {
            var result = connector.hashCode()
            result = 31 * result + p1.hashCode()
            result = 31 * result + p2.hashCode()
            return result
        }
    }
}