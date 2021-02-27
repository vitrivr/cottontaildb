package org.vitrivr.cottontail.database.queries

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator

/**
 * [OperatorNode]s are [Node]s in a Cottontail DB query execution plan and represent flow of  information.
 *
 * [OperatorNode]s take [org.vitrivr.cottontail.model.basics.Record]s as input and transform them into
 * [org.vitrivr.cottontail.model.basics.Record] output.  The relationship of input to output can be m to n.
 *
 * [OperatorNode]s allow for about reasoning and transformation of the execution plan during query optimization
 * and are manipulated by the query planner.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
sealed class OperatorNode : Node {

    /** The arity of this [OperatorNode], i.e., the number of parents or inputs allowed. */
    abstract val inputArity: Int

    /** Internal group identifier used for plan enumeration. */
    abstract val groupId: Int

    /** Whether or not this [OperatorNode] is executable. */
    abstract val executable: Boolean

    /** The [ColumnDef]s produced by this [OperatorNode]. */
    abstract val columns: Array<ColumnDef<*>>

    /** The [ColumnDef]s by which the output of this [OperatorNode] is sorted. By default, there is no particular order. */
    abstract val order: Array<Pair<ColumnDef<*>, SortOrder>>

    /** List of [ColumnDef]s required by this [OperatorNode] in order to be able to function. */
    abstract val requires: Array<ColumnDef<*>>

    /**
     * A logical [OperatorNode] in the Cottontail DB query execution plan.
     *
     * [OperatorNode.Logical]s are purely abstract and cannot be executed directly. They belong to the
     * first phase of the query optimization process, in which the canonical input [OperatorNode.Logical]
     * are transformed into equivalent representations of [OperatorNode.Logical]s.
     *
     * @author Ralph Gasser
     * @version 2.0.0
     */
    abstract class Logical : OperatorNode() {

        /** The [OperatorNode.Logical] that receives the results produced by this [OperatorNode.Logical] as input. May be null, which makes this [OperatorNode.Logical] the root of the tree. */
        var output: OperatorNode.Logical? = null

        /** The root of this [OperatorNode.Logical], i.e., the final [ OperatorNode.Logical] in terms of operation that usually produces the output. */
        val root: OperatorNode.Logical
            get() = this.output?.root ?: this

        /** The base of this [OperatorNode], i.e., the starting point(s) in terms of operation. Depending on the tree structure, multiple bases may exist. */
        abstract val base: Collection<OperatorNode.Logical>

        /** [OperatorNode.Logical]s are never executable. */
        override val executable: Boolean = false

        /**
         * Creates and returns a copy of this [OperatorNode.Logical] without any children or parents.
         *
         * @return Copy of this [OperatorNode.Logical].
         */
        abstract fun copyWithInputs(): Logical

        /**
         * Creates and returns a copy of this [OperatorNode.Logical] with its output reaching down to the [root] of the tree.
         * Furthermore connects the provided [inputs] to the copied [OperatorNode.Logical]s.
         *
         * @param inputs The [Logical]s that act as input. The number of input [OperatorNode.Logical]s must correspond to the inputArity of this [OperatorNode.Logical].
         * @return Copy of this [OperatorNode.Logical] with its output.
         */
        abstract fun copyWithOutput(vararg inputs: Logical): Logical

        /**
         * Creates and returns an implementation of this [OperatorNode.Logical]
         *
         * @return [OperatorNode.Physical] representing this [OperatorNode.Logical].
         */
        abstract fun implement(): Physical
    }

    /**
     * A physical [OperatorNode] in the Cottontail DB query execution plan.
     *
     * [OperatorNode.Physical]s are a direct proxy to a corresponding [Operator]. They belong to the
     * second phase of the query optimization process, in which [OperatorNode.Logical]s are replaced
     * by [OperatorNode.Physical]s so as to generate an executable plan.
     *
     * As opposed to [OperatorNode.Logical]s, [OperatorNode.Physical]s are associated concrete implementations
     * and a cost model that allows  the query planner to select the optimal plan.
     *
     * @author Ralph Gasser
     * @version 2.0.0
     *
     * @see OperatorNode
     */
    abstract class Physical : OperatorNode() {

        /** The [OperatorNode.Logical] that receives the results produced by this [OperatorNode.Logical] as input. May be null, which makes this [OperatorNode.Logical] the root of the tree. */
        var output: OperatorNode.Physical? = null

        /** The root of this [OperatorNode.Physical], i.e., the final [OperatorNode.Physical] in terms of operation that usually produces the output. */
        val root: OperatorNode.Physical
            get() = this.output?.root ?: this

        /** The base of this [OperatorNode], i.e., the starting point(s) in terms of operation. Depending on the tree structure, multiple bases may exist. */
        abstract val base: Collection<OperatorNode.Physical>

        /** [RecordStatistics] about the [ColumnDef]s contained in this [OperatorNode.Physical]. */
        abstract val statistics: RecordStatistics

        /** The estimated number of rows this [OperatorNode.Physical] generates. */
        abstract val outputSize: Long

        /** An estimation of the [Cost] incurred by this [OperatorNode.Physical]. */
        abstract val cost: Cost

        /** An estimation of the [Cost] incurred by the tree up and until this [OperatorNode.Physical]. */
        abstract val totalCost: Cost

        /** Most [OperatorNode.Physical]s are executable by default. */
        override val executable: Boolean = true

        /** True, if this [OperatorNode.Physical] can be partitioned, false otherwise. */
        abstract val canBePartitioned: Boolean

        /**
         * Creates and returns a copy of this [OperatorNode.Physical] without any children or parents.
         *
         * @return Copy of this [OperatorNode.Physical].
         */
        abstract fun copyWithInputs(): Physical

        /**
         * Creates and returns a copy of this [OperatorNode.Physical] with its output reaching down to the [root] of the tree.
         * Furthermore connects the provided [inputs] to the copied [OperatorNode.Physical]s.
         *
         * @param inputs The [Physical]s that act as input. The number of input [OperatorNode.Physical]s must correspond to the inputArity of this [OperatorNode.Physical].
         * @return Copy of this [OperatorNode.Physical] with its output.
         */
        abstract fun copyWithOutput(vararg inputs: Physical): Physical

        /**
         * Converts this [OperatorNode.Physical] to the corresponding [Operator].
         *
         * @param tx The [TransactionContext] the [Operator] should be executed in.
         * @param ctx: The [QueryContext] used for conversion. Mainly for value binding.
         *
         * @return [Operator]
         */
        abstract fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator

        /**
         * Tries to create [p] partitions of this [OperatorNode.Physical] if possible. If the implementing
         * [OperatorNode.Physical] returns true for [canBePartitioned], then this method is expected
         * to return a result, even if the number of partitions returned my be lower than [p] or even one (which
         * means that no partitioning took place).
         *
         * If [canBePartitioned] returns false, this method is expected to throw a [IllegalStateException].
         *
         * @param p The desired number of partitions.
         * @return Array of [OperatorNode.Physical]s.
         *
         * @throws IllegalStateException If this [OperatorNode.Physical] cannot be partitioned.
         */
        abstract fun partition(p: Int): List<Physical>
    }
}