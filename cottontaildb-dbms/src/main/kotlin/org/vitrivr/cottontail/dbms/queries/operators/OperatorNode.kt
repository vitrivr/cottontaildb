package org.vitrivr.cottontail.dbms.queries.operators

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.nodes.Node
import org.vitrivr.cottontail.core.queries.nodes.NodeWithCost
import org.vitrivr.cottontail.core.queries.nodes.NodeWithTrait
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import java.io.PrintStream

/**
 * [OperatorNode]s are [Node]s in a Cottontail DB query execution plan and represent flow and processing of information a query gets executed.
 *
 * Conceptually, [OperatorNode]s take [Record]s as input and transform them into [Record] output. The relationship of input to output can be m to n.
 *
 * [OperatorNode]s allow for reasoning and transformation of the execution plan during query optimization and are manipulated by the query planner.
 *
 * @author Ralph Gasser
 * @version 2.5.0
 */
sealed class OperatorNode : NodeWithTrait {
    /** The arity of this [OperatorNode], i.e., the number of parents or inputs allowed. */
    abstract val inputArity: Int

    /** Internal group identifier used for plan enumeration. Can be null for disconnected [OperatorNode]s. */
    abstract val groupId: GroupId

    /** Internal [depth] index of this [OperatorNode] in the [OperatorNode] tree. Counting starts from [root], which is 0. */
    abstract val depth: Int

    /** The name of this [OperatorNode]. */
    abstract val name: String

    /** Whether this [OperatorNode] is executable. */
    abstract val executable: Boolean

    /** The physical [ColumnDef]s accessed by this [OperatorNode]. */
    abstract val physicalColumns: List<ColumnDef<*>>

    /** The [ColumnDef]s produced by this [OperatorNode]. */
    abstract val columns: List<ColumnDef<*>>

    /** The [ColumnDef]s required by this [OperatorNode] in order to be able to function. */
    abstract val requires: List<ColumnDef<*>>

    /**
     * Creates and returns a copy of this [OperatorNode] without any children or parents.
     *
     * @return Copy of this [OperatorNode].
     */
    abstract override fun copy(): OperatorNode

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    open fun printTo(p: PrintStream = System.out) {
        repeat(this.groupId) { p.print("|\t") }
        p.println(this.toString())
        repeat(this.groupId) { p.print("|\t") }
        p.print("|\n")
    }

    /** Generates and returns a [String] representation of this [OperatorNode]. */
    override fun toString() = "${this.groupId}:${this.name}"

    /**
     * A logical [OperatorNode] in the Cottontail DB query execution plan.
     *
     * [OperatorNode.Logical]s are purely abstract and cannot be executed directly. They belong to the
     * first phase of the query optimization process, in which the canonical input [OperatorNode.Logical]
     * are transformed into equivalent representations of [OperatorNode.Logical]s.
     *
     * @author Ralph Gasser
     * @version 2.5.0
     */
    abstract class Logical : OperatorNode() {

        /** The [OperatorNode.Logical] that receives the results produced by this [OperatorNode.Logical] as input. May be null, which makes this [OperatorNode.Logical] the root of the tree. */
        var output: Logical? = null

        /** The root of this [OperatorNode.Logical], i.e., the final [OperatorNode.Logical] in terms of operation that usually produces the output. */
        val root: Logical
            get() = this.output?.root ?: this

        /** The base of this [OperatorNode], i.e., the starting point(s) in terms of operation. Depending on the tree structure, multiple bases may exist. */
        abstract val base: Collection<Logical>

        /** [OperatorNode.Logical]s are never executable. */
        override val executable: Boolean = false

        /**
         * Creates and returns a copy of this [OperatorNode.Logical] without any children or parents.
         *
         * @return Copy of this [OperatorNode.Logical].
         */
        abstract override fun copy(): Logical

        /**
         * Creates and returns a copy of this [OperatorNode.Logical] and all its inputs that belong to the same [GroupId],
         * up and until the base of the tree.
         *
         * @return Copy of this [OperatorNode.Logical].
         */
        abstract fun copyWithGroupInputs(): Logical

        /**
         * Creates and returns a copy of this [OperatorNode.Logical] and all its inputs up and until the base of the tree,
         * regardless of which [GroupId] they belong to.
         *
         * @return Copy of this [OperatorNode.Logical].
         */
        abstract fun copyWithInputs(): Logical

        /**
         * Creates and returns a copy of this [OperatorNode.Logical] with its output reaching down to the [root] of the tree.
         * Furthermore, connects the provided [input] to the copied [OperatorNode.Logical]s.
         *
         * @param input The [Logical]s that act as input.
         * @return Copy of this [OperatorNode.Logical] with its output.
         */
        abstract fun copyWithOutput(vararg input: Logical): Logical

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
     * by [OperatorNode.Physical]s to generate an executable plan.
     *
     * As opposed to [OperatorNode.Logical]s, [OperatorNode.Physical]s are associated concrete implementations
     * and a cost model that allows the query planner to select the optimal plan.
     *
     * @author Ralph Gasser
     * @version 2.5.0
     *
     * @see OperatorNode
     */
    abstract class Physical : OperatorNode(), NodeWithCost {

        /** The [OperatorNode.Logical] that receives the results produced by this [OperatorNode.Logical] as input. May be null, which makes this [OperatorNode.Logical] the root of the tree. */
        var output: Physical? = null

        /** The root of this [OperatorNode.Physical], i.e., the final [OperatorNode.Physical] in terms of operation that usually produces the output. */
        val root: Physical
            get() = this.output?.root ?: this

        /** The base of this [OperatorNode], i.e., the starting point(s) in terms of operation. Depending on the tree structure, multiple bases may exist. */
        abstract val base: Collection<Physical>

        /** Map containing all [ValueStatistics] about the [ColumnDef]s processed in this [OperatorNode.Physical]. */
        abstract val statistics: Map<ColumnDef<*>, ValueStatistics<*>>

        /** The estimated number of rows this [OperatorNode.Physical] generates. */
        abstract val outputSize: Long

        /** An estimation of the [Cost] incurred by the query plan up and until this [OperatorNode.Physical]. */
        abstract val totalCost: Cost

        /** An estimation of the [Cost] incurred by the parallelizable portion of the query plan up and until this [OperatorNode.Physical]. */
        abstract val parallelizableCost: Cost

        /** An estimation of the [Cost] incurred by the sequential portion of the query plan up and until this [OperatorNode.Physical]. */
        val sequentialCost: Cost
            get() = this.totalCost - this.parallelizableCost

        /** Most [OperatorNode.Physical]s are executable by default. */
        override val executable: Boolean = true

        /** True, if this [OperatorNode.Physical] can be partitioned, false otherwise. */
        abstract val canBePartitioned: Boolean

        /**
         * Creates and returns a copy of this [OperatorNode.Physical] with all its inputs reaching up to the [base] of the tree.
         *
         * @return Copy of this [OperatorNode.Physical].
         */
        abstract fun copyWithInputs(): Physical

        /**
         * Creates and returns a copy of this [OperatorNode.Physical] and all its inputs that belong to the same [GroupId],
         * up and until the base of the tree.
         *
         * @return Copy of this [OperatorNode.Physical].
         */
        abstract fun copyWithGroupInputs(): Physical

        /**
         * Creates and returns a copy of this [OperatorNode.Physical] with all its outputs reaching down to the [root] of the tree.
         * Furthermore, connects the provided [input] to the copied [OperatorNode.Physical]s.
         *
         * @param input The [Physical]s that act as input. Replacement takes place based on the [GroupId]
         * @return Copy of this [OperatorNode.Physical] with its output.
         */
        abstract fun copyWithOutput(vararg input: Physical): Physical

        /**
         * Converts this [OperatorNode.Physical] to the corresponding [Operator].
         *
         * @param ctx: The [QueryContext] used for conversion.
         *
         * @return [Operator]
         */
        abstract fun toOperator(ctx: QueryContext): Operator

        /**
         * Tries to create a partitioned version of this [OperatorNode.Physical].
         *
         * By default, a call to this method propagates up a tree until a [OperatorNode.Physical] that allows for
         * partitioning (see [canBePartitioned]) or the end of the tree has been reached. In the former case,
         * this method return a partitioned copy of this [OperatorNode.Physical].
         *
         * @param partitions The number of partitions.
         * @return Partitioned version of this [OperatorNode.Physical] and its parents or null, if partitioning wasn't possible.
         */
        abstract fun tryPartition(partitions: Int): Physical?

        /**
         * Generates a partitioned version of this [OperatorNode.Physical].
         *
         * Not to be confused with [tryPartition].
         *
         * @param partitions The total number of partitions.
         * @param p The partition number.
         * @return [OperatorNode.Physical]
         */
        abstract fun partition(partitions: Int, p: Int): Physical
    }
}