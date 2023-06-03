package org.vitrivr.cottontail.dbms.queries.operators.basics

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.Node
import org.vitrivr.cottontail.core.queries.nodes.NodeWithTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics
import java.io.PrintStream

/**
 * [OperatorNode]s are [Node]s in a Cottontail DB query execution plan and represent flow and processing of information a query gets executed.
 *
 * Conceptually, [OperatorNode]s take [org.vitrivr.cottontail.core.basics.Tuple]s as input and transform them into [org.vitrivr.cottontail.core.basics.Tuple] output. The relationship of input to output can be m to n.
 *
 * [OperatorNode]s allow for reasoning and transformation of the execution plan during query optimization and are manipulated by the query planner.
 *
 * @author Ralph Gasser
 * @version 2.8.0
 */
sealed class OperatorNode : NodeWithTrait {
    /** The arity of this [OperatorNode], i.e., the number of parents or inputs allowed. */
    abstract val inputArity: Int

    /** Internal group identifier used for plan enumeration. Can be null for disconnected [OperatorNode]s. */
    abstract val groupId: GroupId

    /** Array of [GroupId]s this [OperatorNode] depends on. */
    abstract val dependsOn: Array<GroupId>

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
     * Calculates and returns the total [Digest] for this [OperatorNode], which is used for caching and re-use of query plans
     *
     * The total [Digest] is a combination of this [OperatorNode]'s [Digest] and all upstream [Digest] values.
     *
     * @return The total [Digest] for this [OperatorNode]
     */
    abstract fun totalDigest(): Digest

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
     */
    sealed class Logical : OperatorNode() {

        /** The [OperatorNode.Logical] that receives the results produced by this [OperatorNode.Logical] as input. May be null, which makes this [OperatorNode.Logical] the root of the tree. */
        var output: Logical? = null
            internal set

        /** The root of this [OperatorNode.Logical], i.e., the final [OperatorNode.Logical] in terms of operation that usually produces the output. */
        val root: Logical
            get() = this.output?.root ?: this

        /** The base of this [OperatorNode], i.e., the starting point(s) in terms of operation. Depending on the tree structure, multiple bases may exist. */
        abstract val base: Collection<Logical>

        /** [OperatorNode.Logical]s are never executable. */
        override val executable: Boolean = false

        /**
         * Creates and returns a copy of this [OperatorNode.Logical] using the given parents as input.
         *
         * @return Copy of this [OperatorNode.Logical].
         */
        abstract fun copyWithNewInput(vararg input: Logical): Logical

        /**
         * Creates and returns a copy of this [OperatorNode.Logical] and all its inputs up and until the base of the tree,
         * regardless of which [GroupId] they belong to.
         *
         * @return Copy of this [OperatorNode.Logical].
         */
        abstract fun copyWithExistingInput(): Logical

        /**
         * Creates and returns a copy of this [OperatorNode.Logical] and all inputs that belong to the same [GroupId], up and until the base of the tree.
         *
         * @param replacements The input [OperatorNode.Logical] that act as replacement for the remaining inputs. Can be empty!
         * @return Copy of this [OperatorNode.Logical].
         */
        abstract fun copyWithExistingGroupInput(vararg replacements: Logical): Logical

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
     * @see OperatorNode
     */
    sealed class Physical : OperatorNode() {

        /** The [OperatorNode.Logical] that receives the results produced by this [OperatorNode.Logical] as input. May be null, which makes this [OperatorNode.Logical] the root of the tree. */
        var output: Physical? = null
            internal set

        /** The root of this [OperatorNode.Physical], i.e., the final [OperatorNode.Physical] in terms of operation that usually produces the output. */
        val root: Physical
            get() = this.output?.root ?: this

        /** The base of this [OperatorNode], i.e., the starting point(s) in terms of operation. Depending on the tree structure, multiple bases may exist. */
        abstract val base: List<Physical>

        /** Map containing all [ValueStatistics] about the [ColumnDef]s processed in this [OperatorNode.Physical]. */
        abstract val statistics: Map<ColumnDef<*>, ValueStatistics<*>>

        /** Most [OperatorNode.Physical]s are executable by default. */
        override val executable: Boolean = true

        /** The estimated number of rows this [OperatorNode.Physical] generates. */
        context(BindingContext, Tuple)
        abstract val outputSize: Long

        /** An estimation of the [Cost] incurred by this [OperatorNode.Physical]. */
        context(BindingContext, Tuple)
        abstract val cost: Cost

        /** An estimation of the [Cost] incurred by the query plan up and until this [OperatorNode.Physical]. */
        context(BindingContext, Tuple)
        abstract val totalCost: Cost

        /** An estimation of the [Cost] incurred by the parallelizable portion of the query plan up and until this [OperatorNode.Physical]. */
        context(BindingContext, Tuple)
        abstract val parallelizableCost: Cost

        /** An estimation of the [Cost] incurred by the sequential portion of the query plan up and until this [OperatorNode.Physical]. */
        context(BindingContext, Tuple)
        val sequentialCost: Cost
            get() = this.totalCost - this.parallelizableCost

        /**
         * Creates and returns a copy of this [OperatorNode.Physical]. Furthermore, connects the provided [input] to the copied [OperatorNode.Physical]s.
         *
         * @param input The [Physical]s that act as input. Replacement takes place based on the [GroupId]
         * @return Copy of this [OperatorNode.Physical].
         */
        abstract fun copyWithNewInput(vararg input: Physical): Physical

        /**
         * Creates and returns a copy of this [OperatorNode.Physical] including all its inputs reaching up to the [base] of the tree.
         *
         * @return Deep copy of this [OperatorNode.Physical].
         */
        abstract fun copyWithExistingInput(): Physical

        /**
         * Creates and returns a copy of this [OperatorNode.Physical] and all inputs that belong to the same [GroupId], up and until the base of the tree.
         *
         * @param replacements The input [OperatorNode.Physical] that act as replacement for the remaining inputs. Can be empty!
         * @return Copy of this [OperatorNode.Physical].
         */
        abstract fun copyWithExistingGroupInput(vararg replacements: Physical): Physical

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
         * Tries to create a partitioned version of the query plan by using this [OperatorNode.Physical] as partition point.
         *
         * By default, a call to this method propagates up a tree until a [OperatorNode.Physical] that allows for
         * partitioning (i.e. lacks the [NotPartitionableTrait]) or the end of the tree has been reached.
         *
         * @param ctx The [QueryContext] to use for partitioning.
         * @param max The maximum number of partitions. Usually determined by the available threads.
         * @return [OperatorNode.Physical] that represents the base of a new, partitioned query plan or null.
         */
        abstract fun tryPartition(ctx: QueryContext, max: Int): Physical?

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