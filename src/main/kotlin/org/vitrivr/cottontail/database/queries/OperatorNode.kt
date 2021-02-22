package org.vitrivr.cottontail.database.queries

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import java.util.*

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

    /** Whether or not this [OperatorNode] is executable. */
    abstract val executable: Boolean

    /**
     * The [OperatorNode] that receives the results produced by this [OperatorNode] as input.
     *
     * May be null, which makes this [OperatorNode] the root of the tree.
     */
    var output: OperatorNode? = null
        protected set

    /** The [OperatorNode] provides the inputs for this [OperatorNode]. May be empty, which makes this [OperatorNode] the base of the tree */
    val inputs: MutableList<OperatorNode> = LinkedList()

    /** The root of this [OperatorNode], i.e., the final [OperatorNode] in terms of operation that usually produces the output. */
    val root: OperatorNode
        get() = this.output?.root ?: this

    /** The base of this [OperatorNode], i.e., the starting point(s) in terms of operation. Dependingon on the tree structure, multiple bases may exist. */
    val base: Collection<OperatorNode>
        get() = when (this.inputs.size) {
            0 -> listOf(this)
            1 -> this.inputs.first().base
            else -> this.inputs.flatMap { it.base }
        }

    /** The [ColumnDef]s produced by this [OperatorNode]. */
    abstract val columns: Array<ColumnDef<*>>

    /** The [ColumnDef]s by which the output of this [OperatorNode] is sorted. By default, there is no particular order. */
    open val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** List of [ColumnDef]s required by this [OperatorNode] in order to be able to function. */
    open val requires: Array<ColumnDef<*>>
        get() = this.inputs.flatMap { it.requires.toList() }.toTypedArray()

    /**
     * Adds the provided [OperatorNode] to the [OperatorNode.inputs] of this [OperatorNode]. This method takes care of
     * updating the input's output pointer.
     *
     * @param input The [OperatorNode] that should act as new input of this [OperatorNode].
     * @return Reference to the input [OperatorNode]
     */
    fun <T : OperatorNode> addInput(input: T): T {
        check(this.inputArity > this.inputs.size) { "Cannot add input OperatorNode because $this already has too many inputs (arity = ${this.inputArity})." }
        this.inputs.add(input)
        input.output = this
        return input
    }

    /**
     * Performs late value binding using the given [QueryContext].
     *
     * [OperatorNode] are required to propagate calls to [bindValues] up the tree in addition
     * to executing the binding locally. Consequently, the call is propagated to all input [OperatorNode]
     * of this [OperatorNode].
     *
     * @param ctx [QueryContext] to use to resolve this [Binding].
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: QueryContext): OperatorNode {
        for (i in this.inputs) {
            i.bindValues(ctx)
        }
        return this
    }

    /**
     * Creates and returns an exact copy of this [OperatorNode] without any [inputs] or [output]s.
     *
     * @return Copy of this [OperatorNode].
     */
    abstract fun copy(): OperatorNode

    /**
     * Creates and returns a deep copy of this [OperatorNode] including a deep copy of all incoming
     * input [OperatorNode]s
     *
     * @return Exact copy of this [OperatorNode] and all parent [OperatorNode]s,
     */
    fun deepCopy(): OperatorNode {
        val t = this.copy()
        for (p in this.inputs) {
            val c = p.deepCopy()
            t.addInput(c)
        }
        return t
    }

    /**
     * Creates and returns a copy of all outgoing [OperatorNode] to this [OperatorNode].
     * May be null, if this [OperatorNode] has no children.
     *
     * @return Exact copy of this [OperatorNode]'s child and its children.
     */
    fun copyOutput(): OperatorNode? {
        val c = this.output
        return if (c != null) {
            val cc = c.copy()
            c.copyOutput()?.addInput(cc)
            cc
        } else {
            null
        }
    }

    /**
     * Calculates and returns the digest for this [OperatorNode.Physical].
     *
     * @return Digest for this [OperatorNode.Physical]
     */
    override fun digest(): Long = this.javaClass.hashCode().toLong()

    /**
     * Calculates and returns the deep digest for this [OperatorNode]. Consists of a combination
     * of all digests down from the base of the tree up and until this [OperatorNode],
     *
     * Consequently, the call is propagated to all input [OperatorNode] of this [OperatorNode].
     *
     * @return Deep digest for this [OperatorNode]
     */
    fun deepDigest(): Long {
        var digest = this.digest()
        for (i in this.inputs) {
            digest = 29 * digest + i.deepDigest()
        }
        return digest
    }

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

        /** [OperatorNode.Logical]s are never executable. */
        override val executable: Boolean = false

        /**
         * Creates and returns a copy of this [OperatorNode.Logical] without any children or parents.
         *
         * @return Copy of this [OperatorNode.Logical].
         */
        abstract override fun copy(): Logical

        /**
         * Creates and returns an implementation of this [OperatorNode.Logical]
         *
         * @return [OperatorNode.Physical] representing this [OperatorNode.Logical].
         */
        abstract fun implement(): Physical

        /**
         * Creates and returns an implementation of this [OperatorNode.Logical]
         *
         * @return [OperatorNode.Physical] representing this [OperatorNode.Logical].
         */
        fun deepImplement(): Physical {
            val t = this.implement()
            for (p in this.inputs) {
                when (p) {
                    is Logical -> t.addInput(p.deepImplement())
                    is Physical -> throw IllegalStateException("Encountered physical node expression in logical node expression tree. This is considered a programmer's error!")
                }
            }
            return t
        }
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

        /** The estimated number of rows this [OperatorNode.Physical] generates. */
        abstract val outputSize: Long

        /** An estimation of the [Cost] incurred by this [OperatorNode.Physical]. */
        abstract val cost: Cost

        /** An estimation of the [Cost] incurred by the tree up and until this [OperatorNode.Physical]. */
        val totalCost: Cost
            get() = if (this.inputs.isEmpty()) {
                this.cost
            } else {
                var cost = this.cost
                for (p in this.inputs) {
                    cost += when (p) {
                        is Logical -> throw IllegalStateException("Encountered logical node expression in physical node expression tree. This is considered a programmer's error!")
                        is Physical -> p.totalCost
                    }
                }
                cost
            }

        /** True, if this [OperatorNode.Physical] can be partitioned, false otherwise. */
        abstract val canBePartitioned: Boolean

        /**
         * Creates and returns a copy of this [OperatorNode.Physical] without any children or parents.
         *
         * @return Copy of this [OperatorNode.Physical].
         */
        abstract override fun copy(): Physical

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