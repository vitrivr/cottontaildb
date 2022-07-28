package org.vitrivr.cottontail.dbms.queries.operators.basics

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.binding.MissingRecord
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import java.io.PrintStream

/**
 * An abstract [OperatorNode.Physical] implementation that has multiple [OperatorNode.Physical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.8.0
 */
abstract class NAryPhysicalOperatorNode(vararg inputs: Physical): OperatorNode.Physical() {

    /** The inputs to this [NAryPhysicalOperatorNode]. The first input belongs to the same group. */
    val inputs: List<Physical> = inputs.toList()

    /** A [NAryPhysicalOperatorNode]'s index is always the [depth] of its first input + 1. */
    final override var depth: Int = 0
        private set

    /**
     * The group ID of a [NAryPhysicalOperatorNode] is always the one of its left parent.
     *
     * This is an (arbitrary) definition but very relevant when implementing [BinaryPhysicalOperatorNode]s.
     */
    final override val groupId: GroupId
        get() = this.inputs.firstOrNull()?.groupId ?: 0

    /** The [base] of a [NAryPhysicalOperatorNode] is the sum of its input's bases. */
    final override val base: List<Physical> by lazy {
        this.inputs.flatMap { it.base }
    }

    /** The input arity of a [NAryPhysicalOperatorNode] depends on the number of inputs. */
    final override val inputArity: Int
        get() = this.inputs.size

    /** The [totalCost] of a [NAryPhysicalOperatorNode] is the sum of its own and its input cost. */
    context(BindingContext,Record)    final override val totalCost: Cost
        get() {
            var cost = this.cost
            for (i in inputs) {
                cost += i.totalCost
            }
            return cost
        }

    /** By default, a [NAryPhysicalOperatorNode]'s requirements are empty. Can be overridden! */
    override val requires: List<ColumnDef<*>>
        get() =  emptyList()

    /** [NAryPhysicalOperatorNode]s are executable if all their inputs are executable.  Can be overridden! */
    override val executable: Boolean
        get() = (this.inputs.size == this.inputArity) && this.inputs.all { it.executable }

    /** By default, the [NAryPhysicalOperatorNode] outputs the physical [ColumnDef] of its input.  Can be overridden! */
    override val physicalColumns: List<ColumnDef<*>>
        get() = (this.inputs.firstOrNull()?.physicalColumns ?: emptyList())

    /** By default, the [NAryPhysicalOperatorNode] outputs the [ColumnDef] of its input.  Can be overridden! */
    override val columns: List<ColumnDef<*>>
        get() = (this.inputs.firstOrNull()?.columns ?: emptyList())

    /** By default, a [NAryPhysicalOperatorNode]'s statistics are retained.  Can be overridden! */
    override val statistics: Map<ColumnDef<*>, ValueStatistics<*>>
        get() = this.inputs.firstOrNull()?.statistics ?: emptyMap()

    /** By default, a [NAryPhysicalOperatorNode]'s parallelizable costs are [Cost.ZERO].  Can be overridden! */
    context(MissingRecord,BindingContext)
    override val parallelizableCost: Cost
        get() = Cost.ZERO

    init {
        this.inputs.forEach { it.output = this }
    }

    /**
     * Creates and returns a copy of this [NAryPhysicalOperatorNode] using the provided nodes as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input to this [NAryPhysicalOperatorNode].
     * @return Copy of this [NAryPhysicalOperatorNode] with new input.
     */
    abstract override fun copyWithNewInput(vararg input: Physical): NAryPhysicalOperatorNode

    /**
     * Creates and returns a copy of this [NAryPhysicalOperatorNode] and its entire output [OperatorNode.Physical] tree using the provided nodes as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [NAryPhysicalOperatorNode] with its output.
     */
    final override fun copyWithOutput(vararg input: Physical): NAryPhysicalOperatorNode {
        val copy = this.copyWithNewInput(*input)
        this.output?.copyWithOutput(copy)
        return copy
    }

    /**
     * Creates and returns a copy of this [NAryPhysicalOperatorNode] and the entire input [OperatorNode.Physical] tree that belong to the same [GroupId].
     *
     * @return Copy of this [NAryPhysicalOperatorNode].
     */
    final override fun copyWithExistingGroupInput(vararg replacements: Physical): NAryPhysicalOperatorNode {
        require(replacements.size == this.inputArity - 1) { "The input arity for NAryPhysicalOperatorNode.copyWithGroupInputs() must be (${this.inputArity -1}) but is ${replacements.size}. This is a programmer's error!"}
        return this.copyWithNewInput(this.inputs.first().copyWithExistingInput(), *replacements)
    }

    /**
     * Creates and returns a copy of this [NAryPhysicalOperatorNode] and the entire input [OperatorNode.Physical] tree.
     *
     * @return Copy of this [NAryPhysicalOperatorNode].
     */
    final override fun copyWithExistingInput(): NAryPhysicalOperatorNode {
        return this.copyWithNewInput(*this.inputs.map { it.copyWithExistingInput() }.toTypedArray())
    }

    /**
     * By default, a [NAryPhysicalOperatorNode] cannot be partitioned and hence this method returns null.
     *
     * Must be overridden in order to support partitioning.
     */
    override fun tryPartition(ctx: QueryContext, max: Int): Physical? = null

    /**
     * By default, [NAryPhysicalOperatorNode] cannot be partitioned and hence calling this method throws an exception.
     *
     * @param partitions The total number of partitions.
     * @param p The partition index.
     * @return null
     */
    override fun partition(partitions: Int, p: Int): Physical {
        throw UnsupportedOperationException("NAryPhysicalOperatorNodes cannot be partitioned!")
    }

    /**
     * Calculates and returns the digest for this [BinaryPhysicalOperatorNode].
     *
     * @return [Digest] for this [BinaryPhysicalOperatorNode]
     */
    override fun digest(): Digest {
        var result = this.hashCode().toLong()
        for (i in this.inputs) {
            result += 27L * result + i.digest()
        }
        result += 27L * result + this.depth.hashCode()
        return result
    }

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    override fun printTo(p: PrintStream) {
        this.inputs.forEach { it.printTo(p) }
        super.printTo(p)
    }
}