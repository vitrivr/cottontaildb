package org.vitrivr.cottontail.dbms.queries.operators.basics

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.*
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.recordset.PlaceholderRecord
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.merge.MergeLimitingSortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.merge.MergePhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.LimitPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import java.io.PrintStream

/**
 * An abstract [OperatorNode.Physical] implementation that has a single [OperatorNode] as input.
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
abstract class UnaryPhysicalOperatorNode(val input: Physical) : OperatorNode.Physical() {

    /** The arity of the [UnaryPhysicalOperatorNode] is always on. */
    final override val inputArity = 1

    /** A [UnaryLogicalOperatorNode]'s index is always the [depth] of its [input] + 1. This is set in the [input]'s setter. */
    final override var depth: Int = 0
        private set

    /** The group Id of a [UnaryPhysicalOperatorNode] is always the one of its parent.*/
    final override val groupId: GroupId = this.input.groupId

    /** A [UnaryPhysicalOperatorNode] inherits its dependencies from its parent. */
    final override val dependsOn: Array<GroupId>
        get() = this.input.dependsOn

    /** The [base] of a [UnaryPhysicalOperatorNode] is always its parent's base. */
    final override val base: List<Physical>
        get() = this.input.base

    /** By default, a [UnaryPhysicalOperatorNode] has no specific requirements. */
    override val requires: List<ColumnDef<*>> = emptyList()

    /** By default, [UnaryPhysicalOperatorNode]s are executable if their input is executable. */
    override val executable: Boolean
        get() = this.input.executable

    /** By default, the [UnaryPhysicalOperatorNode] outputs the physical [ColumnDef] of its input. Can be overridden! */
    override val physicalColumns: List<ColumnDef<*>>
        get() = this.input.physicalColumns

    /** By default, the [UnaryPhysicalOperatorNode] outputs the [ColumnDef] of its input. Can be overridden! */
    override val columns: List<ColumnDef<*>>
        get() = this.input.columns

    /** By default, a [UnaryLogicalOperatorNode] inherits its traits from its parent. Can be overridden! */
    override val traits: Map<TraitType<*>,Trait>
        get() = this.input.traits

    /** By default, a [UnaryPhysicalOperatorNode]'s statistics are retained from its input. Can be overridden! */
    override val statistics:Map<ColumnDef<*>, ValueStatistics<*>>
        get() = this.input.statistics

    /** The [totalCost] of a [UnaryPhysicalOperatorNode] is always the sum of its own and its input cost. */
    context(BindingContext,Record)    final override val totalCost: Cost
        get() = this.input.totalCost + this.cost

    /** The [parallelizableCost] of a [UnaryPhysicalOperatorNode] is always the sum of its own and its input cost. */
    context(BindingContext,Record)    final override val parallelizableCost: Cost
        get() = if (this.hasTrait(NotPartitionableTrait)) {
            this.totalCost
        } else {
            this.input.parallelizableCost
        }

    /** By default, the output size of a [UnaryPhysicalOperatorNode] is the same as its input's output size. Can be overridden! */
    context(BindingContext,Record)
    override val outputSize: Long
        get() = this.input.outputSize

    init {
        this.input.output = this
    }

    /**
     * Creates and returns a copy of this [UnaryPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [UnaryPhysicalOperatorNode].
     */
    abstract override fun copyWithNewInput(vararg input: Physical): UnaryPhysicalOperatorNode

    /**
     * Creates and returns a copy of this [UnaryPhysicalOperatorNode] and its entire output [OperatorNode.Physical] tree using the provided nodes as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [UnaryPhysicalOperatorNode] with its output.
     */
    final override fun copyWithOutput(vararg input: Physical): UnaryPhysicalOperatorNode {
        require(input.size == 1) { "The input arity for UnaryPhysicalOperatorNode.copyWithOutput() must be 1 but is ${input.size}. This is a programmer's error!"}
        val copy = this.copyWithNewInput(*input)
        this.output?.copyWithOutput(copy)
        return copy
    }

    /**
     * Creates and returns a copy of this [UnaryPhysicalOperatorNode] and the entire input [OperatorNode.Physical] tree that
     * belong to the same [GroupId].
     *
     * @param replacements The input [OperatorNode.Physical] that act as replacement for the remaining inputs. Can be empty!
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithExistingGroupInput(vararg replacements: Physical): UnaryPhysicalOperatorNode {
        require(replacements.isEmpty()) { "The input arity for UnaryPhysicalOperatorNode.copyWithGroupInputs() must be 0 but is ${replacements.size}. This is a programmer's error!" }
        return this.copyWithNewInput(this.input.copyWithExistingGroupInput())
    }

    /**
     * Creates and returns a copy of this [UnaryPhysicalOperatorNode] and the entire input [OperatorNode.Physical] tree.
     *
     * @return Copy of this [UnaryPhysicalOperatorNode].
     */
    final override fun copyWithExistingInput(): UnaryPhysicalOperatorNode
        = this.copyWithNewInput(this.input.copyWithExistingInput())

    /**
     * Tries to create a partitioned version of this [UnaryPhysicalOperatorNode] and its parents.
     *
     * In general, partitioning is only possible if the [input] doesn't have a [NotPartitionableTrait]. In absence of the
     * trait, partitioning is implemented using different strategies depending on the [OrderTrait] and [LimitTrait] of the
     * incoming tree.
     *
     * Otherwise, [UnaryPhysicalOperatorNode] propagates this call up a tree until a [OperatorNode.Physical]
     * that does not have this trait is reached.
     *
     * @return Array of [OperatorNode.Physical]s.
     */
    override fun tryPartition(ctx: QueryContext, max: Int): Physical? {
        require(max > 1) { "Expected number of partitions to be greater than one but encountered $max." }
        return if (!this.input.hasTrait(NotPartitionableTrait)) {
            val partitions = with(ctx.bindings) {
                with(PlaceholderRecord) {
                    ctx.costPolicy.parallelisation(this@UnaryPhysicalOperatorNode.parallelizableCost, this@UnaryPhysicalOperatorNode.totalCost, max)
                }
            }
            if (partitions <= 1) return null
            val inbound = (0 until partitions).map { this.input.partition(partitions, it) }
            when {
                this.input.hasTrait(LimitTrait) && this.input.hasTrait(OrderTrait) -> {
                    val order = input[OrderTrait]!!
                    val limit = input[LimitTrait]!!
                    this.copyWithOutput(MergeLimitingSortPhysicalOperatorNode(*inbound.toTypedArray(), sortOn = order.order, limit = limit.limit))
                }
                this.input.hasTrait(OrderTrait) -> TODO()
                this.input.hasTrait(LimitTrait) -> {
                    val limit = this.input[LimitTrait]!!
                    this.copyWithOutput(LimitPhysicalOperatorNode(MergePhysicalOperatorNode(*inbound.toTypedArray()), limit = limit.limit))
                }
                else -> this.copyWithOutput(MergePhysicalOperatorNode(*inbound.toTypedArray()))
            }
        } else {
            this.input.tryPartition(ctx, max)
        }
    }

    /**
     * Generates a partitioned version of this [UnaryPhysicalOperatorNode].
     *
     * By default, this call is simply propagated upstream, because partitioning usually
     * takes place close to the base of the tree (i.e. the source).
     *
     * Not to be confused with [tryPartition].
     *
     * @param partitions The total number of partitions.
     * @param p The partition number.
     * @return [OperatorNode.Physical]
     */
    override fun partition(partitions: Int, p: Int): UnaryPhysicalOperatorNode
        = this.copyWithNewInput(this.input.partition(partitions, p))

    /**
     * Calculates and returns the digest for this [UnaryPhysicalOperatorNode].
     *
     * @return [Digest] for this [UnaryPhysicalOperatorNode]
     */
    final override fun digest(): Digest {
        val result = 27L * this.hashCode() + this.input.digest()
        return 27L * result + this.depth.hashCode()
    }

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    override fun printTo(p: PrintStream) {
        this.input.printTo(p)
        super.printTo(p)
    }
}

