package org.vitrivr.cottontail.dbms.queries.operators.physical

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.nodes.traits.*
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.planning.cost.CostPolicy
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.merge.MergeLimitingSortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.merge.MergePhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.SortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.LimitPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import java.io.PrintStream

/**
 * An abstract [OperatorNode.Physical] implementation that has a single [OperatorNode] as input.
 *
 * @author Ralph Gasser
 * @version 2.7.0
 */
abstract class UnaryPhysicalOperatorNode(input: Physical? = null) : OperatorNode.Physical() {

    /** The arity of the [UnaryPhysicalOperatorNode] is always on. */
    final override val inputArity = 1

    /** A [UnaryLogicalOperatorNode]'s index is always the [depth] of its [input] + 1. This is set in the [input]'s setter. */
    final override var depth: Int = 0
        private set

    /** The group Id of a [UnaryPhysicalOperatorNode] is always the one of its parent.*/
    final override val groupId: GroupId
        get() = this.input?.groupId ?: 0

    /** The input [OperatorNode.Logical]. */
    var input: Physical? = null
        protected set(value) {
            require(value?.output == null) { "Cannot connect $value to $this: Output is already occupied!" }
            field?.output = null
            value?.output = this
            this.depth = value?.depth?.plus(1) ?: 0
            field = value
        }

    /** The [base] of a [UnaryPhysicalOperatorNode] is always its parent's base. */
    final override val base: Collection<Physical>
        get() = this.input?.base ?: emptyList()

    /** The [totalCost] of a [UnaryPhysicalOperatorNode] is always the sum of its own and its input cost. */
    final override val totalCost: Cost
        get() = (this.input?.totalCost ?: Cost.ZERO) + this.cost

    /** The [parallelizableCost] of a [UnaryPhysicalOperatorNode] is always the sum of its own and its input cost. */
    final override val parallelizableCost: Cost
        get() {
            val input = this.input ?: return Cost.ZERO
            return if (this.hasTrait(NotPartitionableTrait)) {
                this.totalCost
            } else {
                input.parallelizableCost
            }
        }

    /** By default, a [UnaryPhysicalOperatorNode] has no specific requirements. */
    override val requires: List<ColumnDef<*>> = emptyList()

    /** By default, [UnaryPhysicalOperatorNode]s are executable if their input is executable. */
    override val executable: Boolean
        get() = this.input?.executable ?: false

    /** By default, the [UnaryPhysicalOperatorNode] outputs the physical [ColumnDef] of its input. */
    override val physicalColumns: List<ColumnDef<*>>
        get() = (this.input?.physicalColumns ?: emptyList())

    /** By default, the [UnaryPhysicalOperatorNode] outputs the [ColumnDef] of its input. */
    override val columns: List<ColumnDef<*>>
        get() = (this.input?.columns ?: emptyList())

    /** By default, the output size of a [UnaryPhysicalOperatorNode] is the same as its input's output size. */
    override val outputSize: Long
        get() = (this.input?.outputSize ?: 0)

    /** By default, a [UnaryLogicalOperatorNode] inherits its traits from its parent. */
    override val traits: Map<TraitType<*>,Trait>
        get() = this.input?.traits ?: emptyMap()

    /** By default, a [UnaryPhysicalOperatorNode]'s statistics are retained from its input.*/
    override val statistics:Map<ColumnDef<*>, ValueStatistics<*>>
        get() = this.input?.statistics ?: emptyMap()

    init {
        this.input = input
    }

    /**
     * Creates and returns a copy of this [UnaryPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [UnaryPhysicalOperatorNode].
     */
    abstract override fun copy(): UnaryPhysicalOperatorNode

    /**
     * Creates and returns a copy of this [UnaryPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [UnaryPhysicalOperatorNode].
     */
    final override fun copy(vararg input: Physical): UnaryPhysicalOperatorNode {
        require(input.size <= this.inputArity) { "Cannot provide more than ${this.inputArity} inputs for ${this.javaClass.simpleName}." }
        val copy = this.copy()
        copy.input = input.getOrNull(0)
        return copy
    }

    /**
     * Creates and returns a copy of this [UnaryPhysicalOperatorNode] and its entire output [OperatorNode.Physical] tree using the provided nodes as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [UnaryPhysicalOperatorNode] with its output.
     */
    final override fun copyWithOutput(vararg input: Physical): Physical {
        require(input.size <= this.inputArity) { "Cannot provide more than ${this.inputArity} inputs for ${this.javaClass.simpleName}." }
        val copy = this.copy(*input)
        return (this.output?.copyWithOutput(copy) ?: copy).root
    }

    /**
     * Creates and returns a copy of this [UnaryPhysicalOperatorNode] and the entire input [OperatorNode.Physical] tree that belong to the same [GroupId].
     *
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithGroupInputs(): UnaryPhysicalOperatorNode {
        val copy = this.copy()
        copy.input = this.input?.copyWithGroupInputs()
        return copy
    }

    /**
     * Creates and returns a copy of this [UnaryPhysicalOperatorNode] and the entire input [OperatorNode.Physical] tree.
     *
     * @return Copy of this [OperatorNode.Physical].
     */
    final override fun copyWithInputs() = this.copyWithGroupInputs()

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
    override fun tryPartition(policy: CostPolicy, max: Int): Physical? {
        require(max > 1) { "Expected number of partitions to be greater than one but encountered $max." }
        val input = this.input ?: throw IllegalStateException("Tried to propagate call to tryPartition to an absent input. This is a programmer's error!")
        return if (!input.hasTrait(NotPartitionableTrait)) {
            val partitions = policy.parallelisation(this.parallelizableCost, this.totalCost, max)
            if (partitions <= 1) return null
            val inbound = (0 until partitions).map { input.partition(partitions, it) }
            when {
                input.hasTrait(LimitTrait) && input.hasTrait(OrderTrait) -> {
                    val order = input[OrderTrait]!!
                    val limit = input[LimitTrait]!!
                    this.copyWithOutput(MergeLimitingSortPhysicalOperatorNode(*inbound.toTypedArray(), sortOn = order.order, limit = limit.limit))
                }
                input.hasTrait(OrderTrait) -> {
                    val order = input[OrderTrait]!!
                    this.copyWithOutput(SortPhysicalOperatorNode(MergePhysicalOperatorNode(*inbound.toTypedArray()), sortOn = order.order))
                }
                input.hasTrait(LimitTrait) -> {
                    val limit = input[LimitTrait]!!
                    this.copyWithOutput(LimitPhysicalOperatorNode(MergePhysicalOperatorNode(*inbound.toTypedArray()), limit = limit.limit))
                }
                else -> this.copyWithOutput(MergePhysicalOperatorNode(*inbound.toTypedArray()))
            }
        } else {
            input.tryPartition(policy, max)
        }
    }

    /**
     * Generates a partitioned version of this [UnaryPhysicalOperatorNode].
     *
     * By default, this call is simply propagated for [UnaryPhysicalOperatorNode], because
     * partitioning usually takes place close to the root of the tree (i.e. the source).
     *
     * Not to be confused with [tryPartition].
     *
     * @param partitions The total number of partitions.
     * @param p The partition number.
     * @return [OperatorNode.Physical]
     */
    override fun partition(partitions: Int, p: Int): Physical {
        val input = this.input ?: throw IllegalStateException("Tried to propagate call to partition($partitions, $p) to an absent input. This is a programmer's error!")
        val copy = this.copy()
        copy.input = input.partition(partitions, p)
        return copy
    }

    /**
     * Calculates and returns the digest for this [UnaryPhysicalOperatorNode].
     *
     * @return [Digest] for this [UnaryPhysicalOperatorNode]
     */
    final override fun digest(): Digest {
        val result = 27L * this.hashCode() + (this.input?.digest() ?: -1L)
        return 27L * result + this.depth.hashCode()
    }

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    override fun printTo(p: PrintStream) {
        this.input?.printTo(p)
        super.printTo(p)
    }
}

