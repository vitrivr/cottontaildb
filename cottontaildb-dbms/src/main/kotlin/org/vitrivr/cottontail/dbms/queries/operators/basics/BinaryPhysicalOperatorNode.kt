package org.vitrivr.cottontail.dbms.queries.operators.basics

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.PlaceholderPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics
import java.io.PrintStream

/**
 * An abstract [OperatorNode.Physical] implementation that has exactly two [OperatorNode.Physical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
abstract class BinaryPhysicalOperatorNode(val left: Physical, val right: Physical) : OperatorNode.Physical() {

    /** Input arity of [BinaryPhysicalOperatorNode] is always two. */
    final override val inputArity: Int = 2

    /** A [BinaryPhysicalOperatorNode]'s index is always the [depth] of its [left] input + 1. This is set in the [left]'s setter. */
    final override var depth: Int = 0
        private set

    /**
     * The group ID of a [BinaryPhysicalOperatorNode] is always the one of its left parent.
     *
     * This is an (arbitrary) definition but very relevant when implementing [BinaryPhysicalOperatorNode]s.
     */
    final override val groupId: GroupId = this.left.groupId

    /** The [base] of a [BinaryPhysicalOperatorNode] is the sum of its input's bases. */
    final override val base: List<Physical> by lazy {
        this.left.base + this.right.base
    }

    /** The [totalCost] of a [BinaryPhysicalOperatorNode] is always the sum of its own and its input cost. Can be overridden!*/
    context(BindingContext, Tuple)    final override val totalCost: Cost
        get() = this.left.totalCost + this.right.totalCost + this.cost

    /** By default, the [BinaryPhysicalOperatorNode] outputs the physical [ColumnDef] of its left input. Can be overridden! */
    override val physicalColumns: List<ColumnDef<*>>
        get() = this.left.physicalColumns

    /** By default, the [BinaryPhysicalOperatorNode] outputs the [ColumnDef] of its left input. Can be overridden! */
    override val columns: List<ColumnDef<*>>
        get() = this.left.columns

    /** By default, the [BinaryPhysicalOperatorNode] inherits its [Trait]s from its left input. Can be overridden! */
    override val traits: Map<TraitType<*>, Trait>
        get() = this.left.traits

    /** By default, the output size of a [UnaryPhysicalOperatorNode] is the same as its left input's output size. Can be overridden! */
    context(BindingContext, Tuple)    override val outputSize: Long
        get() = this.left.outputSize

    /** By default, a [BinaryPhysicalOperatorNode]'s has no specific requirements. Can be overridden! */
    override val requires: List<ColumnDef<*>>
        get() = emptyList()

    /** By default, a [BinaryPhysicalOperatorNode]'s statistics are retained from its left input. Can be overridden! */
    override val statistics: Map<ColumnDef<*>, ValueStatistics<*>>
        get() = this.left.statistics

    init {
        this.left.output = this
        this.right.output = this
    }

    /**
     * Creates and returns a copy of this [BinaryPhysicalOperatorNode] without any children and with new parents.
     *
     * @param input The [OperatorNode.Physical] that act as new parents for this [BinaryPhysicalOperatorNode]. Must contain two entries!
     * @return Copy of this [BinaryPhysicalOperatorNode].
     */
    abstract override fun copyWithNewInput(vararg input: Physical): BinaryPhysicalOperatorNode

    /**
     * Creates and returns a copy of this [BinaryPhysicalOperatorNode] and its children tree using the provided nodes as new parents.
     *
     * @param input The [OperatorNode.Physical]s that act as input. Must contain two entries!
     * @return Copy of this [BinaryPhysicalOperatorNode] with its output.
     */
    final override fun copyWithOutput(vararg input: Physical): BinaryPhysicalOperatorNode {
        val copy = when (input.size) {
            2 -> this.copyWithNewInput(input[0], input[1])
            1 -> this.copyWithNewInput(input[0], PlaceholderPhysicalOperatorNode(this.right.groupId, this.right.columns, this.right.physicalColumns))
            0-> this.copyWithNewInput(PlaceholderPhysicalOperatorNode(this.left.groupId, this.left.columns, this.left.physicalColumns), PlaceholderPhysicalOperatorNode(this.right.groupId, this.right.columns, this.right.physicalColumns))
            else -> throw IllegalArgumentException("The input arity for BinaryPhysicalOperatorNode.copyWithOutput() must be smaller or equal to 2 but is ${input.size}. This is a programmer's error!")
        }
        this.output?.copyWithOutput(copy)
        return copy
    }

    /**
     * Creates and returns a copy of this [BinaryPhysicalOperatorNode] and its parents but without any children.
     *
     * @return Copy of this [BinaryPhysicalOperatorNode].
     */
    final override fun copyWithExistingInput(): BinaryPhysicalOperatorNode
        = this.copyWithNewInput(this.left.copyWithExistingInput(), this.right.copyWithExistingInput())

    /**
     * Creates and returns a copy of this [BinaryPhysicalOperatorNode] and the entire input [OperatorNode.Physical] tree that belong to the same [GroupId].
     *
     * @param replacements The input [OperatorNode.Physical] that act as replacement for the remaining inputs. Can be empty!
     * @return Copy of this [BinaryPhysicalOperatorNode].
     */
    final override fun copyWithExistingGroupInput(vararg replacements: Physical): BinaryPhysicalOperatorNode = when (replacements.size) {
        1 -> this.copyWithNewInput(this.left.copyWithExistingGroupInput(), replacements[0])
        0 -> this.copyWithNewInput(this.left.copyWithExistingGroupInput(), PlaceholderPhysicalOperatorNode(this.right.groupId, this.right.columns, this.right.physicalColumns))
        else -> throw IllegalArgumentException("The input arity for BinaryPhysicalOperatorNode.copyWithGroupInputs() must be smaller or equal to 1 but is ${replacements.size}. This is a programmer's error!")
    }

    /**
     * Determines, if this [BinaryPhysicalOperatorNode] can be executed in the given [QueryContext].
     *
     * Typically, a [BinaryPhysicalOperatorNode] can be executed if its inputs can be executed.
     *
     * @param ctx The [QueryContext] to check.
     * @return True if this [BinaryPhysicalOperatorNode] is executable, false otherwise.
     */
    override fun canBeExecuted(ctx: QueryContext): Boolean {
        return this.left.canBeExecuted(ctx) && this.right.canBeExecuted(ctx)
    }

    /**
     * By default, a [BinaryPhysicalOperatorNode] cannot be partitioned and hence this method returns null.
     *
     * Must be overridden in order to support partitioning.
     */
    override fun tryPartition(ctx: QueryContext, max: Int): Physical? = null

    /**
     * By default, [BinaryPhysicalOperatorNode] cannot be partitioned and hence calling this method throws an exception.
     *
     * @param partitions The total number of partitions.
     * @param p The partition index.
     * @return null
     */
    override fun partition(partitions: Int, p: Int): Physical {
        throw UnsupportedOperationException("BinaryPhysicalOperatorNode cannot be partitioned!")
    }

    /**
     * Calculates and returns the total [Digest] for this [BinaryPhysicalOperatorNode].
     *
     * @return Total [Digest] for this [BinaryPhysicalOperatorNode]
     */
    final override fun totalDigest(): Digest {
        var result = this.depth.hashCode().toLong()
        result += 151L * result + this.left.totalDigest()
        result += 151L * result + this.right.totalDigest()
        result += 151L * result + this.digest()
        return result
    }

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    override fun printTo(p: PrintStream) {
        this.left.printTo(p)
        this.right.printTo(p)
        super.printTo(p)
    }
}