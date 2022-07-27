package org.vitrivr.cottontail.dbms.queries.operators.basics

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
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

    /** The [totalCost] of a [BinaryPhysicalOperatorNode] is always the sum of its own and its input cost. */
    final override val totalCost: Cost by lazy {
        this.left.totalCost + this.right.totalCost + this.cost
    }

    /** By default, [BinaryPhysicalOperatorNode]s are executable if both their inputs are executable. */
    override val executable: Boolean by lazy {
        this.left.executable && this.right.executable
    }

    /** By default, the [BinaryPhysicalOperatorNode] outputs the physical [ColumnDef] of its left input. */
    override val physicalColumns: List<ColumnDef<*>>
        get() = this.left.physicalColumns

    /** By default, the [BinaryPhysicalOperatorNode] outputs the [ColumnDef] of its left input. */
    override val columns: List<ColumnDef<*>>
        get() = this.left.columns

    /** By default, the output size of a [UnaryPhysicalOperatorNode] is the same as its left input's output size. */
    override val outputSize: Long
        get() = this.left.outputSize

    /** By default, a [BinaryPhysicalOperatorNode]'s has no specific requirements. */
    override val requires: List<ColumnDef<*>>
        get() = emptyList()

    /** By default, a [BinaryPhysicalOperatorNode]'s statistics are retained from its left input. */
    override val statistics: Map<ColumnDef<*>,ValueStatistics<*>>
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
        require(input.size == 2) { "The input arity for BinaryPhysicalOperatorNode.copyWithOutput() must be 2 but is ${input.size}. This is a programmer's error!"}
        val copy = this.copyWithNewInput(*input)
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
    final override fun copyWithExistingGroupInput(vararg replacements: Physical): BinaryPhysicalOperatorNode {
        require(replacements.size == 1) { "The input arity for BinaryPhysicalOperatorNode.copyWithGroupInputs() must be 1 but is ${replacements.size}. This is a programmer's error!"}
        return this.copyWithNewInput(this.left.copyWithExistingInput(), replacements[0])
    }

    /**
     * Calculates and returns the digest for this [BinaryPhysicalOperatorNode].
     *
     * @return [Digest] for this [BinaryPhysicalOperatorNode]
     */
    override fun digest(): Digest {
        var result = 27L * hashCode() + this.left.digest()
        result = 27L * result + this.right.digest()
        return 27L * result + this.depth.hashCode()
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