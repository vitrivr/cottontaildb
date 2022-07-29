package org.vitrivr.cottontail.dbms.queries.operators.basics

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.dbms.queries.operators.logical.PlaceholderLogicalOperatorNode
import java.io.PrintStream

/**
 * An abstract [OperatorNode.Logical] implementation that has exactly two [OperatorNode.Logical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
abstract class BinaryLogicalOperatorNode(val left: Logical, val right: Logical): OperatorNode.Logical() {

    /** Input arity of [UnaryLogicalOperatorNode] is always two. */
    final override val inputArity: Int = 2

    /** A [BinaryLogicalOperatorNode]'s index is always the [depth] of its [left] input + 1. */
    final override var depth: Int = 0
        private set

    /**
     * The group ID of a [BinaryLogicalOperatorNode] is always the one of its left parent.
     *
     * This is an (arbitrary) definition but very relevant when implementing [BinaryLogicalOperatorNode]s.
     */
    final override val groupId: GroupId
        get() = this.left.groupId

    /** The [base] of a [BinaryLogicalOperatorNode] is the sum of its input's bases. Can be overridden! */
    final override val base by lazy {
        this.left.base + this.right.base
    }

    /** By default, the [BinaryLogicalOperatorNode] outputs the physical [ColumnDef] of its input. Can be overridden! */
    override val physicalColumns: List<ColumnDef<*>>
        get() = this.left.physicalColumns

    /** By default, the [BinaryLogicalOperatorNode] outputs the [ColumnDef] of its input. Can be overridden! */
    override val columns: List<ColumnDef<*>>
        get() = this.left.columns

    /** By default, the [BinaryLogicalOperatorNode] inherits its [Trait]s from its left input. Can be overridden! */
    override val traits: Map<TraitType<*>, Trait>
        get() = this.left.traits

    /** By default, a [BinaryLogicalOperatorNode]'s requirements are empty. Can be overridden! */
    override val requires: List<ColumnDef<*>>
        get() = emptyList()

    init {
        this.left.output = this
        this.right.output = this
    }

    /**
     * Creates and returns a copy of this [BinaryLogicalOperatorNode] without any children and with new parents.
     *
     * @param input The [OperatorNode.Physical] that act as new parents for this [BinaryLogicalOperatorNode]. Must contain two entries!
     * @return Copy of this [BinaryLogicalOperatorNode].
     */
    abstract override fun copyWithNewInput(vararg input: Logical): BinaryLogicalOperatorNode

    /**
     * Creates and returns a copy of this [BinaryLogicalOperatorNode] and its children tree using the provided nodes as new parents.
     *
     * @param input The [OperatorNode.Physical]s that act as input. Must contain two entries!
     * @return Copy of this [BinaryLogicalOperatorNode] with its output.
     */
    final override fun copyWithOutput(vararg input: Logical): BinaryLogicalOperatorNode {
        val copy = when (input.size) {
            2 -> this.copyWithNewInput(input[0], input[1])
            1 -> this.copyWithNewInput(input[0], PlaceholderLogicalOperatorNode(this.right.groupId, this.right.columns, this.right.physicalColumns))
            0-> this.copyWithNewInput(PlaceholderLogicalOperatorNode(this.left.groupId, this.left.columns, this.left.physicalColumns), PlaceholderLogicalOperatorNode(this.right.groupId, this.right.columns, this.right.physicalColumns))
            else -> throw IllegalArgumentException("The input arity for BinaryPhysicalOperatorNode.copyWithOutput() must be smaller or equal to 2 but is ${input.size}. This is a programmer's error!")
        }
        this.output?.copyWithOutput(copy)
        return copy
    }

    /**
     * Creates and returns a copy of this [BinaryLogicalOperatorNode] and its parents but without any children.
     *
     * @return Copy of this [BinaryLogicalOperatorNode].
     */
    final override fun copyWithExistingInput(): BinaryLogicalOperatorNode
        = this.copyWithNewInput(this.left.copyWithExistingInput(), this.right.copyWithExistingInput())

    /**
     * Creates and returns a copy of this [BinaryLogicalOperatorNode] and the entire input [OperatorNode.Physical] tree that belong to the same [GroupId].
     *
     * @param replacements The input [OperatorNode.Physical] that act as replacement for the remaining inputs. Can be empty!
     * @return Copy of this [BinaryLogicalOperatorNode].
     */
    final override fun copyWithExistingGroupInput(vararg replacements: Logical): BinaryLogicalOperatorNode = when (replacements.size) {
        1 -> this.copyWithNewInput(this.left.copyWithExistingGroupInput(), replacements[0])
        0 -> this.copyWithNewInput(this.left.copyWithExistingGroupInput(), PlaceholderLogicalOperatorNode(this.right.groupId, this.right.columns, this.right.physicalColumns))
        else -> throw IllegalArgumentException("The input arity for BinaryPhysicalOperatorNode.copyWithGroupInputs() must be smaller or equal to 1 but is ${replacements.size}. This is a programmer's error!")
    }

    /**
     * Calculates and returns the digest for this [BinaryLogicalOperatorNode].
     *
     * @return [Digest] for this [BinaryLogicalOperatorNode]
     */
    override fun digest(): Digest {
        var result = 33L * hashCode() + (this.left.digest())
        result = 33L * result + (this.right.digest())
        return 33L * result + this.depth.hashCode()
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