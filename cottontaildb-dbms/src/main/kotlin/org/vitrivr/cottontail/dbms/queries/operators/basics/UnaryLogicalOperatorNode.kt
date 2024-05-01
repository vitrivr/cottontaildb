package org.vitrivr.cottontail.dbms.queries.operators.basics

import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import java.io.PrintStream

/**
 * An abstract [OperatorNode.Logical] implementation that has a single [OperatorNode] as input.
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
abstract class UnaryLogicalOperatorNode(val input: Logical): OperatorNode.Logical() {
    /** Input arity of [UnaryLogicalOperatorNode] is always one. */
    final override val inputArity: Int = 1

    /** A [UnaryLogicalOperatorNode]'s index is always the [depth] of its [input] + 1. */
    final override var depth: Int = 0
        private set

    /** The group Id of a [UnaryLogicalOperatorNode] is always the one of its parent.*/
    final override val groupId: GroupId
        get() = this.input.groupId

    /** A [UnaryLogicalOperatorNode] inherits its dependency from its parent. */
    final override val dependsOn: Array<GroupId>
        get() = this.input.dependsOn

    /** The [base] of a [UnaryLogicalOperatorNode] is always its [input]'s base. Can be overridden! */
    final override val base: Collection<Logical>
        get() = this.input.base

    /** By default, the [UnaryLogicalOperatorNode] outputs the [Binding.Column] of its input. Can be overridden! */
    override val columns: List<Binding.Column>
        get() = this.input.columns

    /** By default, a [UnaryLogicalOperatorNode]'s requirements are unspecified. Can be overridden! */
    override val requires: List<Binding.Column>
        get() = emptyList()

    /** By default, a [UnaryLogicalOperatorNode] inherits its traits from its parent. Can be overridden! */
    override val traits: Map<TraitType<*>,Trait>
        get() = this.input.traits

    init {
        this.input.output = this
    }

    /**
     * Creates and returns a copy of this [UnaryLogicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Logical]s that act as input.
     * @return Copy of this [UnaryLogicalOperatorNode].
     */
    abstract override fun copyWithNewInput(vararg input: Logical): UnaryLogicalOperatorNode

    /**
     * Creates and returns a copy of this [UnaryLogicalOperatorNode] and its entire output [OperatorNode.Logical] tree using the provided nodes as input.
     *
     * @param input The [OperatorNode.Logical]s that act as input.
     * @return Copy of this [UnaryLogicalOperatorNode] with its output.
     */
    final override fun copyWithOutput(vararg input: Logical): UnaryLogicalOperatorNode {
        require(input.size == 1) { "The input arity for UnaryLogicalOperatorNode.copyWithOutput() must be 1 but is ${input.size}. This is a programmer's error!"}
        val copy = this.copyWithNewInput(*input)
        this.output?.copyWithOutput(copy)
        return copy
    }

    /**
     * Creates and returns a copy of this [UnaryLogicalOperatorNode] and the entire input [OperatorNode.Logical] tree that
     * belong to the same [GroupId].
     *
     * @param replacements The input [OperatorNode.Logical] that act as replacement for the remaining inputs. Can be empty!
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithExistingGroupInput(vararg replacements: Logical): UnaryLogicalOperatorNode {
        require(replacements.isEmpty()) { "The input arity for UnaryLogicalOperatorNode.copyWithGroupInputs() must be 0 but is ${replacements.size}. This is a programmer's error!" }
        return this.copyWithNewInput(this.input.copyWithExistingGroupInput())
    }

    /**
     * Creates and returns a copy of this [UnaryLogicalOperatorNode] and the entire input [OperatorNode.Logical] tree.
     *
     * @return Copy of this [UnaryLogicalOperatorNode].
     */
    final override fun copyWithExistingInput(): UnaryLogicalOperatorNode
        = this.copyWithNewInput(this.input.copyWithExistingInput())

    /**
     * Calculates and returns the total [Digest] for this [UnaryLogicalOperatorNode].
     *
     * @return Total [Digest] for this [UnaryLogicalOperatorNode]
     */
    final override fun totalDigest(): Digest{
        var result = 197L * this.depth + this.input.totalDigest()
        result += 197L * result + this.digest()
        return result
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