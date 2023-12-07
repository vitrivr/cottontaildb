package org.vitrivr.cottontail.dbms.queries.operators.basics

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.nodes.CopyableNode
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType

/**
 * An abstract [OperatorNode.Logical] implementation that has no input.
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
abstract class NullaryLogicalOperatorNode: OperatorNode.Logical(), CopyableNode {
    /** Input arity of [NullaryLogicalOperatorNode] is always zero. */
    final override val inputArity: Int = 0

    /** A [NullaryLogicalOperatorNode] is always the root of a tree and thus has the index 0. */
    final override val depth: Int = 0

    /** A [NullaryLogicalOperatorNode] does not depend on any other [GroupId]. */
    final override val dependsOn: Array<GroupId>
        get() = arrayOf(this.groupId)

    /** The [base] of a [NullaryLogicalOperatorNode] is always itself. */
    final override val base: Collection<Logical>
        get() = listOf(this)

    /** By default, a [NullaryLogicalOperatorNode] doesn't have any requirement. */
    override val requires: List<ColumnDef<*>>
        get() = emptyList()

    /** By default, a [NullaryLogicalOperatorNode] has an empty set of [Trait]s. */
    override val traits: Map<TraitType<*>,Trait>
        get() = emptyMap()

    /**
     * Creates and returns a copy of this [NullaryLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [NullaryLogicalOperatorNode].
     */
    abstract override fun copy(): NullaryLogicalOperatorNode

    /**
     * Creates and returns a copy of this [NullaryLogicalOperatorNode] using the provided [OperatorNode.Logical] as input.
     *
     * @param input The [OperatorNode.Logical]s that act as input to this [NullaryLogicalOperatorNode].
     * @return Copy of this [NullaryLogicalOperatorNode] with new input.
     */
    final override fun copyWithNewInput(vararg input: Logical): NullaryLogicalOperatorNode {
        require(input.isEmpty()) { "The input arity for NullaryLogicalOperatorNode.copy() must be 0 but is ${input.size}. This is a programmer's error!"}
        return this.copy()
    }

    /**
     * Creates and returns a copy of this [NullaryLogicalOperatorNode].
     *
     * @param replacements The list of replacements. Must be empty!
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithExistingGroupInput(vararg replacements: Logical): NullaryLogicalOperatorNode {
        require(replacements.isEmpty()) { "The input arity for NullaryLogicalOperatorNode.copyWithExistingGroupInput() must be 0 but is ${replacements.size}. This is a programmer's error!"}
        return this.copy()
    }

    /**
     * Creates and returns a copy of this [NullaryLogicalOperatorNode].
     *
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithExistingInput(): NullaryLogicalOperatorNode = this.copy()

    /**
     * Creates and returns a copy of this [NullaryLogicalOperatorNode] with its output reaching down to the [root] of the tree.
     * Furthermore, connects the provided [input] to the copied [NullaryLogicalOperatorNode]s.
     *
     * @param input The [OperatorNode.Logical]s that act as input. Must be empty!
     * @return Copy of this [NullaryLogicalOperatorNode] with its output.
     */
    override fun copyWithOutput(vararg input: Logical): Logical {
        require(input.isEmpty()) { "Cannot provide input for NullaryLogicalOperatorNode." }
        val copy = this.copy()
        return (this.output?.copyWithOutput(copy) ?: copy).root
    }

    /**
     * Calculates and returns the total [Digest] for this [NullaryLogicalOperatorNode].
     *
     * @return Total [Digest] for this [NullaryLogicalOperatorNode]
     */
    final override fun totalDigest(): Digest = this.digest()
}