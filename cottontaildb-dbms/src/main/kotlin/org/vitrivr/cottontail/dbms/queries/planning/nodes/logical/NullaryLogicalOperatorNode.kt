package org.vitrivr.cottontail.dbms.queries.planning.nodes.logical

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.queries.OperatorNode
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder

/**
 * An abstract [OperatorNode.Logical] implementation that has no input.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
abstract class NullaryLogicalOperatorNode : OperatorNode.Logical() {
    /** Input arity of [NullaryLogicalOperatorNode] is always zero. */
    final override val inputArity: Int = 0

    /** A [NullaryLogicalOperatorNode] is always the root of a tree and thus has the index 0. */
    final override val depth: Int = 0

    /** The [base] of a [NullaryLogicalOperatorNode] is always itself. */
    final override val base: Collection<Logical>
        get() = listOf(this)

    /** By default, a [NullaryLogicalOperatorNode]'s output is unordered. */
    override val sortOn: List<Pair<ColumnDef<*>, SortOrder>>
        get() = emptyList()

    /** By default, a [NullaryLogicalOperatorNode] doesn't have any requirement. */
    override val requires: List<ColumnDef<*>>
        get() = emptyList()

    /**
     * Creates and returns a copy of this [NullaryLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [NullaryLogicalOperatorNode].
     */
    abstract override fun copy(): NullaryLogicalOperatorNode

    /**
     * Creates and returns a copy of this [NullaryLogicalOperatorNode].
     *
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithGroupInputs(): NullaryLogicalOperatorNode = this.copy()

    /**
     * Creates and returns a copy of this [NullaryLogicalOperatorNode].
     *
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithInputs(): NullaryLogicalOperatorNode = this.copy()

    /**
     * Creates and returns a copy of this [NullaryLogicalOperatorNode] with its output reaching down to the [root] of the tree.
     * Furthermore connects the provided [input] to the copied [NullaryLogicalOperatorNode]s.
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
     * Calculates and returns the digest for this [NullaryLogicalOperatorNode].
     *
     * @return [Digest] for this [NullaryLogicalOperatorNode]
     */
    final override fun digest(): Digest = this.hashCode().toLong()
}