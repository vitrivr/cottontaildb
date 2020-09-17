package org.vitrivr.cottontail.database.queries.planning.nodes.physical

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression

/**
 * An abstract [PhysicalNodeExpression] implementation that has no input node.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class NullaryPhysicalNodeExpression : PhysicalNodeExpression() {
    /** The arity of the [NullaryPhysicalNodeExpression] is always on. */
    override val inputArity = 0


    /**
     * Creates [p] partitions of this [NullaryPhysicalNodeExpression]
     *
     * @param p The desired number of partitions.
     * @return Array of [NullaryPhysicalNodeExpression]s.
     */
    abstract fun partition(p: Int): List<NullaryPhysicalNodeExpression>
}