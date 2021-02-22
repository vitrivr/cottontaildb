package org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates.FilterPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate

/**
 * A [UnaryLogicalOperatorNode] that formalizes filtering using some [BooleanPredicate].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class FilterLogicalOperatorNode(val predicate: BooleanPredicate) : UnaryLogicalOperatorNode() {

    /** The [FilterLogicalOperatorNode] returns the [ColumnDef] of its input, or no column at all. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns

    /** The [FilterLogicalOperatorNode] requires all [ColumnDef]s used in the [BooleanPredicate]. */
    override val requires: Array<ColumnDef<*>>
        get() = this.predicate.columns.toTypedArray()

    /**
     * Returns a copy of this [FilterLogicalOperatorNode]
     *
     * @return Copy of this [FilterLogicalOperatorNode]
     */
    override fun copy(): FilterLogicalOperatorNode = FilterLogicalOperatorNode(this.predicate)

    /**
     * Returns a [FilterPhysicalOperatorNode] representation of this [FilterLogicalOperatorNode]
     *
     * @return [FilterPhysicalOperatorNode]
     */
    override fun implement(): Physical = FilterPhysicalOperatorNode(this.predicate)

    /**
     * Calculates and returns the digest for this [FilterLogicalOperatorNode].
     *
     * @return Digest for this [FilterLogicalOperatorNode]
     */
    override fun digest(): Long = 31L * super.digest() + this.predicate.digest()
}