package org.vitrivr.cottontail.dbms.queries.operators.logical

import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.PlaceholderPhysicalOperatorNode


/**
 * This is a [NullaryLogicalOperatorNode] that acts as placeholder for higher-level group inputs.
 *
 * Mainly used during decomposition of query plans into groups.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class PlaceholderLogicalOperatorNode(override val groupId: GroupId, override val columns: List<Binding.Column>): NullaryLogicalOperatorNode() {
    override val name = "Placeholder"
    override fun copy(): NullaryLogicalOperatorNode = PlaceholderLogicalOperatorNode(this.groupId, this.columns)
    override fun implement() = PlaceholderPhysicalOperatorNode(this.groupId, this.columns)
    override fun digest(): Digest = 0L
}