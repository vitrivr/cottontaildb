package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.FilterLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.KnnLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset.FilterPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset.KnnPhysicalNodeExpression

/**
 * A [RewriteRule] that implements a [FilterLogicalNodeExpression] by a [FilterPhysicalNodeExpression].
 *
 * This is a simple 1:1 replacement (implementation).
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object FilterImplementationRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is FilterLogicalNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is FilterLogicalNodeExpression) {
            val parent = node.copyWithInputs().inputs.first()
            val children = node.copyOutput()
            val p = FilterPhysicalNodeExpression(node.predicate)
            parent.updateOutput(p)
            if (children != null) {
                p.updateOutput(children)
            }
            return p
        }
        return null
    }
}