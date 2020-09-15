package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.ProjectionLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset.ProjectionPhysicalNodeExpression

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object ProjectionImplementationRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is ProjectionLogicalNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is ProjectionLogicalNodeExpression) {
            val parent = node.copyWithInputs().inputs.first()
            val children = node.copyOutput()
            val p = ProjectionPhysicalNodeExpression(node.type, node.fields)
            parent.updateOutput(p)
            if (children != null) {
                p.updateOutput(children)
            }
            return p
        }
        return null
    }
}