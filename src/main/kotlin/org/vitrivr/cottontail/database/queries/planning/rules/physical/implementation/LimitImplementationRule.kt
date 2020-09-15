package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LimitLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset.LimitPhysicalNodeExpression

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object LimitImplementationRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is LimitLogicalNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is LimitLogicalNodeExpression) {
            val parent = node.copyWithInputs().inputs.first()
            val children = node.copyOutput()
            val p = LimitPhysicalNodeExpression(node.limit, node.skip)
            parent.updateOutput(p)
            if (children != null) {
                p.updateOutput(children)
            }
            return p
        }
        return null
    }
}