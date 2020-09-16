package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.KnnLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset.KnnPhysicalNodeExpression

/**
 * A [RewriteRule] that implements a [KnnLogicalNodeExpression] by a [KnnPhysicalNodeExpression].
 *
 * This is a simple 1:1 replacement (implementation).
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object KnnImplementationRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is KnnLogicalNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is KnnLogicalNodeExpression) {
            val parent = node.copyWithInputs().inputs.first()
            val children = node.copyOutput()
            val p = KnnPhysicalNodeExpression(node.predicate)
            parent.updateOutput(p)
            if (children != null) {
                p.updateOutput(children)
            }
            return p
        }
        return null
    }
}