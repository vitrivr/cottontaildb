package org.vitrivr.cottontail.database.queries.planning.basics

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class AbstractNodeExpression : NodeExpression {
    override var child: AbstractNodeExpression? = null
        protected set

    override val parents = mutableListOf<NodeExpression>()

    override fun setChild(node: NodeExpression): NodeExpression {
        require(node is AbstractNodeExpression) { "Given Node is not an AbstractNode and therefore not compatible." }
        if (this.child != null) {
            this.child!!.parents.remove(this)
        }
        node.parents.add(this)
        this.child = node
        return node
    }
}