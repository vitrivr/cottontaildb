package org.vitrivr.cottontail.database.queries.planning.basics

import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage

/**
 * A single [NodeExpression] expression in the Cottontail DB execution plan, which acts as a proxy for a [ExecutionStage].
 * [NodeExpression]s are used during the optimization phase of Cottontail DB query execution.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface NodeExpression {
    /** The child [NodeExpression] of this [NodeExpression]. */
    val child: NodeExpression?

    /** Returns the root of this [NodeExpression], i.e. start of the tree. */
    val root: NodeExpression
        get() = this.child?.root ?: this

    /** The parent [NodeExpression]s of this [NodeExpression]. May be empty. */
    val parents: List<NodeExpression>

    /** The estimated number of rows this [NodeExpression] generates. */
    val output: Long

    /** An estimation of the [Cost] incurred by this [NodeExpression]. */
    val cost: Cost

    /** An estimation of the [Cost] incurred by the tree up and until this [NodeExpression]. */
    val totalCost: Cost
        get() = if (this.parents.isEmpty()) {
            this.cost
        } else {
            var cost = this.cost
            for (p in this.parents) {
                cost += p.totalCost
            }
            cost
        }

    /**
     * Sets the [NodeExpression.child] of this [NodeExpression] to the  given [NodeExpression], updating its parents accordingly
     *
     * @param child The [NodeExpression] that should act as a child of this [NodeExpression].
     * @return Reference to the provided [NodeExpression]
     */
    fun setChild(child: NodeExpression): NodeExpression

    /**
     * Creates and returns a copy of this [NodeExpression].
     *
     * @return Exact copy of this [NodeExpression].
     */
    fun copy(): NodeExpression

    /**
     * Creates and returns a copy of the tree up and until this [NodeExpression]. Includes at least
     * one [NodeExpression] which is the current [NodeExpression]
     *
     * @return Exact copy of this [NodeExpression] and all parent [NodeExpression]s,
     */
    fun copyParent(): NodeExpression {
        val t = this.copy()
        for (p in this.parents) {
            val c = p.copyParent()
            c.setChild(t)
        }
        return t
    }

    /**
     * Creates and returns a copy of the tree down from this [NodeExpression]. May be null, if this
     * [NodeExpression] has no children.
     *
     * @return Exact copy of this [NodeExpression]'s child and its children.
     */
    fun copyChildren(): NodeExpression? {
        val c = this.child
        return if (c != null) {
            val cc = c.copy()
            val ccc = c.copyChildren()
            if (ccc != null) {
                cc.setChild(ccc)
            }
            cc
        } else {
            null
        }
    }

    /**
     * Converts this [NodeExpression] to the respective [ExecutionStage].
     *
     * @return [ExecutionStage]
     */
    fun toStage(context: QueryPlannerContext): ExecutionStage
}