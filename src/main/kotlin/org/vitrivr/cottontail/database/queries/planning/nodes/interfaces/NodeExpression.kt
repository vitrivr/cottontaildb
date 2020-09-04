package org.vitrivr.cottontail.database.queries.planning.nodes.interfaces

import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.RuleShuttle
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage

/**
 * [NodeExpression]s are components in the Cottontail DB execution plan and represent flow of
 * information. [NodeExpression]s take 0 to n [org.vitrivr.cottontail.model.recordset.Recordset]s
 * as input and transform them into a single output [org.vitrivr.cottontail.model.recordset.Recordset].
 *
 * [NodeExpression]s allow for reasoning and transformation of the execution plan during query optimization.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
sealed class NodeExpression {

    /** Returns the root of this [NodeExpression], i.e., the start of the tree. */
    val root: NodeExpression
        get() = this.output?.root ?: this

    /** The [NodeExpression] provides the inputs for this [NodeExpression]. */
    val inputs: MutableList<NodeExpression> = mutableListOf()

    /**
     * The  [NodeExpression] that receives the results produced by this [NodeExpression] as input.
     *
     * May be null, which makes this [NodeExpression] the root of the tree.
     */
    var output: NodeExpression? = null
        protected set

    /** The arity of this [NodeExpression], i.e., the number of parents or inputs allowed. */
    abstract val inputArity: Int

    /** True, if the tree up and until this [NodeExpression] is executable. */
    val executable: Boolean
        get() = (this is PhysicalNodeExpression) && this.inputs.all { executable }

    /**
     * Applies the [RewriteRule]s contained in the given [RuleShuttle] to this [NodeExpression]
     * and, if a transformation can take place, adds the resulting [NodeExpression] to the list
     * of candidates. Then passes the [RuleShuttle] up the tree.
     *
     * @param shuttle The [RuleShuttle] to apply.
     * @param candidates The list of candidates.
     */
    fun <T : NodeExpression> apply(shuttle: RuleShuttle<T>, candidates: MutableList<T>) {
        shuttle.apply(this, candidates)
        this.inputs.forEach { it.apply(shuttle, candidates) }
    }

    /**
     * Updates the [NodeExpression.output] of this [NodeExpression] to the given [NodeExpression].
     *
     * This method should take care of updating the child's parents as well as the old child's parents
     * (if such exist).
     *
     * @param child The [NodeExpression] that should act as new child of this [NodeExpression].
     * @return Reference to the provided [NodeExpression]
     */
    fun updateOutput(output: NodeExpression): NodeExpression {
        this.output?.inputs?.remove(this)
        output.inputs.add(this)
        this.output = output
        return output
    }

    /**
     * Creates and returns a copy of this [NodeExpression] without any children or parents.
     *
     * @return Copy of this [NodeExpression].
     */
    abstract fun copy(): NodeExpression

    /**
     * Creates and returns a copy of the tree up and until this [NodeExpression]. Includes at least
     * one [NodeExpression] which is the current [NodeExpression]
     *
     * @return Exact copy of this [NodeExpression] and all parent [NodeExpression]s,
     */
    fun copyWithInputs(): NodeExpression {
        val t = this.copy()
        for (p in this.inputs) {
            val c = p.copyWithInputs()
            c.updateOutput(t)
        }
        return t
    }

    /**
     * Creates and returns a copy of the tree down from this [NodeExpression]. May be null, if this
     * [NodeExpression] has no children.
     *
     * @return Exact copy of this [NodeExpression]'s child and its children.
     */
    fun copyOutput(): NodeExpression? {
        val c = this.output
        return if (c != null) {
            val cc = c.copy()
            val ccc = c.copyOutput()
            if (ccc != null) {
                cc.updateOutput(ccc)
            }
            cc
        } else {
            null
        }
    }

    /**
     * A logical [NodeExpression] in the Cottontail DB query execution plan.
     *
     * [LogicalNodeExpression]s are purely abstract and cannot be executed directly. They belong to the
     * first phase of the query optimization process, in which a plain input expression is transformed
     * into equivalent, logical expressions.
     *
     * @author Ralph Gasser
     * @version 1.0
     *
     * @see NodeExpression
     * @see PhysicalNodeExpression
     */
    abstract class LogicalNodeExpression : NodeExpression() {

        /**
         * Creates and returns a copy of this [LogicalNodeExpression] without any children or parents.
         *
         * @return Copy of this [LogicalNodeExpression].
         */
        abstract override fun copy(): LogicalNodeExpression

    }

    /**
     * A physical [NodeExpression] in the Cottontail DB query execution plan.
     *
     * [PhysicalNodeExpression]s are a proxy to a corresponding [ExecutionStage]. They belong to the
     * second phase of the query optimization process, in which [LogicalNodeExpression]s are replaced
     * by [PhysicalNodeExpression]s so as to generate an executable plan.
     *
     * [PhysicalNodeExpression] are associated with a cost model that allows the query planner to select
     * the optimal plan.
     *
     * @author Ralph Gasser
     * @version 1.0
     *
     * @see NodeExpression
     * @see PhysicalNodeExpression
     */

    abstract class PhysicalNodeExpression : NodeExpression() {
        /** The estimated number of rows this [NodeExpression] generates. */
        abstract val outputSize: Long

        /** An estimation of the [Cost] incurred by this [NodeExpression]. */
        abstract val cost: Cost

        /** An estimation of the [Cost] incurred by the tree up and until this [NodeExpression]. */
        val totalCost: Cost
            get() = if (this.inputs.isEmpty()) {
                this.cost
            } else {
                var cost = this.cost
                for (p in this.inputs) {
                    if (p is PhysicalNodeExpression) {
                        cost += p.totalCost
                    }
                }
                cost
            }

        /**
         * Creates and returns a copy of this [PhysicalNodeExpression] without any children or parents.
         *
         * @return Copy of this [PhysicalNodeExpression].
         */
        abstract override fun copy(): PhysicalNodeExpression

        /**
         * Converts this [PhysicalNodeExpression] to the corresponding [ExecutionStage].
         *
         * @return [ExecutionStage]
         */
        abstract fun toStage(context: QueryPlannerContext): ExecutionStage
    }
}