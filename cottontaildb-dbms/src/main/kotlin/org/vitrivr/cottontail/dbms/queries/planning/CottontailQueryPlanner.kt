package org.vitrivr.cottontail.dbms.queries.planning

import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.planning.cost.NormalizedCost
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.*
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * This is a rather simple query planner that optimizes a [OperatorNode] tree by recursively applying a set of [RewriteRule]s
 * to get more sophisticated yet equivalent [OperatorNode] trees. Query planning & optimization takes place in three stages:
 *
 * 1. The planner rewrites the logical tree means of other [OperatorNode.Logical]s, to generate several, equivalent representations of the query.
 * 2. The candidate trees are "implemented", i.e., the planner creates a physical tree for each logical tree.
 * 3. The planner rewrites the physical trees by replacing [OperatorNode.Physical]s by [OperatorNode.Physical] to arrive at a better execution plan.
 *
 * Finally, the best plan in terms of [Cost] is selected.
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class CottontailQueryPlanner(private val logicalRules: Collection<RewriteRule>, private val physicalRules: Collection<RewriteRule>, planCacheSize: Int = 100) {

    /** Internal cache used to store query plans for known queries. */
    private val planCache = CottontailPlanCache(planCacheSize)

    /**
     * Executes query planning for a given [QueryContext] and generates a [OperatorNode.Physical] for it.
     *
     * @param logical The [OperatorNode.Logical] to plan.
     * @param bypassCache Flag indicating, if the plan cache should be bypassed (forces new planning).
     * @param cache Flag indicating, if the resulting plan should be cached.
     *
     * @throws QueryException.QueryPlannerException If planner fails to generate a valid execution plan.
     */
    context(QueryContext)
    fun planAndSelect(logical: OperatorNode.Logical, bypassCache: Boolean = false, cache: Boolean = false): OperatorNode.Physical {
        /* Plan the query. */
        val candidates = this.plan(logical).map { (groupId, plans) ->
            groupId to plans.first().first
        }.toMap()

        /* Combine different sub-plans, if they exist. */
        var selected = candidates[0] ?: throw IllegalStateException("No query plan for groupId zero; This is a programmer's error!")
        if (candidates.size > 1) {
            selected = this@CottontailQueryPlanner.compose(selected, candidates)
        }

        /* Update plan cache and return. */
        return selected
    }

    /**
     * Generates a list of equivalent [OperatorNode.Physical]s by recursively applying [RewriteRule]s
     * on the seed [OperatorNode.Logical] and derived [OperatorNode]s.
     *
     * @param logical The [OperatorNode.Logical] to plan.
     * @param limit The number of plan variant to return. Defaults to 1.
     *
     * @return Map of [GroupId] to list of candidate plans with associated score.
     */
    context(QueryContext)
    fun plan(logical: OperatorNode.Logical, limit: Int = 1): Map<GroupId,List<Pair<OperatorNode.Physical,Float>>> {
        /* Decomposes the tree into subtrees based on group. */
        val decomposition = this.decompose(logical)

        /* Stage 1: Logical query planning for each group. */
        val stage1 = decomposition.map { (groupId, plan) -> groupId to this.optimizeLogical(plan) }.toMap()

        /* Stage 2: Physical query planning for each group. */
        val stage2 = stage1.map { (groupId, plans) ->
            groupId to plans.flatMap {
                this.optimizePhysical(it.implement())
            }
        }.toMap()

        /* Generate candidate plans. */
        return with(this@QueryContext.bindings) {
            with(MissingTuple) {
                stage2.map { (groupId, plans) ->
                    val validPlans = plans.filter { it.totalCost.isValid() }
                    val total = validPlans.map { it.totalCost }
                    val normalized = NormalizedCost.normalize(total)
                    val candidates = validPlans.zip(normalized).map { (p, cost) -> p to this@QueryContext.costPolicy.toScore(cost) }.sortedBy { it.second }.take(10)
                    groupId to candidates
                }.toMap()
            }
        }
    }

    /**
     * Decomposes the given [OperatorNode.Logical], i.e., splits the tree into a sub-tree per group.
     * This is a preparation for query optimisation.
     *
     * @param operator [OperatorNode.Logical] That should be decomposed.
     */
    private fun decompose(operator: OperatorNode.Logical): Map<Int, OperatorNode.Logical> {
        val decomposition = Int2ObjectLinkedOpenHashMap<OperatorNode.Logical>()
        decomposition[operator.groupId] = operator.copyWithExistingGroupInput()
        var next: OperatorNode.Logical? = operator
        while (next != null) {
            when (next) {
                is NullaryLogicalOperatorNode -> return decomposition
                is UnaryLogicalOperatorNode -> {
                    next = next.input
                }

                is BinaryLogicalOperatorNode -> {
                    decomposition.putAll(this.decompose(next.right.copyWithExistingInput()))
                    next = next.left
                }

                is NAryLogicalOperatorNode -> {
                    next.inputs.drop(1).forEach { decomposition.putAll(this.decompose(it.copyWithExistingInput())) }
                    next = next.inputs.first()
                }
            }
        }
        return decomposition
    }

    /**
     * Composes a decomposition [Map] into a [OperatorNode.Physical], i.e., splits the tree into a sub-tree per group.
     * This is a preparation for query optimisation.
     *
     * @param decomposition The decomposition [Map] that should be recomposed.
     */
    private fun compose(startAt: OperatorNode.Physical, decomposition: Map<GroupId, OperatorNode.Physical>): OperatorNode.Physical {
        var next: OperatorNode.Physical = startAt
        do {
            next = when (next) {
                is NullaryPhysicalOperatorNode -> return next.root
                is UnaryPhysicalOperatorNode -> next.input
                is BinaryPhysicalOperatorNode ->  next.copyWithOutput(next.left.copyWithExistingInput(), compose(decomposition[next.right.groupId]!!, decomposition)).left
                is NAryPhysicalOperatorNode -> next.copyWithOutput(next.inputs[0].copyWithExistingInput(), *next.inputs.drop(1).map { compose(decomposition[it.groupId]!!, decomposition) }.toTypedArray()).inputs[0]
            }
        } while (true)
    }

    /**
     * Performs logical optimization, i.e., by replacing parts in the logical execution plan.
     *
     * @param operator The [OperatorNode.Logical] that should be optimized. Optimization starts from the given node, regardless of whether it is root or not.
     */
    context(QueryContext)
    private fun optimizeLogical(operator: OperatorNode.Logical): List<OperatorNode.Logical> {
        /* List of candidates, objects to explore and digests */
        val candidates = MemoizingOperatorList(operator)
        val explore = MemoizingOperatorList(operator)

        /* Now start exploring... */
        var pointer = explore.dequeue()
        while (pointer != null) {
            /* Apply rules to node and add results to list for exploration. */
            for (rule in this.logicalRules) {
                if (rule.canBeApplied(pointer, this@QueryContext)) {
                    val result = rule.apply(pointer, this@QueryContext)
                    if (result is OperatorNode.Logical) {
                        explore.enqueue(result.root)
                        candidates.enqueue(result.root)
                    }
                }
            }

            /* Add all inputs to operators that need further exploration. */
            when (pointer) {
                is NAryLogicalOperatorNode -> explore.enqueue(pointer.inputs.first())
                is BinaryLogicalOperatorNode -> explore.enqueue(pointer.left)
                is UnaryLogicalOperatorNode -> explore.enqueue(pointer.input)
                is NullaryLogicalOperatorNode -> { /* No op. */ }
            }

            /* Get next in line. */
            pointer = explore.dequeue()
        }

        /** Add candidates for group to list. */
        return candidates.toList()
    }

    /**
     * Performs physical optimization, i.e., by replacing parts in the physical execution plan.
     *
     * @param operator The [OperatorNode.Physical] that should be optimized. Optimization starts from the given node, regardless of whether it is root or not.
     */
    context(QueryContext)
    private fun optimizePhysical(operator: OperatorNode.Physical): List<OperatorNode.Physical> {
        /* List of candidates, objects to explore and digests */
        val candidates = MemoizingOperatorList<OperatorNode.Physical>()
        val explore = MemoizingOperatorList(operator.root)
        if (operator.canBeExecuted(this@QueryContext)) {
            candidates.enqueue(operator)
        }

        /* Now start exploring... */
        var pointer = explore.dequeue()
        while (pointer != null) {
            /* Apply rules to node and add results to list for exploration. */
            for (rule in this.physicalRules) {
                if (rule.canBeApplied(pointer, this@QueryContext)) {
                    val result = rule.apply(pointer, this@QueryContext)
                    if (result is OperatorNode.Physical) {
                        explore.enqueue(result.root)
                        if (result.root.canBeExecuted(this@QueryContext)) {
                            candidates.enqueue(result.root)
                        }
                    }
                }
            }

            /* Add all inputs to operators that need further exploration. */
            when (pointer) {
                is NAryPhysicalOperatorNode -> explore.enqueue(pointer.inputs.firstOrNull() ?: throw IllegalStateException("Encountered null node in physical operator node tree (node = $pointer). This is a programmer's error!"))
                is BinaryPhysicalOperatorNode -> explore.enqueue(pointer.left)
                is UnaryPhysicalOperatorNode -> explore.enqueue(pointer.input)
                else -> { /* No op. */ }
            }

            /* Get next in line. */
            pointer = explore.dequeue()
        }
        return candidates.toList()
    }
}