package org.vitrivr.cottontail.database.queries.planning

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.BinaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NAryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.BinaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.model.exceptions.QueryException
import java.util.*
import kotlin.collections.LinkedHashMap

/**
 * This is a rather simple query planner that optimizes a [OperatorNode] by recursively applying a set of [RewriteRule]s to get more
 * sophisticated yet equivalent [OperatorNode]s. Query planning & optimization takes place in three stages:
 *
 * 1. The logical tree is rewritten by means of other [OperatorNode.Logical]s, to generate several, equivalent representations of the query.
 * 2. The candidate trees are "implemented", i.e., a physical tree is created for each logical tree .
 * 3. The physical tree is rewritten by replacing [OperatorNode.Physical]s by [OperatorNode.Physical] to arrive at an executable query plan.
 *
 * Finally, the best plan in terms of [Cost] is selected.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class CottontailQueryPlanner(private val logicalRules: Collection<RewriteRule>, private val physicalRules: Collection<RewriteRule>, val planCacheSize: Int = 100) {

    /** Internal cache used to store query plans for known queries. */
    private val planCache = LinkedHashMap<Long, OperatorNode.Physical>()

    /**
     * Executes query planning for a given [QueryContext] and generates a [OperatorNode.Physical] for it.
     *
     * @param context The [QueryContext] to plan for.
     * @param bypassCache If the plan cache should be bypassed (forces new planning).
     *
     * @throws QueryException.QueryPlannerException If planner fails to generate a valid execution plan.
     */
    fun planAndSelect(context: QueryContext, bypassCache: Boolean = false, cache: Boolean = false) {
        /* Try to obtain PhysicalNodeExpression from plan cache, unless bypassCache has been set to true. */
        val logical = context.logical
        require(logical != null) { "Cannot plan for a QueryContext that doesn't have a valid logical query representation." }
        val digest = logical.digest()
        if (!bypassCache) {
            if (this.planCache.containsKey(digest)) {
                context.physical = this.planCache[digest]
                return
            }
        }

        /* Execute actual query planning and select candidate with lowest cost. */
        val candidates = this.plan(context)
        context.physical = candidates.minByOrNull { it.totalCost } ?: throw QueryException.QueryPlannerException("Failed to generate a physical execution plan for expression: $logical.")

        /* Update plan cache. */
        if (!cache) {
            if (this.planCache.size >= planCacheSize) {
                this.planCache.remove(this.planCache.keys.first())
            }
            this.planCache[digest] = context.physical!!
        }
    }

    /**
     * Generates a list of equivalent [OperatorNode.Physical]s by recursively applying [RewriteRule]s
     * on the seed [OperatorNode.Logical] and derived [OperatorNode]s.
     *
     * @param context The [QueryContext] to plan for.
     *
     * @return List of [OperatorNode.Physical] that implement the [OperatorNode.Logical]
     */
    fun plan(context: QueryContext): Collection<OperatorNode.Physical> {
        val logical = context.logical
        require(logical != null) { QueryException.QueryPlannerException("Cannot perform query planning for a QueryContext that doesn't have a logical query plan.") }

        /** Stage 1: Logical execution plans, i.e. plan alternatives for the input plan. */
        val stage1 = this.optimizeLogical(logical, context)

        /** Stage 2: Physical execution plans. */
        val stage2 = stage1.map { it.implement() }.filter { it.executable }

        /** Stage 3: Optimized, physical execution plans that are executable. */
        val stage3 = stage2.flatMap { this.optimizePhysical(it, context) }.filter { it.executable }
        return stage3
    }

    /**
     * Clears the plan cache of this [CottontailQueryPlanner]
     */
    fun clearCache() = this.planCache.clear()

    /**
     * Performs logical optimization, i.e., by replacing parts in the logical execution plan.
     *
     * @param operator The [OperatorNode.Logical] that should be optimized. Optimization starts from the given node, regardless of whether it is root or not.
     * @param ctx The [QueryContext] used for optimization.
     */
    private fun optimizeLogical(operator: OperatorNode.Logical, ctx: QueryContext): List<OperatorNode.Logical> {
        /* List of candidates, objects to explore and digests */
        val candidates = MemoizingOperatorList(operator)
        val explore = MemoizingOperatorList(operator)

        /* Now start exploring... */
        var pointer = explore.dequeue()
        while (pointer != null) {
            /* Apply rules to node and add results to list for exploration. */
            for (rule in this.logicalRules) {
                val result = rule.apply(pointer, ctx)
                if (result is OperatorNode.Logical) {
                    explore.enqueue(result)
                    candidates.enqueue(result)
                }
            }

            /* Add all inputs to operators that need further exploration. */
            when (pointer) {
                is NAryLogicalOperatorNode -> {
                    pointer.inputs.forEach { explore.enqueue(it) }
                }
                is BinaryLogicalOperatorNode -> {
                    explore.enqueue(pointer.left)
                    explore.enqueue(pointer.right)
                }
                is UnaryLogicalOperatorNode -> {
                    explore.enqueue(pointer.input)
                }
                is OperatorNode.Physical -> throw IllegalStateException("Encountered physical operator node in logical operator node tree. This is a programmer's error!")
            }

            /* Get next in line. */
            pointer = explore.dequeue()
        }
        return candidates.toList()
    }

    /**
     * Performs physical optimization, i.e., by replacing parts in the physical execution plan.
     *
     * @param operator The [OperatorNode.Physical] that should be optimized. Optimization starts from the given node, regardless of whether it is root or not.
     * @param ctx The [QueryContext] used for optimization.
     */
    private fun optimizePhysical(operator: OperatorNode.Physical, ctx: QueryContext): List<OperatorNode.Physical> {
        /* List of candidates, objects to explore and digests */
        val candidates = MemoizingOperatorList(operator.root)
        val explore = MemoizingOperatorList(operator.root)

        /* Now start exploring... */
        var pointer = explore.dequeue()
        while (pointer != null) {
            /* Apply rules to node and add results to list for exploration. */
            for (rule in this.physicalRules) {
                val result = rule.apply(pointer, ctx)
                if (result is OperatorNode.Physical) {
                    explore.enqueue(result)
                    candidates.enqueue(result)
                }
            }

            /* Add all inputs to operators that need further exploration. */
            when (pointer) {
                is NAryPhysicalOperatorNode -> {
                    pointer.inputs.forEach { explore.enqueue(it, force = true) }
                }
                is BinaryPhysicalOperatorNode -> {
                    explore.enqueue(pointer.left, force = true)
                    explore.enqueue(pointer.right, force = true)
                }
                is UnaryPhysicalOperatorNode -> {
                    explore.enqueue(pointer.input, force = true)
                }
                is OperatorNode.Logical -> throw IllegalStateException("Encountered logical operator node in physical operator node tree. This is a programmer's error!")
            }

            /* Get next in line. */
            pointer = explore.dequeue()
        }
        return candidates.toList()
    }
}