package org.vitrivr.cottontail.dbms.queries.context

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.QueryHint
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.OrderTrait
import org.vitrivr.cottontail.core.queries.planning.cost.CostPolicy
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.queries.binding.DefaultBindingContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.CottontailQueryPlanner

/**
 * A context for query binding and planning. Tracks logical and physical query plans, enables late binding of [Value]s
 * and isolates different strands of execution within a query from one another.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class DefaultQueryContext(override val queryId: String, override val catalogue: Catalogue, override val txn: Transaction, override val hints: Set<QueryHint> = emptySet()): QueryContext {

    /** List of bound [Value]s for this [DefaultQueryContext]. */
    override val bindings: BindingContext = DefaultBindingContext()

    /** The individual [OperatorNode.Logical], each representing different sub-queries. */
    private val nodes: MutableMap<GroupId, OperatorNode.Logical> = Int2ObjectOpenHashMap()

    /** The [OperatorNode.Logical] representing the query and the sub-queries held by this [DefaultQueryContext]. */
    override val logical: OperatorNode.Logical?
        get() = this.nodes[0]

    /** The [OperatorNode.Physical] representing the query and the sub-queries held by this [DefaultQueryContext]. */
    override var physical: OperatorNode.Physical? = null
        private set

    /** Output [ColumnDef] for the query held by this [DefaultQueryContext] (as per canonical plan). */
    override val output: List<ColumnDef<*>>?
        get() = this.nodes[0]?.columns

    /** Output order for the query held by this [DefaultQueryContext] (as per canonical plan). */
    override val order: List<Pair<ColumnDef<*>, SortOrder>>
        get() = this.nodes[0]?.get(OrderTrait)?.order ?: emptyList()

    /** [CostPolicy] is derived from [QueryHint] or global setting in that order. */
    override val costPolicy: CostPolicy = this.hints.filterIsInstance(QueryHint.CostPolicy::class.java).singleOrNull() ?: this.catalogue.config.cost

    @Volatile
    private var groupIdCounter: GroupId = 0

    /**
     * Returns the next available [GroupId].
     *
     * @return
     */
    fun nextGroupId(): GroupId = this.groupIdCounter++

    /**
     * Registers an [OperatorNode.Logical] with this [DefaultQueryContext] and assigns a new [GroupId] for it.
     */
    fun register(plan: OperatorNode.Logical) {
        this.nodes[plan.groupId] = plan
    }

    /**
     * Starts the query planning processing using the given [CottontailQueryPlanner]. The query planning
     * process tries to generate a near-optimal [OperatorNode.Physical] from the registered [OperatorNode.Logical].
     *
     * @param planner The [CottontailQueryPlanner] instance to use for planning.
     */
    override fun plan(planner: CottontailQueryPlanner) {
        this.physical = planner.planAndSelect(this)
    }

    /**
     * Converts the registered [OperatorNode.Logical] to the equivalent [OperatorNode.Physical] and skips query planning.
     */
    override fun implement() {
        this.physical = this.logical?.implement()
    }

    /**
     * Returns the [OperatorNode.Logical] for the given [GroupId].
     *
     * @param groupId The [GroupId] to return an [OperatorNode.Logical] for.
     * @return [OperatorNode.Logical]
     */
    operator fun get(groupId: GroupId): OperatorNode.Logical = this.nodes[groupId] ?: throw QueryException.QueryPlannerException("Failed to access sub-query with groupId $groupId")

    /**
     * Creates a [Subcontext] for this [DefaultQueryContext].
     *
     * @return [Subcontext] of this [DefaultQueryContext].
     */
    override fun split(): QueryContext = Subcontext()

    /**
     * Returns an executable [Operator] for this [DefaultQueryContext]. Requires a functional, [OperatorNode.Physical]
     *
     * @return [Operator]
     */
    override fun toOperatorTree(): Operator {
        val local = this.physical
        check(local != null) { IllegalStateException("Cannot generate an operator tree without a valid, physical node expression tree.") }
        if (!this.hints.contains(QueryHint.NoParallel)) {
            val availableWorkers = this.txn.availableIntraQueryWorkers
            val iqp = this.costPolicy.parallelisation(local.parallelizableCost, local.totalCost, availableWorkers)
            if (iqp > 1) {
                val partitioned = local.tryPartition(iqp)
                if (partitioned != null) {
                    return partitioned.toOperator(this)
                }
            }
        }
        return local.toOperator(this)
    }

    /**
     * A [QueryContext] that is a [Subcontext] of the outer [DefaultQueryContext].
     */
    private inner class Subcontext: QueryContext{
        /** A [Subcontext] has its own copy of the [BindingContext]. */
        override val bindings: BindingContext = this@DefaultQueryContext.bindings.copy()
        override val queryId: String
            get() = this@DefaultQueryContext.queryId
        override val catalogue: Catalogue
            get() = this@DefaultQueryContext.catalogue
        override val txn: Transaction
            get() = this@DefaultQueryContext.txn
        override val hints: Set<QueryHint>
            get() = this@DefaultQueryContext.hints
        override val costPolicy: CostPolicy
            get() = this@DefaultQueryContext.costPolicy
        override val logical: OperatorNode.Logical?
            get() = this@DefaultQueryContext.logical
        override val physical: OperatorNode.Physical?
            get() = this@DefaultQueryContext.physical
        override val output: List<ColumnDef<*>>?
            get() = this@DefaultQueryContext.output
        override val order: List<Pair<ColumnDef<*>, SortOrder>>
            get() = this@DefaultQueryContext.order
        override fun plan(planner: CottontailQueryPlanner) = this@DefaultQueryContext.plan(planner)
        override fun implement() = this@DefaultQueryContext.implement()
        override fun split(): QueryContext = this@DefaultQueryContext.split()
        override fun toOperatorTree(): Operator = this@DefaultQueryContext.toOperatorTree()
    }
}