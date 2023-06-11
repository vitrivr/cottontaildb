package org.vitrivr.cottontail.dbms.queries.context

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.OrderTrait
import org.vitrivr.cottontail.core.queries.planning.cost.CostPolicy
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.queries.QueryHint
import org.vitrivr.cottontail.dbms.queries.binding.DefaultBindingContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.CottontailQueryPlanner
import java.util.*

/**
 * A context for query binding and planning. Tracks logical and physical query plans, enables late binding of [Value]s
 * and isolates different strands of execution within a query from one another.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DefaultQueryContext(override val queryId: String, override val catalogue: Catalogue, override val txn: Transaction, override val hints: Set<QueryHint> = emptySet()): QueryContext {

    /** List of bound [Value]s for this [DefaultQueryContext]. */
    override val bindings: BindingContext = DefaultBindingContext()

    /** The [OperatorNode.Logical] representing the query and the sub-queries held by this [DefaultQueryContext]. */
    override val logical: List<OperatorNode.Logical> = LinkedList()

    /** The [OperatorNode.Physical] representing the query and the sub-queries held by this [DefaultQueryContext]. */
    override val physical: List<OperatorNode.Physical>  = LinkedList()

    /** Output [ColumnDef] for the query held by this [DefaultQueryContext] (as per canonical plan). */
    override val output: List<ColumnDef<*>>
        get() = this.logical.first().columns

    /** Output order for the query held by this [DefaultQueryContext] (as per canonical plan). */
    override val order: List<Pair<ColumnDef<*>, SortOrder>>
        get() = this.logical.first()[OrderTrait]?.order ?: emptyList()

    /** [CostPolicy] is derived from [QueryHint] or global setting in that order. */
    override val costPolicy: CostPolicy = this.hints.filterIsInstance(QueryHint.CostPolicy::class.java).singleOrNull() ?: this.catalogue.config.cost

    /** Internal counter used to obtain the next [GroupId]. */
    @Volatile
    private var groupIdCounter: GroupId = 0

    /**
     * Returns the next available [GroupId].
     *
     * @return Next available [GroupId].
     */
    override fun nextGroupId(): GroupId = this.groupIdCounter++

    /**
     * Registers a new [OperatorNode.Logical] to this [QueryContext]
     *
     * Invalidates all existing [OperatorNode.Physical] held by this [QueryContext].
     *
     * @param plan The [OperatorNode.Logical] to assign.
     */
    override fun register(plan: OperatorNode.Logical) {
        (this.logical as LinkedList).add(plan)
        (this.physical as LinkedList).clear()
    }

    /**
     * Assigns a new [OperatorNode.Physical] to this [QueryContext] overwriting the existing [OperatorNode.Physical].
     * This can be used to bypass query planning and/or implementation steps
     *
     * Invalidates all existing [OperatorNode.Logical] and [OperatorNode.Physical] held by this [QueryContext].
     *
     * @param plan The [OperatorNode.Logical] to assign.
     */
    override fun register(plan: OperatorNode.Physical) {
        (this.physical as LinkedList).add(plan)
    }

    /**
     * Starts the query planning processing using the given [CottontailQueryPlanner]. The query planning
     * process tries to generate a near-optimal [OperatorNode.Physical] from the registered [OperatorNode.Logical].
     *
     * @param planner The [CottontailQueryPlanner] instance to use for planning.
     * @param bypassCache Flag indicating, whether the [CottontailQueryPlanner] should bypass the plan cache.
     * @param cache Flag indicating, whether the resulting plan should be cached.
     */
    override fun plan(planner: CottontailQueryPlanner, bypassCache: Boolean, cache: Boolean) {
        (this.physical as LinkedList).clear()
        with (this) {
            for (l in this.logical) {
                this@DefaultQueryContext.physical.add(planner.planAndSelect(l, bypassCache, cache))
            }
        }
    }

    /**
     * Converts the registered [OperatorNode.Logical] to the equivalent [OperatorNode.Physical] and skips query planning.
     */
    override fun implement() {
        (this.physical as LinkedList).clear()
        for (l in this.logical) {
            this.physical.add(l.implement())
        }
    }

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
        when (this.physical.size) {
            0 -> throw IllegalStateException("Cannot generate an operator tree without a valid, physical node expression tree.")
            1 -> { /* Case: Simple query (no sub-queries). */
                val local = this.physical.first()
                val maxParallelism = this.hints.filterIsInstance<QueryHint.Parallelism>().firstOrNull()?.max?.coerceAtMost(this.txn.availableIntraQueryWorkers) ?: this.txn.availableIntraQueryWorkers
                if (maxParallelism > 1) {
                    val partitioned = local.tryPartition(this, maxParallelism)?.root
                    if (partitioned != null) {
                        return partitioned.toOperator(this)
                    }
                }
                return local.toOperator(this)
            }
            else -> { /* Case: Complex query (with sub-queries). */
                TODO()
            }
        }
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
        override val logical: List<OperatorNode.Logical>
            get() = this@DefaultQueryContext.logical
        override val physical: List<OperatorNode.Physical>
            get() = this@DefaultQueryContext.physical
        override val output: List<ColumnDef<*>>
            get() = this@DefaultQueryContext.output
        override val order: List<Pair<ColumnDef<*>, SortOrder>>
            get() = this@DefaultQueryContext.order
        override fun nextGroupId(): GroupId = this@DefaultQueryContext.nextGroupId()
        override fun register(plan: OperatorNode.Logical) = this@DefaultQueryContext.register(plan)
        override fun register(plan: OperatorNode.Physical) = this@DefaultQueryContext.register(plan)
        override fun plan(planner: CottontailQueryPlanner, bypassCache: Boolean, cache: Boolean) = this@DefaultQueryContext.plan(planner, bypassCache, cache)
        override fun implement() = this@DefaultQueryContext.implement()
        override fun split(): QueryContext = this@DefaultQueryContext.split()
        override fun toOperatorTree(): Operator = this@DefaultQueryContext.toOperatorTree()
    }
}