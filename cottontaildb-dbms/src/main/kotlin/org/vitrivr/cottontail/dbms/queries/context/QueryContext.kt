package org.vitrivr.cottontail.dbms.queries.context

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.planning.cost.CostPolicy
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.queries.QueryHint
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.dbms.statistics.StatisticsManager
import org.vitrivr.cottontail.server.Instance

/**
 * A context for query binding, planning and execution. The [QueryContext] bundles all the
 * relevant aspects of a query such as  the logical and physical plans, [Transaction]
 * and [BindingContext]
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
interface QueryContext {

    /** An identifier for this [QueryContext]. */
    val queryId: String

    /** The [Catalogue] this [QueryContext] uses. */
    val catalogue: Catalogue

    /** The [StatisticsManager] this [QueryContext] uses. */
    val statistics: StatisticsManager

    /** The canonical [CatalogueTx] for this [QueryContext]. */
    val catalogueTx: CatalogueTx

    /** The [Transaction] the query held by this [QueryContext] is associated with. */
    val txn: Transaction

    /** The [BindingContext] exposed by this [QueryContext]. */
    val bindings: BindingContext

    /** Set of [QueryHint]s held by this [QueryContext]. These hints influence query planning. */
    val hints: Set<QueryHint>

    /** The [CostPolicy] that should be applied within this [QueryContext] */
    val costPolicy: CostPolicy

    /** The [OperatorNode.Logical] representing (sub-)queries held by this [QueryContext]. */
    val logical: List<OperatorNode.Logical>

    /** The [OperatorNode.Physical] representing the (sub-)queries held by this [QueryContext]. */
    val physical: List<OperatorNode.Physical>

    /** Output [ColumnDef] for the query held by this [QueryContext] (as per canonical plan). */
    val output: List<Binding.Column>

    /** Output order for the query held by this [QueryContext] (as per canonical plan). */
    val order: List<Pair<Binding.Column, SortOrder>>

    /**
     * Returns the next available [GroupId].
     *
     * @return Next available [GroupId].
     */
    fun nextGroupId(): GroupId

    /**
     * Registers a new [OperatorNode.Logical] to this [QueryContext]
     *
     * @param plan The [OperatorNode.Logical] to assign.
     */
    fun register(plan: OperatorNode.Logical)

    /**
     * Registers a new [OperatorNode.Physical] with this [QueryContext].
     *
     * @param plan The [OperatorNode.Physical] to assign.
     */
    fun register(plan: OperatorNode.Physical)

    /**
     * Starts the query planning processing using the given [CottontailQueryPlanner]. The query planning
     * process tries to generate a near-optimal [OperatorNode.Physical] from the registered [OperatorNode.Logical].
     *
     * @param planner The [CottontailQueryPlanner] instance to use for planning.
     * @param bypassCache Flag indicating, whether the [CottontailQueryPlanner] should bypass the plan cache.
     * @param cache Flag indicating, whether the resulting plan should be cached.
     */
    fun plan(planner: CottontailQueryPlanner, bypassCache: Boolean = false, cache: Boolean = false)

    /**
     * Converts the registered [OperatorNode.Logical] to the equivalent [OperatorNode.Physical] and skips query planning.
     */
    fun implement()

    /**
     * Splits this [QueryContext] into a subcontext.
     *
     * A subcontext usually shares the majority of properties with the parent [QueryContext],
     * but uses its dedicated [BindingContext]. This is mainly used to allow for parallelisation.
     */
    fun split(): QueryContext

    /**
     * Implements the query held by this [QueryContext]. Requires a functional, [OperatorNode.Physical]
     *
     * @return [Operator]
     */
    fun toOperatorTree(): Operator
}