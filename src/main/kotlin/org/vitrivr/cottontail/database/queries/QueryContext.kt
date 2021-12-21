package org.vitrivr.cottontail.database.queries

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.binding.BindingContext

import org.vitrivr.cottontail.database.queries.binding.DefaultBindingContext
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.Transaction
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionStatus
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A context for query binding and planning. Tracks logical and physical query plans, enables late binding of [Value]s
 * and isolates different strands of execution within a query from one another.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class QueryContext(val queryId: String, val catalogue: Catalogue, val txn: TransactionManager.TransactionImpl) {

    /** List of bound [Value]s for this [QueryContext]. */
    val bindings: BindingContext = DefaultBindingContext()

    /** The individual [OperatorNode.Logical], each representing different sub-queries. */
    private val nodes: MutableMap<GroupId, OperatorNode.Logical> = Int2ObjectOpenHashMap()

    /** The [OperatorNode.Logical] representing the query and the sub-queries held by this [QueryContext]. */
    val logical: OperatorNode.Logical?
        get() = this.nodes[0]

    /** The [OperatorNode.Physical] representing the query and the sub-queries held by this [QueryContext]. */
    var physical: OperatorNode.Physical? = null
        internal set

    /** Output [ColumnDef] for the query held by this [QueryContext] (as per canonical plan). */
    val output: List<ColumnDef<*>>?
        get() = this.nodes[0]?.columns

    /** Output order for the query held by this [QueryContext] (as per canonical plan). */
    val order: List<Pair<ColumnDef<*>, SortOrder>>?
        get() = this.nodes[0]?.sortOn

    @Volatile
    private var groupIdCounter: GroupId = 0

    /**
     * Returns the next available [GroupId].
     *
     * @return
     */
    fun nextGroupId(): GroupId = this.groupIdCounter++

    /**
     * Registers an [OperatorNode.Logical] with this [QueryContext] and assigns a new [GroupId] for it.
     */
    fun register(plan: OperatorNode.Logical) {
        this.nodes[plan.groupId] = plan
    }

    /**
     * Returns the [OperatorNode.Logical] for the given [GroupId].
     *
     * @param groupId The [GroupId] to return an [OperatorNode.Logical] for.
     * @return [OperatorNode.Logical]
     */
    operator fun get(groupId: GroupId): OperatorNode.Logical = this.nodes[groupId] ?: throw QueryException.QueryPlannerException("Failed to access sub-query with groupId $groupId")

    /**
     * Returns an executable [Operator] for this [QueryContext]. Requires a functional, [OperatorNode.Physical]
     */
    fun toOperatorTree(): Operator {
        val local = this.physical
        check(local != null) { IllegalStateException("Cannot generate an operator tree without a valid, physical node expression tree.") }
        return local.toOperator(this)
    }
}