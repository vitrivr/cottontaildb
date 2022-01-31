package org.vitrivr.cottontail.dbms.queries

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.binding.DefaultBindingContext
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A context for query binding and planning. Tracks logical and physical query plans, enables late binding of [Value]s
 * and isolates different strands of execution within a query from one another.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class QueryContext(val queryId: String, val catalogue: Catalogue, val txn: org.vitrivr.cottontail.dbms.execution.TransactionManager.TransactionImpl) {

    /** List of bound [Value]s for this [QueryContext]. */
    val bindings: BindingContext = DefaultBindingContext()

    /** The individual [OperatorNode.Logical], each representing different sub-queries. */
    private val nodes: MutableMap<GroupId, org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical> = Int2ObjectOpenHashMap()

    /** The [OperatorNode.Logical] representing the query and the sub-queries held by this [QueryContext]. */
    val logical: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical?
        get() = this.nodes[0]

    /** The [OperatorNode.Physical] representing the query and the sub-queries held by this [QueryContext]. */
    var physical: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Physical? = null
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
    fun register(plan: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical) {
        this.nodes[plan.groupId] = plan
    }

    /**
     * Returns the [OperatorNode.Logical] for the given [GroupId].
     *
     * @param groupId The [GroupId] to return an [OperatorNode.Logical] for.
     * @return [OperatorNode.Logical]
     */
    operator fun get(groupId: GroupId): org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical = this.nodes[groupId] ?: throw QueryException.QueryPlannerException("Failed to access sub-query with groupId $groupId")

    /**
     * Returns an executable [Operator] for this [QueryContext]. Requires a functional, [OperatorNode.Physical]
     *
     * @return [Operator]
     */
    fun toOperatorTree(): Operator {
        val local = this.physical
        check(local != null) { IllegalStateException("Cannot generate an operator tree without a valid, physical node expression tree.") }
        val parallelisation = local.totalCost.parallelisation()
        if (local.totalCost.parallelisation(parallelisation) > 1) {
            val partitioned = local.tryPartition(parallelisation) /* TODO: Query hint. */
            if (partitioned != null) {
                partitioned.bind(context = this.bindings)
                return partitioned.toOperator(this)
            }
        }
        return local.toOperator(this)
    }
}