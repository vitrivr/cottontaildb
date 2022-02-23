package org.vitrivr.cottontail.dbms.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.onEach
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator

/**
 * A [MergeOperator] merges the results of multiple incoming operators into a single [Flow].
 *
 * The incoming [Operator]s are executed in parallel, hence order of the [Record]s in the
 * outgoing [Flow] may be arbitrary.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */

class MergeOperator(parents: List<Operator>, val context: BindingContext): Operator.MergingPipelineOperator(parents) {
    /** The columns produced by this [MergeOperator]. */
    override val columns: List<ColumnDef<*>>
        get() = this.parents.first().columns

    /** [MergeOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [MergeOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [MergeOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        /* Obtain parent flows amd compose new flow. */
        val parentFlows = this.parents.map { it.toFlow(context) }.toTypedArray()
        return merge(*parentFlows).onEach { this@MergeOperator.context.update(it) }
    }
}