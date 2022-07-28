package org.vitrivr.cottontail.dbms.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [MergeOperator] merges the results of multiple incoming operators into a single [Flow].
 *
 * The incoming [Operator]s are executed in parallel, hence order of the [Record]s in the
 * outgoing [Flow] may be arbitrary.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */

class MergeOperator(parents: List<Operator>, override val context: QueryContext): Operator.MergingPipelineOperator(parents) {
    /** The columns produced by this [MergeOperator]. */
    override val columns: List<ColumnDef<*>>
        get() = this.parents.first().columns

    /** [MergeOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [MergeOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [MergeOperator]
     */
    override fun toFlow(): Flow<Record> = channelFlow {
        this@MergeOperator.parents.map { p ->
            launch {
                p.toFlow().collect {
                    send(it)
                }
            }
        }
    }
}