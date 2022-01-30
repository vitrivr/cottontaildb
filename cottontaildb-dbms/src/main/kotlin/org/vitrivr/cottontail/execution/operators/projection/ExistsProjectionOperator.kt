package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.queries.projection.Projection
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.AbortFlowException
import org.vitrivr.cottontail.execution.operators.basics.Operator

/**
 * An [Operator.PipelineOperator] used during query execution. It returns a single [Record] containing
 * either true or false depending on whether there are incoming [Record]s.
 *
 * Only produces a single [Record]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
class ExistsProjectionOperator(parent: Operator, val out: Binding.Column) : Operator.PipelineOperator(parent) {

    /** Column returned by [ExistsProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = listOf(ColumnDef(Name.ColumnName(Projection.EXISTS.label()), Types.Boolean))

    /** [ExistsProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /**
     * Converts this [ExistsProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [ExistsProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        return flow {
            var exists = false
            try {
                parentFlow.collect {
                    exists = true
                    throw AbortFlowException(this)
                }
            } catch (e: AbortFlowException) {
                e.checkOwnership(this)
            }
            this@ExistsProjectionOperator.out.update(BooleanValue(exists))
            emit(StandaloneRecord(0L, this@ExistsProjectionOperator.columns[0], BooleanValue(exists)))
        }
    }
}