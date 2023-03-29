package org.vitrivr.cottontail.dbms.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.execution.operators.basics.AbortFlowException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * An [Operator.PipelineOperator] used during query execution. It returns a single [Record] containing
 * either true or false depending on whether there are incoming [Record]s.
 *
 * Only produces a single [Record]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class ExistsProjectionOperator(parent: Operator, val out: Binding.Column, override val context: QueryContext) : Operator.PipelineOperator(parent) {

    /** Column returned by [ExistsProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = listOf(ColumnDef(Name.ColumnName(Projection.EXISTS.column()), Types.Boolean))

    /** [ExistsProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /**
     * Converts this [ExistsProjectionOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [ExistsProjectionOperator]
     */
    override fun toFlow(): Flow<Record> = flow {
        val incoming = this@ExistsProjectionOperator.parent.toFlow()
        var exists = false
        try {
            incoming.collect {
                exists = true
                throw AbortFlowException(this)
            }
        } catch (e: AbortFlowException) {
            e.checkOwnership(this)
        }
        emit(StandaloneRecord(0L, this@ExistsProjectionOperator.columns[0], BooleanValue(exists)))
    }
}