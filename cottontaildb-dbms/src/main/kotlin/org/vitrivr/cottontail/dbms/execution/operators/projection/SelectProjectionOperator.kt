package org.vitrivr.cottontail.dbms.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [Operator.PipelineOperator] used during query execution. It generates new [Record]s for
 * each incoming [Record] and removes field not required by the query.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class SelectProjectionOperator(parent: Operator, fields: List<Name.ColumnName>, override val context: QueryContext) : Operator.PipelineOperator(parent) {

    /** Columns produced by [SelectProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = fields.map { f ->
        this.parent.columns.single { c -> c.name == f }
    }

    /** [SelectProjectionOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [SelectProjectionOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [SelectProjectionOperator]
     */
    override fun toFlow(): Flow<Record> = flow {
        val columns = this@SelectProjectionOperator.columns.toTypedArray()
        val incoming = this@SelectProjectionOperator.parent.toFlow()
        incoming.collect { r ->
            emit(StandaloneRecord(r.tupleId, columns, Array(columns.size) { r[columns[it]]}))
        }
    }
}