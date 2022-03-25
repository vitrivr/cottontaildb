package org.vitrivr.cottontail.dbms.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext

/**
 * An [Operator.PipelineOperator] used during query execution. It generates new [Record]s for
 * each incoming [Record] and removes field not required by the query.
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class SelectProjectionOperator(parent: Operator, fields: List<Name.ColumnName>) : Operator.PipelineOperator(parent) {

    /** Columns produced by [SelectProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = fields.map { f ->
        this.parent.columns.single { c -> c.name == f }
    }

    /** [SelectProjectionOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [SelectProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [SelectProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val columns = this.columns.toTypedArray()
        val values = arrayOfNulls<Value?>(columns.size)
        return this.parent.toFlow(context).map { r ->
            columns.forEachIndexed { i, c -> values[i] = r[c]  }
            StandaloneRecord(r.tupleId, columns, values)
        }
    }
}