package org.vitrivr.cottontail.dbms.execution.operators.function

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.QueryContext

/**
 * A [Operator.PipelineOperator] used during query execution. It executes a defined [Function] and generates a new [ColumnDef] from its results.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class FunctionOperator(parent: Operator, val function: Binding.Function, val out: Binding.Column) : Operator.PipelineOperator(parent) {

    /** The column produced by this [FunctionOperator] is determined by the [Function]'s signature. */
    override val columns: List<ColumnDef<*>> = this.parent.columns + this.out.column

    /** The [FunctionOperator] is not a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [FunctionOperator] to a [Flow] and returns it.
     *
     * @param context The [QueryContext] used for execution
     * @return [Flow] representing this [FunctionOperator]
     */
    override fun toFlow(context: org.vitrivr.cottontail.dbms.execution.TransactionContext): Flow<Record> {
        /* Obtain parent flow. */
        val parentFlow = this.parent.toFlow(context)

        /* Prepare empty array that acts a holder for values. */
        val columns = this.columns.toTypedArray()
        val values = Array<Value?>(this.columns.size) { null }
        return parentFlow.map { record ->
            /* Copy values of incoming record. */
            for (i in 0 until record.size) values[i] = record[i]

            /* Execute function and update column value. */
            values[values.lastIndex] = this@FunctionOperator.function.value

            /* Generate and return record. Important: Make new record available to binding context. */
            val rec = StandaloneRecord(record.tupleId, columns, values)
            this.function.context.update(rec)
            rec
        }
    }
}