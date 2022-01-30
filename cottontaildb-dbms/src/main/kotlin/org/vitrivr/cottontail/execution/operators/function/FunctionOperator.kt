package org.vitrivr.cottontail.execution.operators.function

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator

/**
 * A [Operator.PipelineOperator] used during query execution. It executes a defined [Function] and generates a new [ColumnDef] from its results
 *
 * Used to execute outer functions whose result is manifested in a [Record]
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class FunctionOperator(parent: Operator, val function: Function<*>, val out: Binding.Column, val arguments: List<Binding>) : Operator.PipelineOperator(parent) {

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
    override fun toFlow(context: TransactionContext): Flow<Record> {
        /* Obtain parent flow. */
        val parentFlow = this.parent.toFlow(context)

        /* Prepare empty array that acts a holder for values. */
        val columns = this.columns.toTypedArray()
        val values = Array<Value?>(this.columns.size) { null }
        val arguments = Array<Value?>(this.arguments.size) { this.arguments[it].type.defaultValue() }
        return parentFlow.map { record ->
            /* Copy values of incoming record. */
            for (i in 0 until record.size) {
                values[i] = record[i]
            }

            /* Load arguments into array and execute function. */
            for ((argIdx, arg) in this@FunctionOperator.arguments.withIndex()) {
                arguments[argIdx] = arg.value
            }
            values[values.lastIndex] = this@FunctionOperator.function(*arguments)
            this@FunctionOperator.out.update(values[values.lastIndex])

            /* Generate and return record. Important: Make new record available to binding context. */
            StandaloneRecord(record.tupleId, columns, values)
        }
    }
}