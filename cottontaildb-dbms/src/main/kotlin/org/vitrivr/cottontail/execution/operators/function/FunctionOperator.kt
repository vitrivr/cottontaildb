package org.vitrivr.cottontail.execution.operators.function

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.functions.Function
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
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
class FunctionOperator(parent: Operator, val function: Function<*>, val arguments: List<Binding>, override val binding: BindingContext, val name: Name.ColumnName) : Operator.PipelineOperator(parent) {

    /** The column produced by this [FunctionOperator] is determined by the [Function]'s signature. */
    override val columns: List<ColumnDef<*>> = this.parent.columns + ColumnDef(
        name = name,
        type = this.function.signature.returnType!!,
        nullable = false
    )

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

        /* Prepare arguments for function. */
        val dynamicArgs = mutableListOf<Pair<Int, Binding>>()
        this.arguments.forEachIndexed {  i, b ->
            if (b.static) {
                this.function.provide(i, b.value)
            } else {
                dynamicArgs.add(i to b)
            }
        }

        /* Prepare empty array that acts a holder for values. */
        val columns = this.columns.toTypedArray()
        val values = Array<Value?>(this.columns.size) { null }
        return parentFlow.map { record ->
            /* Provide arguments. */
            dynamicArgs.forEach { this@FunctionOperator.function.provide(it.first, it.second.value) }

            /* Copy record and append function call. */
            var i = 0
            record.forEach { _, v -> values[i++] = v }
            values[values.lastIndex] = this@FunctionOperator.function()

            /* Generate and return record. Important: Make new record available to binding context. */
            val rec = StandaloneRecord(record.tupleId, columns, values)
            this@FunctionOperator.binding.bindRecord(rec)
            rec
        }
    }
}