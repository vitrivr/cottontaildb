package org.vitrivr.cottontail.execution.operators.function

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [Operator.PipelineOperator] used during query execution. It executes a defined [Function] and generates a new [ColumnDef] from its results
 *
 * Used to execute outer functions whose result is manifested in a [Record]
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class FunctionProjectionOperator(parent: Operator, val function: Function<*>, val arguments: List<Binding>, override val binding: BindingContext, alias: Name.ColumnName? = null) : Operator.PipelineOperator(parent) {

    /** The column produced by this [FunctionProjectionOperator] is determined by the [Function]'s signature. */
    override val columns: List<ColumnDef<*>> = this.parent.columns + ColumnDef(
        name = alias ?: Name.ColumnName(function.signature.name.simple),
        type = this.function.signature.returnType!!,
        nullable = false
    )

    /** The [DistanceProjectionOperator] is not a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [FunctionProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [QueryContext] used for execution
     * @return [Flow] representing this [FunctionProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        /* Obtain parent flow. */
        val parentFlow = this.parent.toFlow(context)

        /* Prepare arguments for function. */
        val dynamicArgs = mutableListOf<Pair<Int,Binding>>()
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
            dynamicArgs.forEach { this@FunctionProjectionOperator.function.provide(it.first, it.second.value) }

            /* Copy record and append function call. */
            var i = 0
            record.forEach { _, v -> values[i++] = v }
            values[values.lastIndex] = this@FunctionProjectionOperator.function()

            /* Generate and return record. Important: Make new record available to binding context. */
            val rec = StandaloneRecord(record.tupleId, columns, values)
            this@FunctionProjectionOperator.binding.bindRecord(rec)
            rec
        }
    }
}