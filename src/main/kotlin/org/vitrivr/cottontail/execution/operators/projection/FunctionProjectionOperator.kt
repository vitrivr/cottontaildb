package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.Binding
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
 * @author Ralph Gasser
 * @version 1.0.1
 */
class FunctionProjectionOperator(parent: Operator, val function: Function<*>, val arguments: List<Binding>, alias: Name.ColumnName? = null) : Operator.PipelineOperator(parent) {

    /** The column produced by this [FunctionProjectionOperator] is determined by the [Function]'s signature. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        *this.parent.columns,
        ColumnDef(alias ?: Name.ColumnName(function.signature.name), this.function.signature.returnType!!)
    )

    /** The [DistanceProjectionOperator] is not a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [FunctionProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [QueryContext] used for execution
     * @return [Flow] representing this [FunctionProjectionOperator]
     */
    override fun toFlow(context: QueryContext): Flow<Record> {
        /* Obtain parent flow. */
        val parentFlow = this.parent.toFlow(context)

        /* Prepare arguments for function. */
        val columnRefs = mutableListOf<Pair<Int,ColumnDef<*>>>()
        val arguments = Array(this@FunctionProjectionOperator.columns.size) {
            when(val ref = this.arguments[it]) {
                is Binding.Literal -> ref.value
                is Binding.Column -> {
                    columnRefs.add(Pair(it, ref.column))
                    null
                }
            }
        }

        /* Prepare empty array that acts a holder for values. */
        val values = Array<Value?>(this@FunctionProjectionOperator.columns.size) { null }

        return parentFlow.map { record ->
            /* Update arguments based on incoming record. */
            columnRefs.forEach { ref ->
                arguments[ref.first] = record[ref.second]
            }

            /* Update values. */
            var i = 0
            record.forEach { _, v -> values[i++] = v }
            values[values.lastIndex] = this@FunctionProjectionOperator.function(*arguments)

            /* Generate and return record. */
            StandaloneRecord(record.tupleId, this.columns, values)
        }
    }
}