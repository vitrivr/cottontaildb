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
 * @version 1.2.0
 */
class FunctionProjectionOperator(parent: Operator, val function: Function<*>, val arguments: List<Binding>, alias: Name.ColumnName? = null) : Operator.PipelineOperator(parent) {

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
        val arguments = mutableListOf<Binding>()
        if (this.function is Function.Stateful) {
            val prepArgs = mutableListOf<Value?>()
            this.arguments.forEachIndexed { i, b ->
                if (this.function.statefulArguments.contains(i)) {
                    require(b is Binding.Literal) { "Only literal arguments can be used as stateful arguments for a stateful function. " }
                    prepArgs.add(b.value)
                } else {
                    arguments.add(b)
                }
            }
            this.function.prepare(*prepArgs.toTypedArray())
        } else {
            this.arguments.forEach{ b -> arguments.add(b) }
        }

        /* Prepare empty array that acts a holder for values. */
        val argumentValues = Array<Value?>(arguments.size) { null }
        val columns = this.columns.toTypedArray()
        val values = Array<Value?>(this.columns.size) { null }

        return parentFlow.map { record ->
            /* Update arguments based on incoming record. */
            arguments.forEachIndexed { i, b -> argumentValues[i] = b.value }

            /* Update values. */
            var i = 0
            record.forEach { _, v -> values[i++] = v }
            values[values.lastIndex] = this@FunctionProjectionOperator.function(*argumentValues)

            /* Generate and return record. */
            StandaloneRecord(record.tupleId, columns, values)
        }
    }
}