package org.vitrivr.cottontail.execution.operators.function

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A [Operator.PipelineOperator] used during query execution. It executes a defined [Function] and generates [Binding] from its result.
 *
 * Used to execute nested functions whose result is not manifested in a [Record]
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class NestedFunctionOperator(parent: Operator, val function: Function<*>, val out: Binding.Literal, val arguments: List<Binding>) : Operator.PipelineOperator(parent) {

    /** The [NestedFunctionOperator] is not a pipeline breaker. */
    override val breaker: Boolean = false

    /** The [NestedFunctionOperator] does not produce additional columns. */
    override val columns: List<ColumnDef<*>>
        get() = this.parent.columns

    /**
     * Converts this [FunctionOperator] to a [Flow] and returns it.
     *
     * @param context The [QueryContext] used for execution
     * @return [Flow] representing this [FunctionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        val arguments = Array<Value?>(this.arguments.size) { this.arguments[it].type.defaultValue() }
        return parentFlow.onEach { _ ->
            /* Load arguments into array. */
            for ((argIdx, arg) in this@NestedFunctionOperator.arguments.withIndex()) {
                arguments[argIdx] = arg.value
            }
            this@NestedFunctionOperator.out.update(this@NestedFunctionOperator.function(*arguments))
        }
    }
}