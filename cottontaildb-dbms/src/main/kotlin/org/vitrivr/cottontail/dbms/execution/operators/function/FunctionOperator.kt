package org.vitrivr.cottontail.dbms.execution.operators.function

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [Operator.PipelineOperator] used during query execution. It executes a defined [Function] and generates a new [ColumnDef] from its results.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class FunctionOperator(parent: Operator, private val function: Binding.Function, private val out: Binding.Column, override val context: QueryContext) : Operator.PipelineOperator(parent) {

    /** The column produced by this [FunctionOperator] is determined by the [Function]'s signature. */
    override val columns: List<ColumnDef<*>> = this.parent.columns + this.out.column

    /** The [FunctionOperator] is not a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [FunctionOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [FunctionOperator]
     */
    override fun toFlow(): Flow<Tuple> = flow {
        val outColumns = this@FunctionOperator.columns.toTypedArray()
        val incoming = this@FunctionOperator.parent.toFlow()
        with(this@FunctionOperator.context.bindings) {
            incoming.collect { record ->
                with(record) {
                    /* Materialize new values array. */
                    val outValues = Array(outColumns.size) {
                        if (it < outColumns.size - 1) {
                            this[it]
                        } else {
                            this@FunctionOperator.function.getValue()
                        }
                    }
                    emit(StandaloneTuple(record.tupleId, outColumns, outValues))
                }
            }
        }
    }
}