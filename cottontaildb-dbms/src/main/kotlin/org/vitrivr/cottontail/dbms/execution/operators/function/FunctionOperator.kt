package org.vitrivr.cottontail.dbms.execution.operators.function

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.entity.values.StoredTuple
import org.vitrivr.cottontail.dbms.entity.values.StoredValue
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
    override fun toFlow(): Flow<Tuple> = with(this@FunctionOperator.context.bindings) {
        val outColumns = this@FunctionOperator.columns.toTypedArray()
        this@FunctionOperator.parent.toFlow().map { record ->
            with(record) {
                StoredTuple(record.tupleId, outColumns, Array(outColumns.size) {
                    if (it < outColumns.size - 1) {
                        (this as StoredTuple).values[it] as StoredValue
                    } else {
                        this@FunctionOperator.function.getValue()?.let { v -> StoredValue.Inline(v) }
                    }
                })
            }
        }
    }
}