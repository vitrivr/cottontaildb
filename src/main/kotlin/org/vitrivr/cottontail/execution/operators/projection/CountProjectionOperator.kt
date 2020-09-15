package org.vitrivr.cottontail.execution.operators.projection

import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.PipelineBreaker
import org.vitrivr.cottontail.execution.operators.basics.ProducingOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.LongValue
import java.util.concurrent.Callable

/**
 * An [Operator.PipelineBreaker] used during query execution. It counts the number of rows it encounters
 * and returns the value as [Record].
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class CountProjectionOperator(parent: ProducingOperator, context: ExecutionEngine.ExecutionContext) : PipelineBreaker(parent, context) {

    override val columns: Array<ColumnDef<*>> = arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("count()")
            ?: Name.ColumnName("count()"), "LONG"))

    override fun prepareOpen() {}

    override fun incomingOperator() = Callable {
        var counter = 0L
        while (!this.parent.depleted) {
            if (parent.next() != null) {
                counter += 1
            }
        }
        val recordset = Recordset(this.columns)
        recordset.addRowUnsafe(arrayOf(LongValue(counter)))
        recordset
    }
}