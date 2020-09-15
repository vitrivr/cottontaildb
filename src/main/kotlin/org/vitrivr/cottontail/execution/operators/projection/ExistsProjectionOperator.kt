package org.vitrivr.cottontail.execution.operators.projection

import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.PipelineBreaker
import org.vitrivr.cottontail.execution.operators.basics.ProducingOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.BooleanValue
import java.util.concurrent.Callable

/**
 * An [Operator.PipelineBreaker] used during query execution. It returns a single [Record] containing
 * either true or false depending on whether there are incoming [Record]s.
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ExistsProjectionOperator(parent: ProducingOperator, context: ExecutionEngine.ExecutionContext) : PipelineBreaker(parent, context) {

    override val columns: Array<ColumnDef<*>> = arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("exists()")
            ?: Name.ColumnName("exists()"), "BOOLEAN"))

    override fun prepareOpen() {}

    override fun incomingOperator() = Callable {
        var exists = false
        while (!this.parent.depleted) {
            if (parent.next() != null) {
                exists = true
                break
            }
        }
        val recordset = Recordset(this.columns)
        recordset.addRowUnsafe(arrayOf(BooleanValue(exists)))
        recordset
    }
}