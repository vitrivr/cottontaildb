package org.vitrivr.cottontail.execution.operators.merge

import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.MergingPipelineBreaker
import org.vitrivr.cottontail.execution.operators.predicates.KnnOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.Recordset
import java.util.concurrent.Future

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class KnnMergeOperator(parents: List<KnnOperator<*>>, context: ExecutionEngine.ExecutionContext) : MergingPipelineBreaker(parents, context) {

    override val depleted: Boolean
        get() = TODO("Not yet implemented")

    override val columns: Array<ColumnDef<*>>
        get() = TODO("Not yet implemented")

    override fun getNext(): Record? {
        TODO("Not yet implemented")
    }

    override fun executeIncomingOperators(): Future<List<Recordset>> = TODO()
}