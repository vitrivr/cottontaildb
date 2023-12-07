package org.vitrivr.cottontail.dbms.execution.operators

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.test.randomTuple
import java.util.random.RandomGenerator


/**
 * A [Operator.SourceOperator] that can be used to generate a flow of randomly generated [Tuple]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class RandomTupleSourceOperator(groupId: GroupId, val size: Int, private val generator: RandomGenerator, override val columns: List<ColumnDef<*>>, override val context: QueryContext) : Operator.SourceOperator(groupId) {
    override fun toFlow(): Flow<Tuple> = flow {
        val array = this@RandomTupleSourceOperator.columns.toTypedArray()
        var tupleId = 0L
        for (i in 0 until size) {
            emit(array.randomTuple(tupleId++, this@RandomTupleSourceOperator.generator))
        }
    }
}