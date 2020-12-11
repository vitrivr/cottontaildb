package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.LongValue

/**
 * An [AbstractEntityOperator] that counts the number of entries in an [Entity] and returns one [Record] with that number.
 *
 * @author Ralph Gasser
 * @version 1.1.2
 */
class EntityCountOperator(entity: Entity) : AbstractEntityOperator(entity, arrayOf(entity.allColumns().first())) {

    /** The [ColumnDef] returned by this [EntitySampleOperator]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(ColumnDef.withAttributes(entity.name.column("count()"), "LONG"))

    /**
     * Converts this [EntityCountOperator] to a [Flow] and returns it.
     *
     * @param context The [ExecutionEngine.ExecutionContext] used for execution.
     * @return [Flow] representing this [EntityCountOperator]
     */
    override fun toFlow(context: ExecutionEngine.ExecutionContext): Flow<Record> = flow {
        emit(StandaloneRecord(0L, this@EntityCountOperator.columns, arrayOf(LongValue(context.getTx(entity).count()))))
    }
}