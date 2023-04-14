package org.vitrivr.cottontail.dbms.execution.operators.management

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import java.util.ArrayList

/**
 * An [Operator.PipelineOperator] used during query execution. Inserts all incoming entries into an
 * [Entity] that it receives with the provided [Value].
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class InsertOperator(groupId: GroupId, val entity: EntityTx, val records: List<Record>) : Operator.SourceOperator(groupId) {
    companion object {
        /** The columns produced by the [InsertOperator]. */
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("tupleId"), Types.Long, false),
            ColumnDef(Name.ColumnName("duration_ms"), Types.Double, false)
        )
    }

    private val extendedColumns = mutableListOf<ColumnDef<*>>()

    /** Columns produced by [InsertOperator]. */
    override val columns: List<ColumnDef<*>>
        get() = extendedColumns

    init {
        extendedColumns.addAll(COLUMNS)
    }

    /**
     * Converts this [InsertOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [InsertOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        return flow {
            for (record in this@InsertOperator.records) {
                val start = System.currentTimeMillis()
                val tupleId = this@InsertOperator.entity.insert(record)
                val lastGenerated = this@InsertOperator.entity.lastGenerated()
                if (lastGenerated.isEmpty()) {
                    emit(
                        StandaloneRecord(
                            0L,
                            extendedColumns.toTypedArray(),
                            arrayOf(LongValue(tupleId), DoubleValue(System.currentTimeMillis() - start))
                        )
                    )
                } else {

                    val combinedValues = ArrayList<Value?>(COLUMNS.size + lastGenerated.size)
                    combinedValues.add(LongValue(tupleId))
                    combinedValues.add(DoubleValue(System.currentTimeMillis() - start))

                    lastGenerated.forEach {
                        extendedColumns.add(
                            ColumnDef(Name.ColumnName(it.first.name.columnName), it.first.type, it.first.nullable)
                        )
                        combinedValues.add(it.second)
                    }
                    emit(
                        StandaloneRecord(
                            0L,
                            extendedColumns.toTypedArray(),
                            combinedValues.toTypedArray()
                        )
                    )
                }
            }
        }
    }
}