package org.vitrivr.cottontail.execution.operators.projection

import org.vitrivr.cottontail.database.column.*
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.exceptions.OperatorExecutionException
import org.vitrivr.cottontail.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.PipelineBreaker
import org.vitrivr.cottontail.execution.operators.basics.ProducingOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.*
import java.util.concurrent.Callable
import kotlin.math.min

/**
 * An [Operator.PipelineBreaker] used during query execution. It tracks the minimum value it has
 * encountered so far  and returns it as a [Record]. The [MinProjectionOperator] retains the type
 * of the incoming records.
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class MinProjectionOperator(parent: ProducingOperator, context: ExecutionEngine.ExecutionContext, val column: ColumnDef<*>) : PipelineBreaker(parent, context) {

    override val columns: Array<ColumnDef<*>> = when (this.column.type) {
        ByteColumnType -> arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("max(${column.name})")
                ?: Name.ColumnName("max(${column.name})"), "BYTE"))
        ShortColumnType -> arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("max(${column.name})")
                ?: Name.ColumnName("max(${column.name})"), "SHORT"))
        IntColumnType -> arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("max(${column.name})")
                ?: Name.ColumnName("max(${column.name})"), "INT"))
        LongColumnType -> arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("max(${column.name})")
                ?: Name.ColumnName("max(${column.name})"), "LONG"))
        FloatColumnType -> arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("max(${column.name})")
                ?: Name.ColumnName("max(${column.name})"), "FLOAT"))
        DoubleColumnType -> arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("max(${column.name})")
                ?: Name.ColumnName("max(${column.name})"), "DOUBLE"))
        else -> throw OperatorSetupException("The provided column $column type cannot be used for a MIN projection. ")
    }

    override fun prepareOpen() {}

    override fun incomingOperator() = Callable {
        /* Determine maximum. */
        var min = Double.MAX_VALUE
        while (!this.parent.depleted) {
            val next = this.parent.next()
            if (next != null) {
                val value = next[this.column]
                when (value) {
                    is ByteValue -> min = min(min, value.value.toDouble())
                    is ShortValue -> min = min(min, value.value.toDouble())
                    is IntValue -> min = min(min, value.value.toDouble())
                    is LongValue -> min = min(min, value.value.toDouble())
                    is FloatValue -> min = min(min, value.value.toDouble())
                    is DoubleValue -> min = min(min, value.value)
                    else -> {
                    }
                }
            }
        }

        /* Generate recordset. */
        val recordset = Recordset(this.columns)
        when (this.column.type) {
            ByteColumnType -> recordset.addRowUnsafe(arrayOf(ByteValue(min)))
            ShortColumnType -> recordset.addRowUnsafe(arrayOf(ShortValue(min)))
            IntColumnType -> recordset.addRowUnsafe(arrayOf(IntValue(min)))
            LongColumnType -> recordset.addRowUnsafe(arrayOf(LongValue(min)))
            FloatColumnType -> recordset.addRowUnsafe(arrayOf(FloatValue(min)))
            DoubleColumnType -> recordset.addRowUnsafe(arrayOf(DoubleValue(min)))
            else -> throw OperatorExecutionException("The provided column $column cannot be used for a MIN projection. ")
        }
        recordset
    }
}