package org.vitrivr.cottontail.dbms.execution.operators.projection

import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnel
import com.google.common.hash.PrimitiveSink
import kotlinx.coroutines.flow.*
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import java.nio.charset.Charset

/**
 * An [Operator.PipelineOperator] used during query execution. It generates new [Record]s for
 * each incoming [Record] and removes / renames field according to the [fields] definition provided.
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class SelectDistinctProjectionOperator(parent: Operator, fields: List<Name.ColumnName>, expected: Long) : Operator.PipelineOperator(parent) {

    /** [Funnel] implementation for [Record]s. */
    class RecordFunnel : Funnel<org.vitrivr.cottontail.core.basics.Record> {
        override fun funnel(from: Record, into: PrimitiveSink) {
            for (i in 0 until from.columns.size) {
                when (val value = from[i]) {
                    is BooleanValue -> into.putBoolean(value.value)
                    is ByteValue -> into.putByte(value.value)
                    is ShortValue -> into.putShort(value.value)
                    is IntValue -> into.putInt(value.value)
                    is LongValue -> into.putLong(value.value)
                    is FloatValue -> into.putFloat(value.value)
                    is DoubleValue -> into.putDouble(value.value)
                    is DateValue -> into.putLong(value.value)
                    is Complex32Value -> {
                        into.putFloat(value.real.value)
                        into.putFloat(value.imaginary.value)
                    }
                    is Complex64Value -> {
                        into.putDouble(value.real.value)
                        into.putDouble(value.imaginary.value)
                    }
                    is StringValue -> into.putString(value.value, Charset.forName("UTF-8"))
                    is BooleanVectorValue -> value.data.forEach { into.putBoolean(it) }
                    is IntVectorValue -> value.data.forEach { into.putInt(it) }
                    is LongVectorValue -> value.data.forEach { into.putLong(it) }
                    is FloatVectorValue -> value.data.forEach { into.putFloat(it) }
                    is DoubleVectorValue -> value.data.forEach { into.putDouble(it) }
                    is Complex32VectorValue -> value.data.forEach { into.putFloat(it) }
                    is Complex64VectorValue -> value.data.forEach { into.putDouble(it) }
                    null -> into.putByte(Byte.MIN_VALUE)
                }
            }
        }
    }

    /** Columns produced by [SelectProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = this.parent.columns.filter { c -> fields.any { f -> f == c.name }}

    /** [SelectProjectionOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /** The [BloomFilter] used for SELECT DISTINCT. */
    private val bloomFilter = BloomFilter.create(RecordFunnel(), expected, 0.0001)

    /**
     * Converts this [SelectProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [SelectProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val columns = this.columns.toTypedArray()
        return this.parent.toFlow(context).mapNotNull { r ->
            val record = StandaloneRecord(r.tupleId, columns, Array(columns.size) { r[columns[it]]})
            if (!this.bloomFilter.mightContain(record)) {
                this.bloomFilter.put(record)
                record
            } else {
                null
            }
        }
    }
}