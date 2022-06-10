package org.vitrivr.cottontail.data.importer

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.nio.file.Path

class CSVDataImporter(override val path: Path, override val schema: List<ColumnDef<*>>) : DataImporter {

    private val rows = csvReader().readAllWithHeader(path.toFile())
    private var rowIndex = 0

    /** The [Format] handled by this [DataImporter]. */
    override val format: Format = Format.CSV

    /** Indicator whether this [DataImporter] has been closed. */
    override var closed: Boolean = false
        private set

    override fun hasNext(): Boolean = this.rowIndex < rows.size

    override fun next(): Map<ColumnDef<*>, Value?> {
        val value = Object2ObjectArrayMap<ColumnDef<*>, Value?>(this.schema.size)
        val row = rows[rowIndex++]

        for (column in this.schema) {

            val csvValue = row[column.name.columnName]
            if (csvValue == null || csvValue.isEmpty()) {
                value[column] = null
            } else {
                value[column] = when (column.type) {
                    Types.Boolean -> BooleanValue(csvValue.toBoolean())
                    Types.Date -> DateValue(csvValue.toLong())
                    Types.Byte -> ByteValue(csvValue.toByte())
                    Types.Complex32 -> TODO()
                    Types.Complex64 -> TODO()
                    Types.Double -> DoubleValue(csvValue.toDouble())
                    Types.Float -> FloatValue(csvValue.toFloat())
                    Types.Int -> IntValue(csvValue.toInt())
                    Types.Long -> LongValue(csvValue.toLong())
                    Types.Short -> ShortValue(csvValue.toShort())
                    Types.String -> StringValue(csvValue)
                    is Types.BooleanVector -> TODO()
                    is Types.Complex32Vector -> TODO()
                    is Types.Complex64Vector -> TODO()
                    is Types.DoubleVector -> TODO()
                    is Types.FloatVector -> TODO()
                    is Types.IntVector -> TODO()
                    is Types.LongVector -> TODO()
                }
            }
        }

        return value
    }

    override fun close() {
        this.closed = true
    }

}