package org.vitrivr.cottontail.data.importer

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.data.Format
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
                    Types.Complex32 -> Complex32Value(parseVector(csvValue){it.toFloat()}.toFloatArray())
                    Types.Complex64 -> Complex64Value(parseVector(csvValue){it.toDouble()}.toDoubleArray())
                    Types.Double -> DoubleValue(csvValue.toDouble())
                    Types.Float -> FloatValue(csvValue.toFloat())
                    Types.Int -> IntValue(csvValue.toInt())
                    Types.Long -> LongValue(csvValue.toLong())
                    Types.Short -> ShortValue(csvValue.toShort())
                    Types.String -> StringValue(csvValue)
                    is Types.BooleanVector -> BooleanVectorValue(parseVector(csvValue){it.toBoolean()}.toBooleanArray())
                    is Types.Complex32Vector -> Complex32VectorValue(parseVector(csvValue){element -> Complex32Value(parseVector(element){it.toFloat()}.toFloatArray()) }.toTypedArray())
                    is Types.Complex64Vector -> Complex64VectorValue(parseVector(csvValue){element -> Complex64Value(parseVector(element){it.toDouble()}.toDoubleArray())}.toTypedArray())
                    is Types.DoubleVector -> DoubleVectorValue(parseVector(csvValue){it.toDouble()})
                    is Types.FloatVector -> FloatVectorValue(parseVector(csvValue){it.toFloat()})
                    is Types.IntVector -> IntVectorValue(parseVector(csvValue){it.toInt()})
                    is Types.LongVector -> LongVectorValue(parseVector(csvValue){it.toLong()})
                    is Types.ByteString -> TODO()
                }
            }
        }

        return value
    }

    override fun close() {
        this.closed = true
    }

    private fun <T> parseVector(string: String, transform: (String) -> T) : List<T> =
        string.substringAfter('[').substringBeforeLast(']')
            .split(",").map { transform(it.trim()) }

}