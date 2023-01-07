package org.vitrivr.cottontail.data.exporter

import com.github.doyaaaaaken.kotlincsv.client.KotlinCsvExperimental
import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import org.apache.commons.codec.binary.Base64
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.data.importer.DataImporter
import java.nio.file.Path

class CSVDataExporter(override val path: Path) : DataExporter {

    @Suppress("EXPERIMENTAL_IS_NOT_ENABLED")
    @OptIn(KotlinCsvExperimental::class)
    private val writer = csvWriter().openAndGetRawWriter(path.toFile())

    /** The [Format] handled by this [DataImporter]. */
    override val format: Format = Format.CSV

    /** Indicator whether this [DataImporter] has been closed. */
    override var closed: Boolean = false
        private set

    private var header: List<String>? = null

    override fun offer(tuple: Tuple) {
        if (header == null) {
            header = (0 until tuple.size()).map { tuple.nameForIndex(it) }
            writer.writeRow(header)
        }
        val row = header!!.map { colName ->
            when(tuple.type(tuple.indexForName(colName))) {
                Type.BOOLEAN -> tuple.asBoolean(colName)
                Type.BYTE -> tuple.asByte(colName)
                Type.SHORT -> tuple.asShort(colName)
                Type.INTEGER -> tuple.asInt(colName)
                Type.LONG -> tuple.asLong(colName)
                Type.FLOAT -> tuple.asFloat(colName)
                Type.DOUBLE -> tuple.asDouble(colName)
                Type.DATE -> tuple.asDate(colName)
                Type.STRING -> tuple.asString(colName)
                Type.COMPLEX32,
                Type.COMPLEX64 -> encode((tuple[tuple.indexForName(colName)] as Pair<*, *>).toList())
                Type.DOUBLE_VECTOR -> encode(tuple.asDoubleVector(colName)?.toList())
                Type.FLOAT_VECTOR -> encode(tuple.asFloatVector(colName)?.toList())
                Type.LONG_VECTOR -> encode(tuple.asLongVector(colName)?.toList())
                Type.INTEGER_VECTOR -> encode(tuple.asIntVector(colName)?.toList())
                Type.BOOLEAN_VECTOR -> encode(tuple.asBooleanVector(colName)?.toList())
                Type.COMPLEX32_VECTOR,
                Type.COMPLEX64_VECTOR -> encode((tuple[tuple.indexForName(colName)] as Array<Pair<*,*>>).map { encode(it.toList()) })
                Type.BYTESTRING -> Base64.encodeBase64String(tuple[tuple.indexForName(colName)] as ByteArray)
                Type.UNDEFINED -> "undefined"
            }
        }
        writer.writeRow(row)
    }

    override fun close() {
        writer.close()
        this.closed = true
    }

    private fun encode(vector: List<Any?>?) : String = vector?.joinToString(separator = ",", prefix = "[", postfix = "]") ?: "[]"


}
