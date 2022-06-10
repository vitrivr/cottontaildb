package org.vitrivr.cottontail.data.exporter

import com.github.doyaaaaaken.kotlincsv.client.KotlinCsvExperimental
import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
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
            header = (0..tuple.size()).map { tuple.nameForIndex(it) }
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
                Type.COMPLEX32 -> TODO()
                Type.COMPLEX64 -> TODO()
                Type.DOUBLE_VECTOR -> TODO()
                Type.FLOAT_VECTOR -> TODO()
                Type.LONG_VECTOR -> TODO()
                Type.INTEGER_VECTOR -> TODO()
                Type.BOOLEAN_VECTOR -> TODO()
                Type.COMPLEX32_VECTOR -> TODO()
                Type.COMPLEX64_VECTOR -> TODO()
                Type.BLOB -> TODO()
                Type.UNDEFINED -> TODO()
            }
        }
        writer.writeRow(row)
    }

    override fun close() {
        writer.close()
        this.closed = true
    }


}