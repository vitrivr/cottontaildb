package org.vitrivr.cottontail.data.importer

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.extensions.fqn
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*

/**
 * A [DataImporter] implementation that can be used to read a [Format.PROTO] file containing an list of entries.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class ProtoDataImporter(override val path: Path, override val schema: List<ColumnDef<*>>) : DataImporter {

    /** The [Format] handled by this [DataImporter]. */
    override val format: Format = Format.PROTO

    /** Indicator whether this [DataImporter] has been closed. */
    override var closed: Boolean = false
        private set

    /** Internal [InputStream] */
    private val input = Files.newInputStream(this.path, StandardOpenOption.READ)

    /** Next element that can be returned by this [ProtoDataImporter]. */
    protected var next: CottontailGrpc.InsertMessage? = null

    /**
     * Returns the next [CottontailGrpc.InsertMessage.Builder] from the data.
     *
     * @return [CottontailGrpc.InsertMessage.Builder]
     */
    override fun next(): Map<ColumnDef<*>, Any?>{
        val value = Object2ObjectArrayMap<ColumnDef<*>, Any?>(this.schema.size)
        for (column in this.schema) {
            val element = this.next?.elementsList?.singleOrNull {
                it.column.fqn().matches(column.name)
            } ?: throw IllegalStateException("Could not find column ${column.name} in input.")
            value[column] = when (val type = column.type) {
                is Types.Boolean -> element.value.booleanData
                is Types.Byte -> element.value.intData.toByte()
                is Types.Short -> element.value.intData.toByte()
                is Types.Int -> element.value.intData
                is Types.Long -> element.value.longData
                is Types.Float -> element.value.floatData
                is Types.Double -> element.value.doubleData
                is Types.Date -> Date(element.value.dateData)
                is Types.String ->  element.value.stringData
                is Types.Complex32 -> element.value.complex32Data.real to element.value.complex32Data.imaginary
                is Types.Complex64 -> element.value.complex64Data.real to element.value.complex64Data.imaginary
                is Types.IntVector -> element.value.vectorData.intVector.vectorList.toIntArray()
                is Types.LongVector -> element.value.vectorData.longVector.vectorList.toLongArray()
                is Types.FloatVector -> element.value.vectorData.floatVector.vectorList.toFloatArray()
                is Types.DoubleVector -> element.value.vectorData.doubleVector.vectorList.toDoubleArray()
                is Types.BooleanVector -> element.value.vectorData.boolVector.vectorList.toBooleanArray()
                is Types.Complex32Vector -> element.value.vectorData.complex32Vector.vectorList.map { it.real to it.imaginary }.toTypedArray()
                is Types.Complex64Vector -> element.value.vectorData.complex64Vector.vectorList.map { it.real to it.imaginary }.toTypedArray()
                else -> throw java.lang.IllegalStateException("Unhandled column type $type.")
            }
        }
        return value
    }

    /**
     * Checks if there is another entry and returns true if so, and false otherwise.
     *
     * @return True if there is another entry, false otherwise.
     */
    override fun hasNext(): Boolean {
        return try {
            this.next = CottontailGrpc.InsertMessage.parseDelimitedFrom(this.input)
            this.next != null
        } catch (e: Throwable) {
            false
        }
    }

    /**
     * Closes this [ProtoDataImporter]
     */
    override fun close() {
        if (!this.closed) {
            this.input.close()
            this.closed = true
        }
    }
}