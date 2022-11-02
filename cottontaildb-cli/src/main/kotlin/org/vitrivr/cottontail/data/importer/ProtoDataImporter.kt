package org.vitrivr.cottontail.data.importer

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.extensions.*
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * A [DataImporter] implementation that can be used to read a [Format.PROTO] file containing an list of entries.
 *
 * @author Ralph Gasser
 * @version 1.0.0
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
    override fun next(): Map<ColumnDef<*>, Value?>{
        val value = Object2ObjectArrayMap<ColumnDef<*>, Value?>(this.schema.size)
        for (column in this.schema) {
            val element = this.next?.elementsList?.singleOrNull {
                it.column.fqn().matches(column.name)
            } ?: throw IllegalStateException("Could not find column ${column.name} in input.")
            value[column] = when (column.type) {
                is Types.Boolean -> element.value.toShortValue()
                is Types.Byte -> element.value.toByteValue()
                is Types.Short -> element.value.toShortValue()
                is Types.Int -> element.value.toIntValue()
                is Types.Long -> element.value.toLongValue()
                is Types.Float -> element.value.toFloatValue()
                is Types.Double -> element.value.toDoubleValue()
                is Types.Date -> element.value.toDateValue()
                is Types.String ->  element.value.toStringValue()
                is Types.Complex32 -> element.value.toComplex32Value()
                is Types.Complex64 -> element.value.toComplex64Value()
                is Types.IntVector -> element.value.vectorData.toIntVectorValue()
                is Types.LongVector -> element.value.vectorData.toLongVectorValue()
                is Types.FloatVector -> element.value.vectorData.toFloatVectorValue()
                is Types.DoubleVector -> element.value.vectorData.toDoubleVectorValue()
                is Types.BooleanVector -> element.value.vectorData.toBooleanVectorValue()
                is Types.Complex32Vector -> element.value.vectorData.toComplex32VectorValue()
                is Types.Complex64Vector -> element.value.vectorData.toComplex64VectorValue()
                is Types.ByteString -> TODO()
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