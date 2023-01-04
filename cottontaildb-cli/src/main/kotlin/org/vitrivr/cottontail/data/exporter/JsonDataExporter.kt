package org.vitrivr.cottontail.data.exporter

import com.google.gson.stream.JsonWriter
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.data.importer.JsonDataImporter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * A [DataExporter] implementation that can be used to write a [Format.JSON] files containing an array of entries.
 *
 * Data produced by the [JsonDataExporter] can be used as input for the [JsonDataImporter].
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
class JsonDataExporter(override val path: Path, val indent: String = "") : DataExporter {
    /** The [Format] handled by this [DataExporter]. */
    override val format: Format = Format.JSON

    /** Indicator whether this [DataExporter] has been closed. */
    override var closed: Boolean = false
        private set

    /** The [JsonWriter] instance used to read the JSON file. */
    private val writer = JsonWriter(Files.newBufferedWriter(this.path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))

    init {
        this.writer.isLenient = true
        this.writer.setIndent(this.indent)
        this.writer.beginArray() /* Starts writer the JSON array, which is the expected input. */
    }

    /**
     * Offers a new [Tuple] to this [ProtoDataExporter], which will
     * be appended to the output stream.
     *
     * @param tuple The [Tuple] to append.
     */
    @Suppress("UNCHECKED_CAST")
    override fun offer(tuple: Tuple) {
        this.writer.beginObject()
        repeat(tuple.size()) {
            this.writer.name(tuple.nameForIndex(it).split('.').last())
            when(tuple.type(it)) {
                Type.BOOLEAN -> this.writer.value(tuple.asBoolean(it))
                Type.BYTE -> this.writer.value(tuple.asByte(it))
                Type.SHORT -> this.writer.value(tuple.asShort(it))
                Type.INTEGER -> this.writer.value(tuple.asInt(it))
                Type.LONG -> this.writer.value(tuple.asLong(it))
                Type.FLOAT -> this.writer.value(tuple.asFloat(it))
                Type.DOUBLE -> this.writer.value(tuple.asDouble(it))
                Type.DATE -> this.writer.value(tuple.asLong(it))
                Type.STRING -> this.writer.value(tuple.asString(it))
                Type.COMPLEX32 -> {
                    val pair = tuple[it] as Pair<Float,Float>
                    this.writeComplex(pair.first, pair.second)
                }
                Type.COMPLEX64 -> {
                    val pair = tuple[it] as Pair<Double,Double>
                    this.writeComplex(pair.first, pair.second)
                }
                Type.FLOAT_VECTOR -> {
                    this.writer.beginArray()
                    tuple.asFloatVector(it)!!.forEach { v -> this.writer.value(v) }
                    this.writer.endArray()
                }
                Type.LONG_VECTOR -> {
                    this.writer.beginArray()
                    tuple.asLongVector(it)!!.forEach { v -> this.writer.value(v) }
                    this.writer.endArray()
                }
                Type.INTEGER_VECTOR -> {
                    this.writer.beginArray()
                    tuple.asIntVector(it)!!.forEach { v -> this.writer.value(v) }
                    this.writer.endArray()
                }
                Type.BOOLEAN_VECTOR -> {
                    this.writer.beginArray()
                    tuple.asBooleanVector(it)!!.forEach { v -> this.writer.value(v) }
                    this.writer.endArray()
                }
                Type.COMPLEX32_VECTOR -> {
                    this.writer.beginArray()
                    (tuple[it] as Array<Pair<Float,Float>>).forEach { v -> this.writeComplex(v.first, v.second) }
                    this.writer.endArray()
                }
                Type.COMPLEX64_VECTOR -> {
                    this.writer.beginArray()
                    (tuple[it] as Array<Pair<Double,Double>>).forEach { v -> this.writeComplex(v.first, v.second) }
                    this.writer.endArray()
                }
                else -> throw IllegalStateException("Type ${tuple.type(it)} cannot be serialized to JSON!")
            }
        }
        this.writer.endObject()
    }

    /**
     * Writes a complex number to the [JsonWriter].
     *
     * @param real The real part of the number.
     * @param imaginary The imaginary part of the number.
     */
    private fun writeComplex(real: Number, imaginary: Number) {
        this.writer.beginObject()
        this.writer.name("real")
        this.writer.value(real)
        this.writer.name("imaginary")
        this.writer.value(imaginary)
        this.writer.endObject()
    }

    /**
     * Closes this [ProtoDataExporter].
     */
    override fun close() {
        if (!this.closed) {
            this.writer.endArray()
            this.writer.close()
            this.closed = true
        }
    }
}