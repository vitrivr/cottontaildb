package org.vitrivr.cottontail.data.importer

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.data.Format
import java.nio.file.Files
import java.nio.file.Path

/**
 * A [DataImporter] implementation that can be used to read a JSON file containing an array of entries.
 * Each entry needs to be an object for which each key corresponds to a field in the [schema]. The
 * fields must occur in order of definition.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class JsonDataImporter(override val path: Path, override val schema: List<ColumnDef<*>>) : DataImporter {

    /** The [JsonReader] instance used to read the JSON file. */
    private val reader = JsonReader(Files.newBufferedReader(this.path))

    init {
        this.reader.beginArray() /* Starts reading the JSON array, which is the expected input. */
    }

    /** The [Format] handled by this [DataImporter]. */
    override val format: Format = Format.JSON

    /** Indicator whether this [DataImporter] has been closed. */
    override var closed: Boolean = false
        private set

    /**
     * Returns the next [Tuple] from the data.
     *
     * @return [Tuple]
     */
    override fun next(): Map<ColumnDef<*>,Any?> {
        this.reader.beginObject()
        val value = Object2ObjectArrayMap<ColumnDef<*>, Any?>(this.schema.size)
        for (column in this.schema) {
            val parsed = this.reader.nextName().split('.')
            val name = Name.ColumnName(parsed[0], parsed[1], parsed[2])
            check(name == column.name) { "$name does not match the expected column name ${column.name}." }
            value[column] = when (column.type) {
                is Types.Boolean ->this.reader.nextBoolean()
                is Types.Byte -> this.reader.nextInt()
                is Types.Short -> this.reader.nextInt()
                is Types.Int -> this.reader.nextInt()
                is Types.Long -> this.reader.nextLong()
                is Types.Float -> this.reader.nextDouble().toFloat()
                is Types.Double -> this.reader.nextDouble()
                is Types.Date -> this.reader.nextLong()
                is Types.String ->  this.reader.nextString()
                is Types.Complex32 -> this.readComplex32Value()
                is Types.Complex64 -> this.readComplex64Value()
                is Types.IntVector -> this.readIntVector(column.type.logicalSize)
                is Types.LongVector -> this.readLongVector(column.type.logicalSize)
                is Types.FloatVector -> this.readFloatVector(column.type.logicalSize)
                is Types.DoubleVector -> this.readDoubleVector(column.type.logicalSize)
                is Types.BooleanVector -> this.readBooleanVector(column.type.logicalSize)
                is Types.Complex32Vector -> this.readComplex32Vector(column.type.logicalSize)
                is Types.Complex64Vector -> this.readComplex64Vector(column.type.logicalSize)
            }
        }
        this.reader.endObject()
        return value
    }

    /**
     * Checks if there is another entry and returns true if so, and false otherwise.
     *
     * @return True if there is another entry, false otherwise.
     */
    override fun hasNext(): Boolean =
        this.reader.hasNext() && this.reader.peek() == JsonToken.BEGIN_OBJECT

    /**
     * Reads a [Complex32Value] value from the JSON file.
     *
     * @return [Pair] containing the value.
     */
    private fun readComplex32Value(): Pair<Float,Float> {
        this.reader.beginObject()
        this.reader.nextName()
        val real = this.reader.nextDouble().toFloat()
        this.reader.nextName()
        val imaginary = this.reader.nextDouble().toFloat()
        this.reader.endObject()
        return real to imaginary
    }

    /**
     * Reads a [Complex64Value] value from the JSON file.
     *
     * @return [Pair] containing the value.
     */
    private fun readComplex64Value(): Pair<Double,Double>  {
        this.reader.beginObject()
        this.reader.nextName()
        val real = this.reader.nextDouble()
        this.reader.nextName()
        val imaginary = this.reader.nextDouble()
        this.reader.endObject()
        return real to imaginary
    }

    /**
     * Reads a boolean vector of the given size from the JSON file.
     *
     * @param size The size of the boolean vector.
     * @return [BooleanVectorValue] containing the vector.
     */
    private fun readBooleanVector(size: Int): BooleanVectorValue {
        this.reader.beginArray()
        val vector = BooleanVectorValue(BooleanArray(size) { this.reader.nextBoolean() })
        this.reader.endArray()
        return vector
    }

    /**
     * Reads an int vector of the given size from the JSON file.
     *
     * @param size The size of the int vector.
     * @return [IntArray] containing the vector.
     */
    private fun readIntVector(size: Int): IntArray {
        this.reader.beginArray()
        val vector = IntArray(size) { this.reader.nextInt() }
        this.reader.endArray()
        return vector
    }

    /**
     * Reads a long vector of the given size from the JSON file.
     *
     * @param size The size of the long vector.
     * @return [LongArray] containing the vector.
     */
    private fun readLongVector(size: Int): LongArray {
        this.reader.beginArray()
        val vector = LongArray(size) { this.reader.nextLong() }
        this.reader.endArray()
        return vector
    }

    /**
     * Reads a float vector of the given size from the JSON file.
     *
     * @param size The size of the float vector.
     * @return [FloatArray] containing the vector.
     */
    private fun readFloatVector(size: Int): FloatArray {
        this.reader.beginArray()
        val vector = FloatArray(size) { this.reader.nextDouble().toFloat() }
        this.reader.endArray()
        return vector
    }

    /**
     * Reads a double vector of the given size from the JSON file.
     *
     * @param size The size of the double vector.
     * @return [DoubleArray] containing the vector.
     */
    private fun readDoubleVector(size: Int): DoubleArray {
        this.reader.beginArray()
        val vector = DoubleArray(size) { this.reader.nextDouble() }
        this.reader.endArray()
        return vector
    }

    /**
     * Reads a complex 32 vector of the given size from the JSON file.
     *
     * @param size The size of the double vector.
     * @return [Complex32VectorValue] containing the vector.
     */
    private fun readComplex32Vector(size: Int): Array<Pair<Float,Float>> {
        this.reader.beginArray()
        val vector = Array(size) { this.readComplex32Value() }
        this.reader.endArray()
        return vector
    }

    /**
     * Reads a complex64 vector of the given size from the JSON file.
     *
     * @param size The size of the complex64 vector.
     * @return [Complex64VectorValue] containing the vector.
     */
    private fun readComplex64Vector(size: Int): Array<Pair<Double,Double>> {
        this.reader.beginArray()
        val vector = Array(size) { this.readComplex64Value() }
        this.reader.endArray()
        return vector
    }

    /**
     * Closes this [JsonDataImporter]
     */
    override fun close() {
        if (!this.closed) {
            this.reader.close()
            this.closed = true
        }
    }
}