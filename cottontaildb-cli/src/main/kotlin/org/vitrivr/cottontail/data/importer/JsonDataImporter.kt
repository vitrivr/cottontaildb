package org.vitrivr.cottontail.data.importer

import com.google.gson.GsonBuilder
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.data.Format
import java.nio.file.Files
import java.nio.file.Path

/**
 * A [DataImporter] implementation that can be used to read a JSON file containing an array of entries.
 * Each entry needs to be an object for which each key corresponds to a field in the [schema]. The
 * fields must occur in order of definition.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class JsonDataImporter(override val path: Path, override val schema: List<ColumnDef<*>>) : DataImporter {

    /** The [JsonReader] instance used to read the JSON file. */
    private val reader = GsonBuilder()
        .serializeNulls()
        .serializeSpecialFloatingPointValues()
        .create()
        .newJsonReader(Files.newBufferedReader(this.path))

    init {
        this.reader.isLenient = true
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
    override fun next(): Map<ColumnDef<*>,Value?> {
        this.reader.beginObject()
        val value = Object2ObjectArrayMap<ColumnDef<*>, Value?>(this.schema.size)
        for (column in this.schema) {
            val parsed = this.reader.nextName().split('.')
            val name = Name.ColumnName(parsed[0], parsed[1], parsed[2])
            check(name == column.name) { "$name does not match the expected column name ${column.name}." }
            value[column] = when (column.type) {
                is Types.Boolean -> BooleanValue(this.reader.nextBoolean())
                is Types.Byte -> ByteValue(this.reader.nextInt())
                is Types.Short -> ShortValue(this.reader.nextInt())
                is Types.Int -> IntValue(this.reader.nextInt())
                is Types.Long -> LongValue(this.reader.nextLong())
                is Types.Float -> FloatValue(this.reader.nextDouble().toFloat())
                is Types.Double -> DoubleValue(this.reader.nextDouble())
                is Types.Date -> DateValue(this.reader.nextLong())
                is Types.String ->  StringValue(this.reader.nextString())
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
     * @return [Complex32Value] containing the value.
     */
    private fun readComplex32Value(): Complex32Value {
        this.reader.beginObject()
        this.reader.nextName()
        val real = this.reader.nextDouble().toFloat()
        this.reader.nextName()
        val imaginary = this.reader.nextDouble().toFloat()
        this.reader.endObject()
        return Complex32Value(real, imaginary)
    }

    /**
     * Reads a [Complex64Value] value from the JSON file.
     *
     * @return [Complex64Value] containing the value.
     */
    private fun readComplex64Value(): Complex64Value {
        this.reader.beginObject()
        this.reader.nextName()
        val real = this.reader.nextDouble()
        this.reader.nextName()
        val imaginary = this.reader.nextDouble()
        this.reader.endObject()
        return Complex64Value(real, imaginary)
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
     * Reads a int vector of the given size from the JSON file.
     *
     * @param size The size of the int vector.
     * @return [IntVectorValue] containing the vector.
     */
    private fun readIntVector(size: Int): IntVectorValue {
        this.reader.beginArray()
        val vector = IntVectorValue(IntArray(size) { this.reader.nextInt() })
        this.reader.endArray()
        return vector
    }

    /**
     * Reads a long vector of the given size from the JSON file.
     *
     * @param size The size of the long vector.
     * @return [LongVectorValue] containing the vector.
     */
    private fun readLongVector(size: Int): LongVectorValue {
        this.reader.beginArray()
        val vector = LongVectorValue(LongArray(size) { this.reader.nextLong() })
        this.reader.endArray()
        return vector
    }

    /**
     * Reads a float vector of the given size from the JSON file.
     *
     * @param size The size of the float vector.
     * @return [FloatVectorValue] containing the vector.
     */
    private fun readFloatVector(size: Int): FloatVectorValue {
        this.reader.beginArray()
        val vector = FloatVectorValue(FloatArray(size) { this.reader.nextDouble().toFloat() })
        this.reader.endArray()
        return vector
    }

    /**
     * Reads a double vector of the given size from the JSON file.
     *
     * @param size The size of the double vector.
     * @return [DoubleVectorValue] containing the vector.
     */
    private fun readDoubleVector(size: Int): DoubleVectorValue {
        this.reader.beginArray()
        val vector = DoubleVectorValue(DoubleArray(size) { this.reader.nextDouble() })
        this.reader.endArray()
        return vector
    }

    /**
     * Reads a complex 32 vector of the given size from the JSON file.
     *
     * @param size The size of the double vector.
     * @return [Complex32VectorValue] containing the vector.
     */
    private fun readComplex32Vector(size: Int): Complex32VectorValue {
        this.reader.beginArray()
        val vector = Complex32VectorValue(Array(size) { this.readComplex32Value() })
        this.reader.endArray()
        return vector
    }

    /**
     * Reads a complex64 vector of the given size from the JSON file.
     *
     * @param size The size of the complex64 vector.
     * @return [Complex64VectorValue] containing the vector.
     */
    private fun readComplex64Vector(size: Int): Complex64VectorValue {
        this.reader.beginArray()
        val vector = Complex64VectorValue(Array(size) { this.readComplex64Value() })
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