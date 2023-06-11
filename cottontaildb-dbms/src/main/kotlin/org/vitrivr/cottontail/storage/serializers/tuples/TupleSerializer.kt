package org.vitrivr.cottontail.storage.serializers.tuples

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.toByte
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.*
import java.nio.ByteBuffer
import java.nio.charset.Charset

/**
 * A facility that can be used to serialize and deserialize a [Tuple] from/to a [ByteBuffer]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class TupleSerializer(val schema: Array<ColumnDef<*>>) {


    /**
     * Calculates the size of the [ByteBuffer] required to hold the provided [Tuple]
     *
     * @param tuple The [Tuple] to write.
     * @return Size of [ByteBuffer] in bytes.
     */
    fun sizeOf(tuple: Tuple): Int {
        var size = Long.SIZE_BYTES /* Size of tupleId ID + number of columns */
        for (i in 0 until this.schema.size) {
            size += when(val value = tuple[i] as? PublicValue) {
                is BooleanValue,
                is ByteValue -> Byte.SIZE_BYTES
                is BooleanVectorValue -> TODO()
                is Complex32Value -> 2 * Float.SIZE_BYTES
                is Complex32VectorValue -> 2 * value.logicalSize * Float.SIZE_BYTES
                is Complex64Value -> 2 * Double.SIZE_BYTES
                is Complex64VectorValue -> 2 * value.logicalSize * Double.SIZE_BYTES
                is DateValue -> Long.SIZE_BYTES
                is DoubleValue -> Double.SIZE_BYTES
                is DoubleVectorValue -> value.logicalSize * Float.SIZE_BYTES
                is FloatValue -> Float.SIZE_BYTES
                is FloatVectorValue -> value.logicalSize * Float.SIZE_BYTES
                is IntValue -> Int.SIZE_BYTES
                is IntVectorValue -> value.logicalSize * Int.SIZE_BYTES
                is LongValue -> Long.SIZE_BYTES
                is LongVectorValue -> value.logicalSize * Long.SIZE_BYTES
                is ShortValue -> Short.SIZE_BYTES
                is ByteStringValue -> value.logicalSize
                is StringValue -> value.value.length * Char.SIZE_BYTES
                null -> when(this.schema[i].type) {
                    Types.Boolean,
                    Types.Byte -> Byte.SIZE_BYTES
                    Types.Complex32,
                    is Types.Complex32Vector -> 2 * Float.SIZE_BYTES
                    Types.Complex64,
                    is Types.Complex64Vector-> 2 * Double.SIZE_BYTES
                    Types.Double,
                    is Types.DoubleVector -> Double.SIZE_BYTES
                    Types.Float,
                    is Types.FloatVector -> Float.SIZE_BYTES
                    Types.Long,
                    Types.Date,
                    is Types.LongVector -> Long.SIZE_BYTES
                    Types.Short -> Short.SIZE_BYTES
                    Types.ByteString,
                    Types.String,
                    Types.Int,
                    is Types.IntVector -> Int.SIZE_BYTES
                    is Types.BooleanVector -> TODO()
                }
            }
        }
        return size
    }

    /**
     * Writes a [Tuple] to a new [ByteBuffer].
     *
     * @param tuple The [Tuple] to write.
     * @return [ByteBuffer]
     */
    fun toByteBuffer(tuple: Tuple) = toByteBuffer(tuple, ByteBuffer.allocate(sizeOf(tuple)))

    /**
     * Writes a [Tuple] to the provided [ByteBuffer].
     *
     * @param tuple The [Tuple] to write.
     * @return [ByteBuffer]
     */
    fun toByteBuffer(tuple: Tuple, buffer: ByteBuffer): ByteBuffer {
        buffer.mark()
        require(schema.contentDeepEquals(tuple.columns)) { "Tuple is not compatible with provided schema." }
        buffer.putLong(tuple.tupleId)
        for (i in 0 until this.schema.size) {
            val value = tuple[i] as? PublicValue
            this.writeValue(value, this.schema[i].type, buffer)
        }
        return buffer.reset()
    }

    /**
     * Reads a [Tuple] from the provided [ByteBuffer].
     *
     * @param buffer The [ByteBuffer] to read [Tuple] from.
     * @return [Tuple]
     */
    fun fromByteBuffer(buffer: ByteBuffer): Tuple {
        val tupleId = buffer.long
        val values = Array<Value?>(this.schema.size) {
            val column = this.schema[it]
            if (column.nullable) {
                this.readNullableValue(column.type, buffer)
            } else {
                this.readNonNullableValue(column.type, buffer)
            }
        }
        return StandaloneTuple(tupleId, this.schema, values)
    }

    /**
     * Writs a [PublicValue] (or null) of the given [Types] to the [ByteBuffer].
     *
     * @param value The [PublicValue] to write.
     * @param type The [Types] of the [PublicValue] (in case it is null).
     * @param buffer The [ByteBuffer] to write to.
     */
    private fun writeValue(value: PublicValue?, type: Types<*>, buffer: ByteBuffer) {
        when (value) {
            is BooleanValue -> buffer.put(value.value.toByte())
            is ByteValue -> buffer.put(value.value)
            is ShortValue -> buffer.putShort(value.value)
            is IntValue -> buffer.putInt(value.value)
            is LongValue -> buffer.putLong(value.value)
            is FloatValue -> buffer.putFloat(value.value)
            is DoubleValue -> buffer.putDouble(value.value)
            is DateValue -> buffer.putLong(value.value)
            is Complex32Value -> {
                buffer.putFloat(value.data[0])
                buffer.putFloat(value.data[1])
            }

            is Complex64Value -> {
                buffer.putDouble(value.data[0])
                buffer.putDouble(value.data[0])
            }

            is ByteStringValue -> {
                buffer.putInt(value.value.size)
                buffer.put(value.value)
            }

            is StringValue -> {
                val bytes = value.value.toByteArray(Charset.defaultCharset())
                buffer.putInt(bytes.size)
                buffer.put(bytes)
            }

            is Complex32VectorValue -> value.data.forEach { buffer.putFloat(it) }
            is Complex64VectorValue -> value.data.forEach { buffer.putDouble(it) }
            is BooleanVectorValue -> TODO()
            is DoubleVectorValue -> value.data.forEach { buffer.putDouble(it) }
            is FloatVectorValue -> value.data.forEach { buffer.putFloat(it) }
            is IntVectorValue -> value.data.forEach { buffer.putInt(it) }
            is LongVectorValue -> value.data.forEach { buffer.putLong(it) }
            null -> when (type) {
                Types.Boolean,
                is Types.BooleanVector,
                Types.Byte -> buffer.put(Byte.MIN_VALUE)

                Types.Short -> buffer.putShort(Short.MIN_VALUE)
                Types.Int,
                is Types.IntVector -> buffer.putInt(Int.MIN_VALUE)

                Types.Long,
                Types.Date,
                is Types.LongVector -> buffer.putLong(Long.MIN_VALUE)

                Types.Double,
                is Types.DoubleVector -> buffer.putDouble(Double.MIN_VALUE)

                Types.Float,
                is Types.FloatVector -> buffer.putFloat(Float.MIN_VALUE)

                Types.Complex32,
                is Types.Complex32Vector -> {
                    buffer.putFloat(Float.MIN_VALUE)
                    buffer.putFloat(Float.MIN_VALUE)
                }

                Types.Complex64,
                is Types.Complex64Vector -> {
                    buffer.putDouble(Double.MIN_VALUE)
                    buffer.putDouble(Double.MIN_VALUE)
                }

                Types.ByteString,
                Types.String -> buffer.putInt(-1)
            }
        }
    }

    /**
     * Reads a non-nullable [Value] of the given [Types] from the [ByteBuffer].
     *
     * @param type [Types] of the [Value]
     * @param buffer The [ByteBuffer] to read from.
     * @return [Value]
     */
    private fun readNullableValue(type: Types<*>, buffer: ByteBuffer): Value? = when(type) {
        Types.Boolean -> {
            val value = buffer.get()
            if (value == Byte.MIN_VALUE) { null } else { BooleanValue(value == 0.toByte()) }
        }
        Types.Byte -> {
            val value = buffer.get()
            if (value == Byte.MIN_VALUE) { null } else { ByteValue(value) }
        }
        Types.Complex32 -> {
            val real = buffer.float
            val imaginary = buffer.float
            if (real == Float.MIN_VALUE && imaginary == Float.MIN_VALUE) { null } else {  Complex32Value(real, imaginary) }
        }
        Types.Complex64 -> {
            val real = buffer.double
            val imaginary = buffer.double
            if (real == Double.MIN_VALUE && imaginary == Double.MIN_VALUE) { null } else {  Complex64Value(real, imaginary) }
        }
        Types.Double -> {
            val value = buffer.double
            if (value == Double.MIN_VALUE) { null } else { DoubleValue(value) }
        }
        Types.Float -> {
            val value = buffer.float
            if (value == Float.MIN_VALUE) { null } else { FloatValue(value) }
        }
        Types.Int -> {
            val value = buffer.int
            if (value == Int.MIN_VALUE) { null } else { IntValue(value) }
        }
        Types.Long -> {
            val value = buffer.long
            if (value == Long.MIN_VALUE) { null } else { LongValue(value) }
        }
        Types.Short -> {
            val value = buffer.short
            if (value == Short.MIN_VALUE) { null } else { ShortValue(value) }
        }
        Types.Date -> {
            val value = buffer.long
            if (value == Long.MIN_VALUE) { null } else { DateValue(value) }
        }
        Types.ByteString -> {
            val length = buffer.int /* Variable length. */
            if (length == -1) { null } else {  ByteStringValue(ByteArray(length) { buffer.get() }) }
        }
        Types.String -> {
            val length = buffer.int /* Variable length. */
            if (length == -1) { null } else {  StringValue(ByteArray(length) { buffer.get() }.toString(Charset.defaultCharset())) }
        }
        is Types.BooleanVector -> TODO()
        is Types.Complex32Vector -> {
            buffer.mark()
            if (buffer.double == Double.MIN_VALUE) {
                null
            } else {
                buffer.reset()
                Complex32VectorValue(FloatArray(2*type.logicalSize) { buffer.float })
            }
        }
        is Types.Complex64Vector -> {
            buffer.mark()
            if (buffer.double == Double.MIN_VALUE) {
                null
            } else {
                buffer.reset()
                Complex64VectorValue(DoubleArray(2*type.logicalSize) { buffer.double })
            }
        }
        is Types.DoubleVector -> {
            buffer.mark()
            if (buffer.double == Double.MIN_VALUE) {
                null
            } else {
                buffer.reset()
                DoubleVectorValue(DoubleArray(type.logicalSize) { buffer.double })
            }
        }
        is Types.FloatVector -> {
            buffer.mark()
            if (buffer.float == Float.MIN_VALUE) {
                null
            } else {
                buffer.reset()
                FloatVectorValue(FloatArray(type.logicalSize) { buffer.float })
            }
        }
        is Types.IntVector -> {
            buffer.mark()
            if (buffer.int == Int.MIN_VALUE) {
                null
            } else {
                buffer.reset()
                IntVectorValue(IntArray(type.logicalSize) { buffer.int })
            }
        }
        is Types.LongVector -> {
            buffer.mark()
            if (buffer.long == Long.MIN_VALUE) {
                null
            } else {
                buffer.reset()
                LongVectorValue(LongArray(type.logicalSize) { buffer.long })
            }
        }
    }

    /**
     * Reads a non-nullable [Value] of the given [Types] from the [ByteBuffer].
     *
     * @param type [Types] of the [Value]
     * @param buffer The [ByteBuffer] to read from.
     * @return [Value]
     */
    private fun readNonNullableValue(type: Types<*>, buffer: ByteBuffer): Value = when(type) {
        Types.Boolean -> BooleanValue(buffer.get() == 0.toByte())
        Types.Byte -> ByteValue(buffer.get())
        Types.Complex32 -> Complex32Value(buffer.float, buffer.float)
        Types.Complex64 -> Complex64Value(buffer.double, buffer.double)
        Types.Double -> DoubleValue(buffer.double)
        Types.Float -> FloatValue(buffer.float)
        Types.Int -> IntValue(buffer.int)
        Types.Long -> LongValue(buffer.long)
        Types.Short -> ShortValue(buffer.short)
        Types.Date -> DateValue(buffer.long)
        Types.ByteString -> {
            val length = buffer.int /* Variable length. */
            ByteStringValue(ByteArray(length) { buffer.get() })
        }
        Types.String -> {
            val length = buffer.int /* Variable length. */
            StringValue(ByteArray(length) { buffer.get() }.toString(Charset.defaultCharset()))
        }
        is Types.BooleanVector -> TODO()
        is Types.Complex32Vector -> Complex32VectorValue(FloatArray(2*type.logicalSize) { buffer.float })
        is Types.Complex64Vector -> Complex64VectorValue(DoubleArray(2*type.logicalSize) { buffer.double })
        is Types.DoubleVector -> DoubleVectorValue(DoubleArray(type.logicalSize) { buffer.double })
        is Types.FloatVector -> FloatVectorValue(FloatArray(type.logicalSize) { buffer.float })
        is Types.IntVector -> IntVectorValue(IntArray(type.logicalSize) { buffer.int })
        is Types.LongVector -> LongVectorValue(LongArray(type.logicalSize) { buffer.long })
    }
}