package org.vitrivr.cottontail.model.values.types

import org.vitrivr.cottontail.database.statistics.columns.*
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.storage.serializers.*

/**
 * Specifies the [Types] available in Cottontail DB. This construct is a centerpiece of Cottontail DB's type system
 * and allows for type safety in the eye de-/serialization, conversion and casting during query processing.
 *
 * Upon serialization, [Types]s can be stored as strings or numeric ordinals and mapped to the respective class using [Types.forName].
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
sealed interface Types<T : Value> {

    companion object {
        /** Constant for unknown logical size. */
        const val LOGICAL_SIZE_UNKNOWN = -1

        /**
         * Returns the [Types] for the provided name.
         *
         * @param name For which to lookup the [Types].
         * @param logicalSize The logical size of this [Types] (only for vector types).
         */
        fun forName(name: kotlin.String, logicalSize: kotlin.Int): Types<*> = when (name.uppercase()) {
            "BOOLEAN" -> Boolean
            "BYTE" -> Byte
            "SHORT" -> Short
            "INT",
            "INTEGER" -> Int
            "LONG" -> Long
            "DATE" -> Date
            "FLOAT" -> Float
            "DOUBLE" -> Double
            "STRING" -> String
            "COMPLEX32" -> Complex32
            "COMPLEX64" -> Complex64
            "INT_VEC" -> IntVector(logicalSize)
            "LONG_VEC" -> LongVector(logicalSize)
            "FLOAT_VEC" -> FloatVector(logicalSize)
            "DOUBLE_VEC" -> DoubleVector(logicalSize)
            "BOOL_VEC" -> BooleanVector(logicalSize)
            "COMPLEX32_VEC" -> Complex32Vector(logicalSize)
            "COMPLEX64_VEC" -> Complex64Vector(logicalSize)
            else -> throw java.lang.IllegalArgumentException("The column type $name does not exists!")
        }

        /**
         * Returns the [Types] for the provided ordinal.
         *
         * @param ordinal For which to lookup the [Types].
         * @param logicalSize The logical size of this [Types] (only for vector types).
         */
        fun forOrdinal(ordinal: kotlin.Int, logicalSize: kotlin.Int) = when (ordinal) {
            0 -> Boolean
            1 -> Byte
            2 -> Short
            3 -> Int
            4 -> Long
            5 -> Date
            6 -> Float
            7 -> Double
            8 -> String
            9 -> Complex32
            10 -> Complex64
            11 -> IntVector(logicalSize)
            12 -> LongVector(logicalSize)
            13 -> FloatVector(logicalSize)
            14 -> DoubleVector(logicalSize)
            15 -> BooleanVector(logicalSize)
            16 -> Complex32Vector(logicalSize)
            17 -> Complex64Vector(logicalSize)
            else -> throw java.lang.IllegalArgumentException("The column type for ordinal $ordinal does not exists!")
        }
    }

    /** The name of a [Types] implementation. */
    val name: kotlin.String

    /** The logical size of a [Types] implementation, i.e., the number of elements in a vector. */
    val logicalSize: kotlin.Int

    /** The physical size of a [Types] implementation, i.e., the size in bytes in-memory or on disk (w/o compression). */
    val physicalSize: kotlin.Int

    /** The ordinal value of a [Types] implementation. */
    val ordinal: kotlin.Int

    /** Checks if the given [Value] is compatible with this [Types].
     *
     * @param value The [Value] to check.
     */
    fun compatible(value: Value) = this == value.type

    /** Returns the default value for this [Types]. */
    fun defaultValue(): T

    /** Returns a [ValueSerializerFactory] for this [Types]. */
    fun serializerFactory(): ValueSerializerFactory<T>

    /** Creates and returns a [ValueStatistics] object for this [Types]. */
    fun statistics(): ValueStatistics<T>

    /**
     * A [Scalar] type.
     */
    sealed interface Scalar<T: ScalarValue<*>>: Types<T> {
        override val logicalSize
            get() = 1
    }

    /**
     * A [Numeric] type.
     */
    sealed interface Numeric<T: NumericValue<*>>: Scalar<T>

    /**
     * A [Complex] type.
     */
    sealed interface Complex<T: ComplexValue<*>>: Numeric<T>

    /**
     * A [Vector] type
     */
    sealed interface Vector<T: VectorValue<*>, E: ScalarValue<*>>: Types<T> {
        /** The element type of this [Vector] type. */
        val elementType: Scalar<E>
    }

    @Suppress("UNCHECKED_CAST")
    object Boolean : Scalar<BooleanValue> {
        override val name = "BOOLEAN"
        override val ordinal: kotlin.Int = 0
        override val physicalSize = kotlin.Byte.SIZE_BYTES
        override fun defaultValue(): BooleanValue = BooleanValue.FALSE
        override fun serializerFactory(): ValueSerializerFactory<BooleanValue> = BooleanValueSerializerFactory
        override fun statistics(): ValueStatistics<BooleanValue> = BooleanValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Byte : Numeric<ByteValue> {
        override val name = "BYTE"
        override val ordinal: kotlin.Int = 1
        override val physicalSize = kotlin.Byte.SIZE_BYTES
        override fun defaultValue(): ByteValue = ByteValue.ZERO
        override fun serializerFactory(): ValueSerializerFactory<ByteValue> = ByteValueSerializerFactory
        override fun statistics(): ValueStatistics<ByteValue> = ByteValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Short : Numeric<ShortValue> {
        override val name = "SHORT"
        override val ordinal: kotlin.Int = 2
        override val physicalSize = kotlin.Short.SIZE_BYTES
        override fun defaultValue(): ShortValue = ShortValue.ZERO
        override fun serializerFactory(): ValueSerializerFactory<ShortValue> = ShortValueSerializerFactory
        override fun statistics(): ValueStatistics<ShortValue> = ShortValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Int : Numeric<IntValue> {
        override val name = "INTEGER"
        override val ordinal: kotlin.Int = 3
        override val physicalSize = kotlin.Int.SIZE_BYTES
        override fun defaultValue(): IntValue = IntValue.ZERO
        override fun serializerFactory(): ValueSerializerFactory<IntValue> = IntValueSerializerFactory
        override fun statistics(): ValueStatistics<IntValue> = IntValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Long : Numeric<LongValue> {
        override val name = "LONG"
        override val ordinal: kotlin.Int = 4
        override val physicalSize = kotlin.Long.SIZE_BYTES
        override fun defaultValue(): LongValue = LongValue.ZERO
        override fun serializerFactory(): ValueSerializerFactory<LongValue> = LongValueSerializerFactory
        override fun statistics(): ValueStatistics<LongValue> = LongValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Date : Scalar<DateValue> {
        override val name = "DATE"
        override val ordinal: kotlin.Int = 5
        override val physicalSize = kotlin.Long.SIZE_BYTES
        override fun defaultValue(): DateValue = DateValue(System.currentTimeMillis())
        override fun serializerFactory(): ValueSerializerFactory<DateValue> = DateValueSerializerFactory
        override fun statistics(): ValueStatistics<DateValue> = DateValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Float : Numeric<FloatValue> {
        override val name = "FLOAT"
        override val ordinal: kotlin.Int = 6
        override val physicalSize = kotlin.Int.SIZE_BYTES
        override fun defaultValue(): FloatValue = FloatValue.ZERO
        override fun serializerFactory(): ValueSerializerFactory<FloatValue> = FloatValueSerializerFactory
        override fun statistics(): ValueStatistics<FloatValue> = FloatValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Double : Numeric<DoubleValue> {
        override val name = "DOUBLE"
        override val ordinal: kotlin.Int = 7
        override val physicalSize = kotlin.Long.SIZE_BYTES
        override fun defaultValue(): DoubleValue = DoubleValue.ZERO
        override fun serializerFactory(): ValueSerializerFactory<DoubleValue> = DoubleValueSerializerFactory
        override fun statistics(): ValueStatistics<DoubleValue> = DoubleValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object String : Scalar<StringValue> {
        override val name = "STRING"
        override val ordinal: kotlin.Int = 8
        override val logicalSize = LOGICAL_SIZE_UNKNOWN
        override val physicalSize = LOGICAL_SIZE_UNKNOWN * Char.SIZE_BYTES
        override fun defaultValue(): StringValue = StringValue.EMPTY
        override fun serializerFactory(): ValueSerializerFactory<StringValue> = StringValueSerializerFactory
        override fun statistics(): ValueStatistics<StringValue> = StringValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Complex32 : Complex<Complex32Value> {
        override val name = "COMPLEX32"
        override val ordinal: kotlin.Int = 9
        override val logicalSize = 1
        override val physicalSize = 2 * kotlin.Int.SIZE_BYTES
        override fun defaultValue(): Complex32Value = Complex32Value.ZERO
        override fun serializerFactory(): ValueSerializerFactory<Complex32Value> = Complex32ValueSerializerFactory
        override fun statistics(): ValueStatistics<Complex32Value> = ValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    object Complex64 : Complex<Complex64Value> {
        override val name = "COMPLEX64"
        override val ordinal: kotlin.Int = 10
        override val logicalSize = 1
        override val physicalSize = 2 * kotlin.Long.SIZE_BYTES
        override fun defaultValue(): Complex64Value = Complex64Value.ZERO
        override fun serializerFactory(): ValueSerializerFactory<Complex64Value> = Complex64ValueSerializerFactory
        override fun statistics(): ValueStatistics<Complex64Value> = ValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    class IntVector(override val logicalSize: kotlin.Int) : Vector<IntVectorValue, IntValue> {
        override val name = "INT_VEC"
        override val ordinal: kotlin.Int = 11
        override val physicalSize = this.logicalSize * kotlin.Int.SIZE_BYTES
        override val elementType = Int
        override fun defaultValue(): IntVectorValue = IntVectorValue.zero(this.logicalSize)
        override fun serializerFactory(): ValueSerializerFactory<IntVectorValue> = IntVectorValueSerializerFactory
        override fun statistics(): ValueStatistics<IntVectorValue> = IntVectorValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    class LongVector(override val logicalSize: kotlin.Int) : Vector<LongVectorValue, LongValue> {
        override val name = "LONG_VEC"
        override val ordinal: kotlin.Int = 12
        override val physicalSize = this.logicalSize * kotlin.Long.SIZE_BYTES
        override val elementType = Long
        override fun defaultValue(): LongVectorValue = LongVectorValue.zero(this.logicalSize)
        override fun serializerFactory(): ValueSerializerFactory<LongVectorValue> = LongVectorValueSerializerFactory
        override fun statistics(): ValueStatistics<LongVectorValue> = LongVectorValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    class FloatVector(override val logicalSize: kotlin.Int) : Vector<FloatVectorValue, FloatValue> {
        override val name = "FLOAT_VEC"
        override val ordinal: kotlin.Int = 13
        override val physicalSize = this.logicalSize * kotlin.Int.SIZE_BYTES
        override val elementType = Float
        override fun defaultValue(): FloatVectorValue = FloatVectorValue.zero(this.logicalSize)
        override fun serializerFactory(): ValueSerializerFactory<FloatVectorValue> = FloatVectorValueSerializerFactory
        override fun statistics(): ValueStatistics<FloatVectorValue> = FloatVectorValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    class DoubleVector(override val logicalSize: kotlin.Int) : Vector<DoubleVectorValue, DoubleValue> {
        override val name = "DOUBLE_VEC"
        override val ordinal: kotlin.Int = 14
        override val physicalSize = this.logicalSize * kotlin.Long.SIZE_BYTES
        override val elementType = Double
        override fun defaultValue(): DoubleVectorValue = DoubleVectorValue.zero(this.logicalSize)
        override fun serializerFactory(): ValueSerializerFactory<DoubleVectorValue> = DoubleVectorValueSerializerFactory
        override fun statistics(): ValueStatistics<DoubleVectorValue> = DoubleVectorValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    class BooleanVector(override val logicalSize: kotlin.Int) : Vector<BooleanVectorValue, BooleanValue> {
        override val name = "BOOL_VEC"
        override val ordinal: kotlin.Int = 15
        override val physicalSize = this.logicalSize * kotlin.Byte.SIZE_BYTES
        override val elementType = Boolean
        override fun defaultValue(): BooleanVectorValue = BooleanVectorValue.zero(this.logicalSize)
        override fun serializerFactory(): ValueSerializerFactory<BooleanVectorValue> = BooleanVectorValueSerializerFactory
        override fun statistics(): ValueStatistics<BooleanVectorValue> = BooleanVectorValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    class Complex32Vector(override val logicalSize: kotlin.Int) : Vector<Complex32VectorValue, Complex32Value> {
        override val name = "COMPLEX32_VEC"
        override val ordinal: kotlin.Int = 16
        override val physicalSize = this.logicalSize * 2 * kotlin.Int.SIZE_BYTES
        override val elementType = Complex32
        override fun defaultValue(): Complex32VectorValue = Complex32VectorValue.zero(this.logicalSize)
        override fun serializerFactory(): ValueSerializerFactory<Complex32VectorValue> = Complex32VectorValueSerializerFactory
        override fun statistics(): ValueStatistics<Complex32VectorValue> = ValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    class Complex64Vector(override val logicalSize: kotlin.Int) : Vector<Complex64VectorValue, Complex32Value> {
        override val name = "COMPLEX64_VEC"
        override val ordinal: kotlin.Int = 17
        override val elementType = Complex32
        override val physicalSize = this.logicalSize * 2 * kotlin.Long.SIZE_BYTES
        override fun defaultValue(): Complex64VectorValue = Complex64VectorValue.zero(this.logicalSize)
        override fun serializerFactory(): ValueSerializerFactory<Complex64VectorValue> = Complex64VectorValueSerializerFactory
        override fun statistics(): ValueStatistics<Complex64VectorValue> = ValueStatistics(this)
    }
}