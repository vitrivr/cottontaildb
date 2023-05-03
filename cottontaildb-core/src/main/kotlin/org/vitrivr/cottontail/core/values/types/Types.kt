package org.vitrivr.cottontail.core.values.types

import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.generators.*

/**
 * Specifies the [Types] available in Cottontail DB. This construct is a centerpiece of Cottontail DB's type system
 * and allows for type safety in the eye de-/serialization, conversion and casting during query processing.
 *
 * Upon serialization, [Types]s can be stored as strings or numeric ordinals and mapped to the respective class using [Types.forName].
 *
 * @author Ralph Gasser
 * @version 1.6.0
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
            "INTEGER_VECTOR" -> IntVector(logicalSize)
            "LONG_VECTOR" -> LongVector(logicalSize)
            "FLOAT_VECTOR" -> FloatVector(logicalSize)
            "DOUBLE_VECTOR" -> DoubleVector(logicalSize)
            "BOOL_VECTOR" -> BooleanVector(logicalSize)
            "COMPLEX32_VECTOR" -> Complex32Vector(logicalSize)
            "COMPLEX64_VECTOR" -> Complex64Vector(logicalSize)
            "BYTESTRING" -> ByteString
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
            18 -> ByteString
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

    /** The [ValueGenerator] used to create instances of this [Types]. */
    val generator: ValueGenerator<T>

    /** Returns the default value for this [Types]. */
    fun defaultValue(): T

    /**
     * A [Scalar] type.
     */
    sealed class Scalar<T: ScalarValue<*>>: Types<T> {
        override val logicalSize
            get() = 1
        override fun equals(other: Any?): kotlin.Boolean
            = (this === other) || (other is Scalar<*> && other.ordinal == this.ordinal)
        override fun hashCode(): kotlin.Int = this.ordinal.hashCode()
        override fun toString(): kotlin.String = this.name
    }

    /**
     * A [Numeric] type.
     */
    sealed class Numeric<T: NumericValue<*>>: Scalar<T>() {
        abstract override val generator: NumericValueGenerator<T>
        override fun defaultValue() = this.generator.zero()
    }

    /**
     * A [Complex] type.
     */
    sealed class Complex<T: ComplexValue<*>>: Numeric<T>()

    /**
     * A [Vector] type
     */
    sealed class Vector<T: VectorValue<*>, E: ScalarValue<*>>: Types<T> {
        /** The element type of this [Vector] type. */
        abstract val elementType: Scalar<E>
        abstract override val generator: VectorValueGenerator<T>
        override fun defaultValue() = this.generator.zero(this.logicalSize)
        override fun equals(other: Any?): kotlin.Boolean =
            (this === other) || (other is Vector<*,*> && other.ordinal == this.ordinal && other.logicalSize == this.logicalSize)
        override fun hashCode(): kotlin.Int = 31 * this.ordinal.hashCode() + this.logicalSize.hashCode()
        override fun toString(): kotlin.String = "${this.name}(${this.logicalSize})"
    }

    object Boolean: Scalar<BooleanValue>() {
        override val name = "BOOLEAN"
        override val ordinal: kotlin.Int = 0
        override val physicalSize = kotlin.Byte.SIZE_BYTES
        override val generator = BooleanValueGenerator
        override fun defaultValue(): BooleanValue = this.generator.ofTrue()
    }

    object Byte: Numeric<ByteValue>() {
        override val name = "BYTE"
        override val ordinal: kotlin.Int = 1
        override val physicalSize = kotlin.Byte.SIZE_BYTES
        override val generator = ByteValueGenerator
    }

    object Short: Numeric<ShortValue>() {
        override val name = "SHORT"
        override val ordinal: kotlin.Int = 2
        override val physicalSize = kotlin.Short.SIZE_BYTES
        override val generator = ShortValueGenerator
    }

    object Int: Numeric<IntValue>(){
        override val name = "INTEGER"
        override val ordinal: kotlin.Int = 3
        override val physicalSize = kotlin.Int.SIZE_BYTES
        override val generator = IntValueGenerator
    }

    object Long: Numeric<LongValue>() {
        override val name = "LONG"
        override val ordinal: kotlin.Int = 4
        override val physicalSize = kotlin.Long.SIZE_BYTES
        override val generator = LongValueGenerator
    }

    object Date: Scalar<DateValue>() {
        override val name = "DATE"
        override val ordinal: kotlin.Int = 5
        override val physicalSize = kotlin.Long.SIZE_BYTES
        override val generator = DateValueGenerator
        override fun defaultValue(): DateValue = this.generator.now()
    }

    object Float: Numeric<FloatValue>() {
        override val name = "FLOAT"
        override val ordinal: kotlin.Int = 6
        override val physicalSize = kotlin.Int.SIZE_BYTES
        override val generator = FloatValueGenerator
    }

    object Double: Numeric<DoubleValue>() {
        override val name = "DOUBLE"
        override val ordinal: kotlin.Int = 7
        override val physicalSize = kotlin.Long.SIZE_BYTES
        override val generator = DoubleValueGenerator
    }

    object String: Scalar<StringValue>() {
        override val name = "STRING"
        override val ordinal: kotlin.Int = 8
        override val logicalSize = LOGICAL_SIZE_UNKNOWN
        override val physicalSize = LOGICAL_SIZE_UNKNOWN * Char.SIZE_BYTES
        override val generator = StringValueGenerator
        override fun defaultValue(): StringValue = this.generator.empty()
    }

    object Complex32: Complex<Complex32Value>() {
        override val name = "COMPLEX32"
        override val ordinal: kotlin.Int = 9
        override val logicalSize = 1
        override val physicalSize = 2 * kotlin.Int.SIZE_BYTES
        override val generator = Complex32ValueGenerator
    }

    object Complex64: Complex<Complex64Value>() {
        override val name = "COMPLEX64"
        override val ordinal: kotlin.Int = 10
        override val logicalSize = 1
        override val physicalSize = 2 * kotlin.Long.SIZE_BYTES
        override val generator = Complex64ValueGenerator
    }

    class IntVector(override val logicalSize: kotlin.Int): Vector<IntVectorValue, IntValue>() {
        override val name = "INTEGER_VEC"
        override val ordinal: kotlin.Int = 11
        override val physicalSize = this.logicalSize * kotlin.Int.SIZE_BYTES
        override val elementType = Int
        override val generator = IntVectorValueGenerator
    }

    class LongVector(override val logicalSize: kotlin.Int): Vector<LongVectorValue, LongValue>() {
        override val name = "LONG_VEC"
        override val ordinal: kotlin.Int = 12
        override val physicalSize = this.logicalSize * kotlin.Long.SIZE_BYTES
        override val elementType = Long
        override val generator = LongVectorValueGenerator
    }

    class FloatVector(override val logicalSize: kotlin.Int): Vector<FloatVectorValue, FloatValue>() {
        override val name = "FLOAT_VECTOR"
        override val ordinal: kotlin.Int = 13
        override val physicalSize = this.logicalSize * kotlin.Int.SIZE_BYTES
        override val elementType = Float
        override val generator = FloatVectorValueGenerator
    }

    class DoubleVector(override val logicalSize: kotlin.Int): Vector<DoubleVectorValue, DoubleValue>() {
        override val name = "DOUBLE_VECTOR"
        override val ordinal: kotlin.Int = 14
        override val physicalSize = this.logicalSize * kotlin.Long.SIZE_BYTES
        override val elementType = Double
        override val generator = DoubleVectorValueGenerator
    }

    class BooleanVector(override val logicalSize: kotlin.Int): Vector<BooleanVectorValue, BooleanValue>() {
        override val name = "BOOLEAN_VECTOR"
        override val ordinal: kotlin.Int = 15
        override val physicalSize = this.logicalSize * kotlin.Byte.SIZE_BYTES
        override val elementType = Boolean
        override val generator = BooleanVectorValueGenerator
    }

    class Complex32Vector(override val logicalSize: kotlin.Int): Vector<Complex32VectorValue, Complex32Value>() {
        override val name = "COMPLEX32_VECTOR"
        override val ordinal: kotlin.Int = 16
        override val physicalSize = this.logicalSize * 2 * kotlin.Int.SIZE_BYTES
        override val elementType = Complex32
        override val generator = Complex32VectorValueGenerator
    }

    class Complex64Vector(override val logicalSize: kotlin.Int): Vector<Complex64VectorValue, Complex32Value>() {
        override val name = "COMPLEX64_VECTOR"
        override val ordinal: kotlin.Int = 17
        override val elementType = Complex32
        override val physicalSize = this.logicalSize * 2 * kotlin.Long.SIZE_BYTES
        override val generator = Complex64VectorValueGenerator
    }

    object ByteString: Scalar<ByteStringValue>() {
        override val name = "BYTESTRING"
        override val ordinal: kotlin.Int = 18
        override val logicalSize = LOGICAL_SIZE_UNKNOWN
        override val physicalSize = LOGICAL_SIZE_UNKNOWN * Char.SIZE_BYTES
        override val generator = ByteStringValueGenerator
        override fun defaultValue(): ByteStringValue = this.generator.empty()
    }
}