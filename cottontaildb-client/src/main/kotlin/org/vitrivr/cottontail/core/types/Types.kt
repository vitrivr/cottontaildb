package org.vitrivr.cottontail.core.types

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.values.*

/**
 * Specifies the [Types] available in Cottontail DB. This construct is a centerpiece of Cottontail DB's type system
 * and allows for type safety in the eye de-/serialization, conversion and casting during query processing.
 *
 * Upon serialization, [Types]s can be stored as strings or numeric ordinals and mapped to the respective class using [Types.forName].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Serializable
sealed class Types<T : Value> {

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
    abstract val name: kotlin.String

    /** The logical size of a [Types] implementation, i.e., the number of elements in a vector. */
    abstract val logicalSize: kotlin.Int

    /** The physical size of a [Types] implementation, i.e., the size in bytes in-memory or on disk (w/o compression). */
    abstract val physicalSize: kotlin.Int

    /** The ordinal value of a [Types] implementation. */
    abstract val ordinal: kotlin.Int

    /**
     * A [Scalar] type.
     */
    @Serializable
    sealed class Scalar<T: ScalarValue<*>>: Types<T>() {
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
    @Serializable
    sealed class Numeric<T: NumericValue<*>>: Scalar<T>()

    /**
     * A [Complex] type.
     */
    @Serializable
    sealed class Complex<T: ComplexValue<*>>: Numeric<T>()

    /**
     * A [Vector] type
     */
    @Serializable
    sealed class Vector<T: VectorValue<*>, E: ScalarValue<*>>: Types<T>() {
        /** The element type of this [Vector] type. */
        abstract val elementType: Scalar<E>
        override fun equals(other: Any?): kotlin.Boolean =
            (this === other) || (other is Vector<*,*> && other.ordinal == this.ordinal && other.logicalSize == this.logicalSize)
        override fun hashCode(): kotlin.Int = 31 * this.ordinal.hashCode() + this.logicalSize.hashCode()
        override fun toString(): kotlin.String = "${this.name}(${this.logicalSize})"
    }

    @Serializable
    @SerialName("BOOLEAN")
    object Boolean: Scalar<BooleanValue>() {
        override val name = "BOOLEAN"
        override val ordinal: kotlin.Int = 0
        override val physicalSize = kotlin.Byte.SIZE_BYTES
    }

    @Serializable
    @SerialName("BYTE")
    object Byte: Numeric<ByteValue>() {
        override val name = "BYTE"
        override val ordinal: kotlin.Int = 1
        override val physicalSize = kotlin.Byte.SIZE_BYTES
    }

    @Serializable
    @SerialName("SHORT")
    object Short: Numeric<ShortValue>() {
        override val name = "SHORT"
        override val ordinal: kotlin.Int = 2
        override val physicalSize = kotlin.Short.SIZE_BYTES
    }

    @Serializable
    @SerialName("INTEGER")
    object Int: Numeric<IntValue>(){
        override val name = "INTEGER"
        override val ordinal: kotlin.Int = 3
        override val physicalSize = kotlin.Int.SIZE_BYTES
    }

    @Serializable
    @SerialName("LONG")
    object Long: Numeric<LongValue>() {
        override val name = "LONG"
        override val ordinal: kotlin.Int = 4
        override val physicalSize = kotlin.Long.SIZE_BYTES
    }

    @Serializable
    @SerialName("DATE")
    object Date: Scalar<DateValue>() {
        override val name = "DATE"
        override val ordinal: kotlin.Int = 5
        override val physicalSize = kotlin.Long.SIZE_BYTES
    }

    @Serializable
    @SerialName("FLOAT")
    object Float: Numeric<FloatValue>() {
        override val name = "FLOAT"
        override val ordinal: kotlin.Int = 6
        override val physicalSize = kotlin.Int.SIZE_BYTES
    }

    @Serializable
    @SerialName("DOUBLE")
    object Double: Numeric<DoubleValue>() {
        override val name = "DOUBLE"
        override val ordinal: kotlin.Int = 7
        override val physicalSize = kotlin.Long.SIZE_BYTES
    }

    @Serializable
    @SerialName("STRING")
    object String: Scalar<StringValue>() {
        override val name = "STRING"
        override val ordinal: kotlin.Int = 8
        override val logicalSize = LOGICAL_SIZE_UNKNOWN
        override val physicalSize = LOGICAL_SIZE_UNKNOWN * Char.SIZE_BYTES
    }

    @Serializable
    @SerialName("COMPLEX32")
    object Complex32: Complex<Complex32Value>() {
        override val name = "COMPLEX32"
        override val ordinal: kotlin.Int = 9
        override val logicalSize = 1
        override val physicalSize = 2 * kotlin.Int.SIZE_BYTES
    }

    @Serializable
    @SerialName("COMPLEX64")
    object Complex64: Complex<Complex64Value>() {
        override val name = "COMPLEX64"
        override val ordinal: kotlin.Int = 10
        override val logicalSize = 1
        override val physicalSize = 2 * kotlin.Long.SIZE_BYTES
    }

    @Serializable
    @SerialName("INTEGER_VECTOR")
    class IntVector(override val logicalSize: kotlin.Int): Vector<IntVectorValue, IntValue>() {
        override val name = "INTEGER_VECTOR"
        override val ordinal: kotlin.Int = 11
        override val physicalSize = this.logicalSize * kotlin.Int.SIZE_BYTES
        override val elementType = Int
    }

    @Serializable
    @SerialName("LONG_VECTOR")
    class LongVector(override val logicalSize: kotlin.Int): Vector<LongVectorValue, LongValue>() {
        override val name = "LONG_VECTOR"
        override val ordinal: kotlin.Int = 12
        override val physicalSize = this.logicalSize * kotlin.Long.SIZE_BYTES
        override val elementType = Long
    }

    @Serializable
    @SerialName("FLOAT_VECTOR")
    class FloatVector(override val logicalSize: kotlin.Int): Vector<FloatVectorValue, FloatValue>() {
        override val name = "FLOAT_VECTOR"
        override val ordinal: kotlin.Int = 13
        override val physicalSize = this.logicalSize * kotlin.Int.SIZE_BYTES
        override val elementType = Float
    }

    @Serializable
    @SerialName("DOUBLE_VECTOR")
    class DoubleVector(override val logicalSize: kotlin.Int): Vector<DoubleVectorValue, DoubleValue>() {
        override val name = "DOUBLE_VECTOR"
        override val ordinal: kotlin.Int = 14
        override val physicalSize = this.logicalSize * kotlin.Long.SIZE_BYTES
        override val elementType = Double
    }

    @Serializable
    @SerialName("BOOLEAN_VECTOR")
    class BooleanVector(override val logicalSize: kotlin.Int): Vector<BooleanVectorValue, BooleanValue>() {
        override val name = "BOOLEAN_VECTOR"
        override val ordinal: kotlin.Int = 15
        override val physicalSize = this.logicalSize * kotlin.Byte.SIZE_BYTES
        override val elementType = Boolean
    }

    @Serializable
    @SerialName("COMPLEX32_VECTOR")
    class Complex32Vector(override val logicalSize: kotlin.Int): Vector<Complex32VectorValue, Complex32Value>() {
        override val name = "COMPLEX32_VECTOR"
        override val ordinal: kotlin.Int = 16
        override val physicalSize = this.logicalSize * 2 * kotlin.Int.SIZE_BYTES
        override val elementType = Complex32
    }

    @Serializable
    @SerialName("COMPLEX64_VECTOR")
    class Complex64Vector(override val logicalSize: kotlin.Int): Vector<Complex64VectorValue, Complex32Value>() {
        override val name = "COMPLEX64_VECTOR"
        override val ordinal: kotlin.Int = 17
        override val elementType = Complex32
        override val physicalSize = this.logicalSize * 2 * kotlin.Long.SIZE_BYTES
    }

    @Serializable
    @SerialName("BYTESTRING")
    object ByteString: Scalar<ByteStringValue>() {
        override val name = "BYTESTRING"
        override val ordinal: kotlin.Int = 18
        override val logicalSize = LOGICAL_SIZE_UNKNOWN
        override val physicalSize = LOGICAL_SIZE_UNKNOWN * Char.SIZE_BYTES
    }
}