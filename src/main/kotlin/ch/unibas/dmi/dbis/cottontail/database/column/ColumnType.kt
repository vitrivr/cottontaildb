package ch.unibas.dmi.dbis.cottontail.database.column

import ch.unibas.dmi.dbis.cottontail.database.serializers.FixedDoubleVectorSerializer
import ch.unibas.dmi.dbis.cottontail.database.serializers.FixedFloatVectorSerializer

import org.mapdb.Serializer

import kotlin.ShortArray

import kotlin.reflect.KClass
import kotlin.reflect.full.safeCast

/**
 * Specifies the type of a Cottontail DB [Column]. This construct allows for some degree of type safety in the eye  de-/serialization.
 * The column types are stored as strings and mapped to the respective class using [ColumnType.typeForName].
 *
 * @see MapDBColumn
 *
 * @author Ralph Gasser
 * @version 1.0
 */
sealed class ColumnType<T : Any> {
    abstract val name : String
    abstract val type: KClass<T>


    companion object {
        /**
         * Returns the [ColumnType] for the provided name.
         *
         * @param name For which to lookup the [ColumnType].
         */
        fun forName(name: String): ColumnType<*> = when(name.toUpperCase()) {
            "BOOLEAN" -> BooleanColumnType()
            "BYTE" -> ByteColumnType()
            "SHORT" -> ShortColumnType()
            "INTEGER" -> IntColumnType()
            "LONG" -> LongColumnType()
            "FLOAT" -> FloatColumnType()
            "DOUBLE" -> DoubleColumnType()
            "STRING" -> StringColumnType()
            "BYTE_VEC" -> ByteArrayColumnType()
            "SHORT_VEC" -> ShortArrayColumnType()
            "INT_VEC" -> IntArrayColumnType()
            "LONG_VEC" -> LongArrayColumnType()
            "FLOAT_VEC" -> FloatArrayColumnType()
            "DOUBLE_VEC" -> DoubleArrayColumnType()
            else -> throw java.lang.IllegalArgumentException("The column type $name does not exists!")
        }
    }


    fun cast(value: Any?) : T? = this.type.safeCast(value)
    fun compatible(value: Any) = this.type.isInstance(value)

    /**
     * Returns a [Serializer] for this [ColumnType]. Some [ColumnType] require a size attribute
     *
     * @param size The size of the column (e.g. for vectors). Defaults to -1.
     */
    abstract fun serializer(size: Int = -1): Serializer<T>


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as ColumnType<*>
        if (name != other.name) return false
        return true
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }

    override fun toString(): String = this.name
}

class BooleanColumnType : ColumnType<Boolean>() {
    override val name = "BOOLEAN"
    override val type: KClass<Boolean> = Boolean::class
    override fun serializer(size: Int): Serializer<Boolean> = Serializer.BOOLEAN
}

class ByteColumnType : ColumnType<Byte>() {
    override val name = "BYTE"
    override val type: KClass<Byte> = Byte::class
    override fun serializer(size: Int): Serializer<Byte> = Serializer.BYTE
}

class ShortColumnType : ColumnType<Short>() {
    override val name = "SHORT"
    override val type: KClass<Short> = Short::class
    override fun serializer(size: Int): Serializer<Short> = Serializer.SHORT
}

class IntColumnType : ColumnType<Int>() {
    override val name = "INTEGER"
    override val type: KClass<Int> = Int::class
    override fun serializer(size: Int): Serializer<Int> = Serializer.INTEGER
}

class LongColumnType : ColumnType<Long>() {
    override val name = "LONG"
    override val type: KClass<Long> = Long::class
    override fun serializer(size: Int): Serializer<Long> = Serializer.LONG_PACKED
}

class FloatColumnType : ColumnType<Float>() {
    override val name = "FLOAT"
    override val type: KClass<Float> = Float::class
    override fun serializer(size: Int): Serializer<Float> = Serializer.FLOAT
}

class DoubleColumnType : ColumnType<Double>() {
    override val name = "DOUBLE"
    override val type: KClass<Double> = Double::class
    override fun serializer(size: Int): Serializer<Double> = Serializer.DOUBLE
}

class StringColumnType : ColumnType<String>() {
    override val name = "STRING"
    override val type: KClass<String> = String::class
    override fun serializer(size: Int): Serializer<String> = Serializer.STRING
}

class ByteArrayColumnType : ColumnType<ByteArray>() {
    override val name = "BYTE_VEC"
    override val type: KClass<ByteArray> = ByteArray::class
    override fun serializer(size: Int): Serializer<ByteArray> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return Serializer.BYTE_ARRAY
    }
}

class ShortArrayColumnType : ColumnType<ShortArray>() {
    override val name = "SHORT_VEC"
    override val type: KClass<ShortArray> = ShortArray::class
    override fun serializer(size: Int): Serializer<ShortArray> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return Serializer.SHORT_ARRAY
    }
}

class IntArrayColumnType : ColumnType<IntArray>() {
    override val name = "INT_VEC"
    override val type: KClass<IntArray> = IntArray::class
    override fun serializer(size: Int): Serializer<IntArray> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return Serializer.INT_ARRAY
    }
}

class LongArrayColumnType : ColumnType<LongArray>() {
    override val name = "LONG_VEC"
    override val type: KClass<LongArray> = LongArray::class
    override fun serializer(size: Int): Serializer<LongArray> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return Serializer.LONG_ARRAY
    }
}

class FloatArrayColumnType : ColumnType<FloatArray>() {
    override val name = "FLOAT_VEC"
    override val type: KClass<FloatArray> = FloatArray::class
    override fun serializer(size: Int): Serializer<FloatArray> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return FixedFloatVectorSerializer(size)
    }
}

class DoubleArrayColumnType : ColumnType<DoubleArray>() {
    override val name = "DOUBLE_VEC"
    override val type: KClass<DoubleArray> = DoubleArray::class
    override fun serializer(size: Int): Serializer<DoubleArray> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return FixedDoubleVectorSerializer(size)
    }
}

