package ch.unibas.dmi.dbis.cottontail.database.column

import ch.unibas.dmi.dbis.cottontail.database.serializers.*
import ch.unibas.dmi.dbis.cottontail.model.values.*
import ch.unibas.dmi.dbis.cottontail.model.values.complex.Complex

import org.mapdb.Serializer
import java.util.*

import kotlin.reflect.KClass
import kotlin.reflect.full.safeCast

/**
 * Specifies the type of a Cottontail DB [Column]. This construct allows for some degree of type safety in the eye de-/serialization.
 * The column types are stored as strings and mapped to the respective class using [ColumnType.typeForName].
 *
 * @see Column
 *
 * @author Ralph Gasser
 * @version 1.0
 */
sealed class ColumnType<T : Any> {
    abstract val name: String
    abstract val type: KClass<out Value<T>>
    abstract val numeric: Boolean

    companion object {
        /**
         * Returns the [ColumnType] for the provided name.
         *
         * @param name For which to lookup the [ColumnType].
         */
        fun forName(name: String): ColumnType<*> = when (name.toUpperCase()) {
            "BOOLEAN" -> BooleanColumnType()
            "BYTE" -> ByteColumnType()
            "SHORT" -> ShortColumnType()
            "INTEGER" -> IntColumnType()
            "LONG" -> LongColumnType()
            "FLOAT" -> FloatColumnType()
            "DOUBLE" -> DoubleColumnType()
            "STRING" -> StringColumnType()
            "COMPLEX" -> ComplexColumnType()
            "INT_VEC" -> IntVectorColumnType()
            "LONG_VEC" -> LongVectorColumnType()
            "FLOAT_VEC" -> FloatVectorColumnType()
            "DOUBLE_VEC" -> DoubleVectorColumnType()
            "BOOL_VEC" -> BooleanVectorColumnType()
            "COMPLEX_VEC" -> ComplexVectorColumnType()
            else -> throw java.lang.IllegalArgumentException("The column type $name does not exists!")
        }
    }


    fun cast(value: Value<*>?): Value<T>? = this.type.safeCast(value)
    fun compatible(value: Value<*>) = this.type.isInstance(value)

    /**
     * Returns a [Serializer] for this [ColumnType]. Some [ColumnType] require a size attribute
     *
     * @param size The size of the column (e.g. for vectors). Defaults to -1.
     */
    abstract fun serializer(size: Int = -1): Serializer<Value<T>>


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

@Suppress("UNCHECKED_CAST")
class BooleanColumnType : ColumnType<Boolean>() {
    override val name = "BOOLEAN"
    override val numeric = true
    override val type: KClass<BooleanValue> = BooleanValue::class
    override fun serializer(size: Int): Serializer<Value<Boolean>> = BooleanValueSerializer as Serializer<Value<Boolean>>
}

@Suppress("UNCHECKED_CAST")
class ByteColumnType : ColumnType<Byte>() {
    override val name = "BYTE"
    override val numeric = true
    override val type: KClass<ByteValue> = ByteValue::class
    override fun serializer(size: Int): Serializer<Value<Byte>> = ByteValueSerializer as Serializer<Value<Byte>>
}

@Suppress("UNCHECKED_CAST")
class ShortColumnType : ColumnType<Short>() {
    override val name = "SHORT"
    override val numeric = true
    override val type: KClass<ShortValue> = ShortValue::class
    override fun serializer(size: Int): Serializer<Value<Short>> = ShortValueSerializer as Serializer<Value<Short>>
}

@Suppress("UNCHECKED_CAST")
class IntColumnType : ColumnType<Int>() {
    override val name = "INTEGER"
    override val numeric = true
    override val type: KClass<IntValue> = IntValue::class
    override fun serializer(size: Int): Serializer<Value<Int>> = IntValueSerializer as Serializer<Value<Int>>
}

@Suppress("UNCHECKED_CAST")
class LongColumnType : ColumnType<Long>() {
    override val name = "LONG"
    override val numeric = true
    override val type: KClass<LongValue> = LongValue::class
    override fun serializer(size: Int): Serializer<Value<Long>> = LongValueSerializer as Serializer<Value<Long>>
}

@Suppress("UNCHECKED_CAST")
class FloatColumnType : ColumnType<Float>() {
    override val name = "FLOAT"
    override val numeric = true
    override val type: KClass<FloatValue> = FloatValue::class
    override fun serializer(size: Int): Serializer<Value<Float>> = FloatValueSerializer as Serializer<Value<Float>>
}

@Suppress("UNCHECKED_CAST")
class DoubleColumnType : ColumnType<Double>() {
    override val name = "DOUBLE"
    override val numeric = true
    override val type: KClass<DoubleValue> = DoubleValue::class
    override fun serializer(size: Int): Serializer<Value<Double>> = DoubleValueSerializer as Serializer<Value<Double>>
}

@Suppress("UNCHECKED_CAST")
class StringColumnType : ColumnType<String>() {
    override val name = "STRING"
    override val numeric = false
    override val type: KClass<StringValue> = StringValue::class
    override fun serializer(size: Int): Serializer<Value<String>> = StringValueSerializer as Serializer<Value<String>>
}

@Suppress("UNCHECKED_CAST")
class ComplexColumnType : ColumnType<Complex>() {
    override val name = "COMPLEX"
    override val numeric = true
    override val type: KClass<ComplexValue> = ComplexValue::class
    override fun serializer(size: Int): Serializer<Value<Complex>> = ComplexValueSerializer as Serializer<Value<Complex>>
}

@Suppress("UNCHECKED_CAST")
class IntVectorColumnType : ColumnType<IntArray>() {
    override val name = "INT_VEC"
    override val numeric = false
    override val type: KClass<IntVectorValue> = IntVectorValue::class
    override fun serializer(size: Int): Serializer<Value<IntArray>> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return FixedIntVectorSerializer(size) as Serializer<Value<IntArray>>
    }
}

@Suppress("UNCHECKED_CAST")
class LongVectorColumnType : ColumnType<LongArray>() {
    override val name = "LONG_VEC"
    override val numeric = false
    override val type: KClass<LongVectorValue> = LongVectorValue::class
    override fun serializer(size: Int): Serializer<Value<LongArray>> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return FixedLongVectorSerializer(size) as Serializer<Value<LongArray>>
    }
}

@Suppress("UNCHECKED_CAST")
class FloatVectorColumnType : ColumnType<FloatArray>() {
    override val name = "FLOAT_VEC"
    override val numeric = false
    override val type: KClass<FloatVectorValue> = FloatVectorValue::class
    override fun serializer(size: Int): Serializer<Value<FloatArray>> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return FixedFloatVectorSerializer(size) as Serializer<Value<FloatArray>>
    }
}

@Suppress("UNCHECKED_CAST")
class DoubleVectorColumnType : ColumnType<DoubleArray>() {
    override val name = "DOUBLE_VEC"
    override val numeric = false
    override val type: KClass<DoubleVectorValue> = DoubleVectorValue::class
    override fun serializer(size: Int): Serializer<Value<DoubleArray>> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return FixedDoubleVectorSerializer(size) as Serializer<Value<DoubleArray>>
    }
}

@Suppress("UNCHECKED_CAST")
class BooleanVectorColumnType : ColumnType<BitSet>() {
    override val name = "BOOL_VEC"
    override val numeric = false
    override val type: KClass<BooleanVectorValue> = BooleanVectorValue::class
    override fun serializer(size: Int): Serializer<Value<BitSet>> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return FixedBooleanVectorSerializer(size) as Serializer<Value<BitSet>>
    }
}

@Suppress("UNCHECKED_CAST")
class ComplexVectorColumnType : ColumnType<Array<Complex>>() {
    override val name = "COMPLEX_VEC"
    override val numeric = false
    override val type: KClass<ComplexVectorValue> = ComplexVectorValue::class
    override fun serializer(size: Int): Serializer<Value<Array<Complex>>> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return FixedComplexVectorSerializer(size) as Serializer<Value<Array<Complex>>>
    }
}