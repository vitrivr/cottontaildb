package ch.unibas.dmis.dbis.cottontail.database.definition

import org.mapdb.Serializer
import org.mapdb.serializer.GroupSerializer
import kotlin.reflect.KClass
import kotlin.reflect.full.safeCast

sealed class ColumnType<T : Any> {
    abstract val name : String
    abstract val serializer: GroupSerializer<T>
    abstract val type: KClass<T>
    fun cast(value: Any) : T? = type.safeCast(value);
    fun compatible(value: Any) = type.isInstance(value);

    override fun equals(other: Any?): Boolean {
        return if (other is ColumnType<*>) {
            other.name == this.name;
        } else {
            false
        }
    }

    companion object {
        /**
         * Return the [ColumnType] for the specified name.
         */
        fun typeForName(name: String): ColumnType<*>? {
            return when(name.toLowerCase()) {
                "int" -> IntColumnType()
                "long" -> LongColumnType()
                "float" -> FloatColumnType()
                "double" -> DoubleColumnType()
                "string" -> StringColumnType()
                "farray_dense" -> FloatArrayColumnType()
                "larray_dense" -> LongArrayColumnType()
                else -> null
            }
        }
    }
}

class LongColumnType : ColumnType<Long>() {
    override val name = "long"
    override val serializer: GroupSerializer<Long> = Serializer.LONG_PACKED
    override val type: KClass<Long> = Long::class
}

class IntColumnType : ColumnType<Int>() {
    override val name = "int"
    override val serializer: GroupSerializer<Int> = Serializer.INTEGER_PACKED
    override val type: KClass<Int> = Int::class
}

class FloatColumnType : ColumnType<Float>() {
    override val name = "float"
    override val serializer: GroupSerializer<Float> = Serializer.FLOAT
    override val type: KClass<Float> = Float::class
}

class DoubleColumnType : ColumnType<Double>() {
    override val name = "double"
    override val serializer: GroupSerializer<Double> = Serializer.DOUBLE
    override val type: KClass<Double> = Double::class
}

class StringColumnType : ColumnType<String>() {
    override val name = "string"
    override val serializer: GroupSerializer<String> = Serializer.STRING
    override val type: KClass<String> = String::class
}

class FloatArrayColumnType : ColumnType<FloatArray>() {
    override val name = "farray_dense"
    override val serializer: GroupSerializer<FloatArray> = Serializer.FLOAT_ARRAY
    override val type: KClass<FloatArray> = FloatArray::class
}

class LongArrayColumnType : ColumnType<LongArray>() {
    override val name = "larray_dense"
    override val serializer: GroupSerializer<LongArray> = Serializer.LONG_ARRAY
    override val type: KClass<LongArray> = LongArray::class
}