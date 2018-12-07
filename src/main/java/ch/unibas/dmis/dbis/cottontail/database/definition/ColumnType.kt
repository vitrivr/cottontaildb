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
                "long" -> LongColumnType()
                "string" -> StringColumnType()
                "farray_dense" -> FloatArrayColumnType()
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