package ch.unibas.dmi.dbis.cottontail.database.schema

import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException

import org.mapdb.Serializer
import org.mapdb.serializer.GroupSerializer
import kotlin.ShortArray

import kotlin.reflect.KClass
import kotlin.reflect.full.safeCast

/**
 * Specifies the type of a Cottontail DB [Column]. This construct allows for some degree of type safety in the eye
 * de-/serialization. The column types are stored as strings and mapped to the respective class using [ColumnType.typeForName].
 *
 * @see Column
 */
sealed class ColumnType<T : Any> {
    abstract val name : String
    abstract val serializer: GroupSerializer<T>
    abstract val type: KClass<T>

    companion object {
        /**
         * Returns the [ColumnType] for the provided name.
         *
         * @param name For which to lookup the [ColumnType].
         */
        fun typeForName(name: String): ColumnType<*> = when(name.toUpperCase()) {
            "BOOLEAN" -> BooleanColumnType()
            "BYTE" -> ByteColumnType()
            "SHORT" -> ShortColumnType()
            "INTEGER" -> IntColumnType()
            "LONG" -> LongColumnType()
            "STRING" -> StringColumnType()
            "DBARRAY" -> ByteArrayColumnType()
            "DSARRAY" -> ShortArrayColumnType()
            "DIARRAY" -> IntArrayColumnType()
            "DLARRAY" -> LongArrayColumnType()
            "DFARRAY" -> FloatArrayColumnType()
            "DDARRAY" -> DoubleArrayColumnType()
            else -> throw DatabaseException("The column type $name does not exists!")
        }

        /** */
        fun specForName(column: String, type: String): ColumnDef = Pair(column, typeForName(type))
    }

    fun cast(value: Any?) : T? = type.safeCast(value);
    fun compatible(value: Any) = type.isInstance(value);


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
}

class BooleanColumnType : ColumnType<Boolean>() {
    override val name = "BOOLEAN"
    override val serializer: GroupSerializer<Boolean> = Serializer.BOOLEAN
    override val type: KClass<Boolean> = Boolean::class
}

class ByteColumnType : ColumnType<Byte>() {
    override val name = "BYTE"
    override val serializer: GroupSerializer<Byte> = Serializer.BYTE
    override val type: KClass<Byte> = Byte::class
}

class ShortColumnType : ColumnType<Short>() {
    override val name = "SHORT"
    override val serializer: GroupSerializer<Short> = Serializer.SHORT
    override val type: KClass<Short> = Short::class
}

class IntColumnType : ColumnType<Int>() {
    override val name = "INTEGER"
    override val serializer: GroupSerializer<Int> = Serializer.INTEGER
    override val type: KClass<Int> = Int::class
}

class LongColumnType : ColumnType<Long>() {
    override val name = "LONG"
    override val serializer: GroupSerializer<Long> = Serializer.LONG_PACKED
    override val type: KClass<Long> = Long::class
}

class FloatColumnType : ColumnType<Float>() {
    override val name = "FLOAT"
    override val serializer: GroupSerializer<Float> = Serializer.FLOAT
    override val type: KClass<Float> = Float::class
}

class DoubleColumnType : ColumnType<Double>() {
    override val name = "DOUBLE"
    override val serializer: GroupSerializer<Double> = Serializer.DOUBLE
    override val type: KClass<Double> = Double::class
}

class StringColumnType : ColumnType<String>() {
    override val name = "STRING"
    override val serializer: GroupSerializer<String> = Serializer.STRING
    override val type: KClass<String> = String::class
}

class ByteArrayColumnType : ColumnType<ByteArray>() {
    override val name = "DBARRAY"
    override val serializer: GroupSerializer<ByteArray> = Serializer.BYTE_ARRAY
    override val type: KClass<ByteArray> = ByteArray::class
}

class ShortArrayColumnType : ColumnType<ShortArray>() {
    override val name = "DSARRAY"
    override val serializer: GroupSerializer<ShortArray> = Serializer.SHORT_ARRAY
    override val type: KClass<ShortArray> = ShortArray::class
}

class IntArrayColumnType : ColumnType<IntArray>() {
    override val name = "DIARRAY"
    override val serializer: GroupSerializer<IntArray> = Serializer.INT_ARRAY
    override val type: KClass<IntArray> = IntArray::class
}

class LongArrayColumnType : ColumnType<LongArray>() {
    override val name = "DLARRAY"
    override val serializer: GroupSerializer<LongArray> = Serializer.LONG_ARRAY
    override val type: KClass<LongArray> = LongArray::class
}

class FloatArrayColumnType : ColumnType<FloatArray>() {
    override val name = "DFARRAY"
    override val serializer: GroupSerializer<FloatArray> = Serializer.FLOAT_ARRAY
    override val type: KClass<FloatArray> = FloatArray::class
}

class DoubleArrayColumnType : ColumnType<DoubleArray>() {
    override val name = "DDARRAY"
    override val serializer: GroupSerializer<DoubleArray> = Serializer.DOUBLE_ARRAY
    override val type: KClass<DoubleArray> = DoubleArray::class
}

