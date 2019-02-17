package ch.unibas.dmi.dbis.cottontail.model.basics

import ch.unibas.dmi.dbis.cottontail.database.column.*
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import java.lang.RuntimeException

/**
 * A definition class for a Cottontail DB column be it in a DB or in-memory context.  Specifies all the properties of such a and facilitates validation.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class ColumnDef<T: Any>(val name: String, val type: ColumnType<T>, val size: Int = -1, val nullable: Boolean = true) {
    companion object {
        /**
         * Returns a [ColumnDef] with the provided attributes. The only difference as compared to using the constructor,
         * is that the [ColumnType] can be provided by name.
         *
         * @param column Name of the new [Column]
         * @param type Name of the [ColumnType] of the new [Column]
         * @param size Size of the new [Column] (e.g. for vectors), where eligible.
         * @param nullable Whether or not the [Column] should be nullable.
         */
        fun withAttributes(column: String, type: String, size: Int = -1, nullable: Boolean = true): ColumnDef<*> = ColumnDef(column, ColumnType.forName(type), size, nullable)
    }

    /**
     * Validates a value with regard to this [ColumnDef] and throws an Exception, if validation fails.
     *
     * @param value The value that should be validated.
     * @throws [DatabaseException.ValidationException] If validation fails.
     */
    fun validateOrThrow(value: Any?) {
        if (value != null) {
            if (!this.type.compatible(value)) {
                throw DatabaseException.ValidationException("The type $type of column '$name' is not compatible with value $value.")
            }
            val cast = this.type.cast(value)
            when {
                cast is DoubleArray && cast.size != this.size -> throw DatabaseException.ValidationException("The size of column '$name' (sc=${this.size}) is not compatible with size of value (sv=${cast.size}).")
                cast is FloatArray && cast.size != this.size -> throw DatabaseException.ValidationException("The size of column '$name' (sc=${this.size}) is not compatible with size of value (sv=${cast.size}).")
                cast is LongArray && cast.size != this.size -> throw DatabaseException.ValidationException("The size of column '$name' (sc=${this.size}) is not compatible with size of value (sv=${cast.size}).")
                cast is IntArray && cast.size != this.size -> throw DatabaseException.ValidationException("The size of column '$name' (sc=${this.size}) is not compatible with size of value (sv=${cast.size}).")
                cast is ShortArray && cast.size != this.size -> throw DatabaseException.ValidationException("The size of column '$name' (sc=${this.size}) is not compatible with size of value (sv=${cast.size}).")
                cast is ByteArray && cast.size != this.size -> throw DatabaseException.ValidationException("The size of column '$name' (sc=${this.size}) is not compatible with size of value (sv=${cast.size}).")
            }
        } else if (!this.nullable) {
            throw DatabaseException.ValidationException("The column '$name' cannot be null!")
        }
    }

    /**
     * Validates a value with regard to this [ColumnDef] return a flag indicating whether validation was passed.
     *
     * @param value The value that should be validated.
     * @return True if value passes validation, false otherwise.
     */
    fun validate(value: Any?) : Boolean {
        if (value != null) {
            if (!this.type.compatible(value)) {
                return false
            }
            val cast = this.type.cast(value)
            return when {
                cast is DoubleArray && cast.size != this.size -> false
                cast is FloatArray && cast.size != this.size -> false
                cast is LongArray && cast.size != this.size -> false
                cast is IntArray && cast.size != this.size -> false
                cast is ShortArray && cast.size != this.size -> false
                cast is ByteArray && cast.size != this.size -> false
                else -> true
            }
        } else return this.nullable
    }

    /**
     * Returns the default value for this [ColumnDef].
     *
     * @return Default value for this [ColumnDef].
     */
    fun defaultValue(): Any? = when {
        this.nullable -> null
        this.type is StringColumnType -> ""
        this.type is FloatColumnType -> 0.0f
        this.type is DoubleColumnType -> 0.0
        this.type is IntColumnType -> 0
        this.type is LongColumnType -> 0L
        this.type is ShortColumnType -> 0.toShort()
        this.type is ByteColumnType -> 0.toByte()
        this.type is BooleanColumnType -> false
        this.type is DoubleArrayColumnType -> DoubleArray(this.size)
        this.type is FloatArrayColumnType -> DoubleArray(this.size)
        this.type is LongArrayColumnType -> LongArray(this.size)
        this.type is IntArrayColumnType -> IntArray(this.size)
        else -> throw RuntimeException("Default value for the specified type $type has not been specified yet!")
    }


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ColumnDef<*>

        if (name != other.name) return false
        if (type != other.type) return false
        if (size != other.size) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + size.hashCode()
        return result
    }

    override fun toString(): String = "$name(type=$type, size=$size, nullable=$nullable)"
}