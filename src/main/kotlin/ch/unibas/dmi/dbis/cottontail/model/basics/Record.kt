package ch.unibas.dmi.dbis.cottontail.model.basics

import java.lang.IllegalArgumentException

/**
 * A [Record] as returned and processed by Cottontail DB. A [Record] corresponds to a single row and
 * it can hold multiple values, each belonging to a different column (defined by [ColumnDef]s). A [ColumnDef]
 * must not necessarily correspond to a physical database [Column].
 *
 * Column-wise access to records through the [Entity] class returns [Record]s. Furthermore, the interface is
 * used in conjunction with the [Recordset] class.
 *
 * @see Recordset
 * @see Entity
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface Record {

    /** The Tuple ID of the [Record]. May be null [Record]s that cannot be mapped to a specific tuple anymore. */
    val tupleId: Long?

    /** Array of [ColumnDef]s that describes the [Columns] of this [Record]. */
    val columns: Array<out ColumnDef<*>>

    /** Array of column values (one entry per column). */
    val values: Array<Any?>

    /** Size of this [Record] in terms of [ColumnDef] it encompasses. */
    val size: Int
        get() = columns.size

    /**
     * Assigns the provided values to this [Record], i.e. the first value is assigned to the first column,
     * the second to the second column etc.
     *
     * @param values The values to assign. Cannot contain more than [Record.size] values.
     */
    fun assign(vararg values: Any?): Record {
        if (values.size <= this.size) {
            for (i in 0 until values.size) {
                this.columns[i].validateOrThrow(values[i])
                this.values[i] = values[i]
            }
            return this
        } else {
            throw IllegalArgumentException("The number of values ${values.size} exceeds this record's size ${this.size}.")
        }
    }

    /**
     * Returns true, if this [Record] contains the specified [ColumnDef] and false otherwise.
     *
     * @param column The [ColumnDef] specifying the column
     * @return True if record contains the [ColumnDef], false otherwise.
     */
    fun has(column: ColumnDef<*>): Boolean = columns.indexOf(column) > -1

    /**
     * Retrieves the value for the specified [ColumnDef] from this [Record].
     *
     * @param column The [ColumnDef] for which to retrieve the value.
     * @return The value for the [ColumnDef]
     */
    operator fun <T: Any> get(column: ColumnDef<T>): T? {
        val index = columns.indexOf(column)
        return if (index > -1) {
            column.type.cast(values[index])
        } else {
            throw IllegalArgumentException("The specified column ${column.name} is not contained in this record.")
        }
    }

    /**
     * Sets the value for the specified [ColumnDef] in this [Record].
     *
     * @param column The [ColumnDef] for which to set the value.
     * @param value The new value for the [ColumnDef]
     */
    operator fun set(column: ColumnDef<*>, value: Any?) {
        val index = columns.indexOf(column)
        if (index > -1) {
            column.validateOrThrow(value)
            values[index] = value
        } else {
            throw IllegalArgumentException("The specified column ${column.name} (type=${column.type.name}) is not contained in this record.")
        }
    }
}