package org.vitrivr.cottontail.core.basics

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A [Record] as returned and processed by Cottontail DB. A [Record] corresponds to a single row and it can hold
 * multiple values, each belonging to a different column (defined by [ColumnDef]s). A [ColumnDef] must not necessarily
 * correspond to a physical database column.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
interface Record {

    /** The [TupleId] of this [Record]. Can be updated! */
    val tupleId: TupleId

    /** [Array] of [ColumnDef]s contained in this [Record]. */
    val columns: Array<ColumnDef<*>>

    /** Size of this [Record] in terms of [ColumnDef] it encompasses. */
    val size: Int
        get() = this.columns.size

    /**
     * Creates and returns a copy of this [Record].
     *
     * The copy of a [Record] is supposed to hold its own copy of the values it holds. However, structural information,
     * such as the columns, may be shared between instances, as they are supposed to be immutable.
     *
     * @return Copy of this [Record].
     */
    fun copy(): Record

    /**
     * Returns true, if this [Record] contains the specified [ColumnDef] and false otherwise.
     *
     * @param column The [ColumnDef] specifying the column
     * @return True if record contains the [ColumnDef], false otherwise.
     */
    fun has(column: ColumnDef<*>): Boolean

    /**
     * Returns column index of the given [ColumnDef] within this [Record]. Returns -1 if [ColumnDef] is not contained
     *
     * @param column The [ColumnDef] to check.
     * @return The column index or -1. of [ColumnDef] is not part of this [Record].
     */
    fun indexOf(column: ColumnDef<*>): Int

    /**
     * Generates and returns a [Map] of the data contained in this [Record]
     *
     * @return Map of column name to value.
     */
    fun toMap(): Map<ColumnDef<*>, Value?>

    /**
     * Retrieves the [Value] for the specified column index from this [Record].
     *
     * @param index The column index for which to retrieve the value.
     * @return The value for the index.
     */
    operator fun get(index: Int): Value?

    /**
     * Sets the [Value] for the specified index in this [Record].
     *
     * @param index The column index for which to set the value.
     * @param value The new [Value]
     */
    operator fun set(index: Int, value: Value?)

    /**
     * Retrieves the [Value]  for the specified [ColumnDef] from this [Record].
     *
     * @param column The [ColumnDef] for which to retrieve the value.
     * @return The value for the [ColumnDef]
     */
    operator fun get(column: ColumnDef<*>): Value?

    /**
     * Sets the [Value]  for the specified [ColumnDef] in this [Record].
     *
     * @param column The [ColumnDef] for which to set the value.
     * @param value The new [Value]
     */
    operator fun set(column: ColumnDef<*>, value: Value?)
}