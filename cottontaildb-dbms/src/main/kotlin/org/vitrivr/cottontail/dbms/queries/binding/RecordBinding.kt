package org.vitrivr.cottontail.dbms.queries.binding

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A [Record] implementation that depends on the existence of [Binding]s for the [Value]s it contains. Used for inserts.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class RecordBinding(override var tupleId: TupleId, override val columns: Array<ColumnDef<*>>, private val values: Array<Binding.Literal>) : Record {

    /**
     * Creates a copy of this [RecordBinding]
     */
    override fun copy(): Record = RecordBinding(this.tupleId, this.columns.copyOf(), this.values.copyOf())

    /**
     * Returns true, if this [RecordBinding] contains the specified [ColumnDef] and false otherwise.
     *
     * @param column The [ColumnDef] specifying the column
     * @return True if record contains the [ColumnDef], false otherwise.
     */
    override fun has(column: ColumnDef<*>): Boolean = this.columns.contains(column)

    /**
     * Returns column index of the given [ColumnDef] within this [Record]. Returns -1 if [ColumnDef] is not contained
     *
     * @param column The [ColumnDef] to check.
     * @return The column index or -1. of [ColumnDef] is not part of this [Record].
     */
    override fun indexOf(column: ColumnDef<*>): Int = this.columns.indexOf(column)

    /**
     * Returns an unmodifiable [Map] of the data contained in this [RecordBinding].
     *
     * @return Unmodifiable [Map] of the data in this [RecordBinding].
     */
    override fun toMap(): Map<ColumnDef<*>, Value> = Object2ObjectArrayMap(this.columns, this.values.map { it.value }.toTypedArray())

    /**
     * Retrieves the value for the specified [ColumnDef] from this [RecordBinding].
     *
     * @param column The [ColumnDef] for which to retrieve the value.
     * @return The value for the [ColumnDef]
     */
    override fun get(column: ColumnDef<*>): Value? = this[this.columns.indexOf(column)]

    /**
     * Retrieves the value for the specified column index from this [RecordBinding].
     *
     * @param index The index for which to retrieve the value.
     * @return The value for the column index.
     */
    override fun get(index: Int): Value? {
        require(index in (0 until this.size)) { "The specified column $index is out of bounds." }
        return this.values[index].value
    }

    /**
     * Sets the [Value]  for the specified [ColumnDef] in this [Record].
     *
     * @param column The [ColumnDef] for which to set the value.
     * @param value The new [Value]
     */
    override fun set(column: ColumnDef<*>, value: Value?) = this.set(this.columns.indexOf(column), value)

    /**
     * Sets the [Value]  for the specified column index in this [Record].
     *
     * @param index The column index for which to set the value.
     * @param value The new [Value]
     */
    override fun set(index: Int, value: Value?) {
        require(index in (0 until this.size)) { "The specified column $index is out of bounds." }
        return this.values[index].update(value)
    }
}