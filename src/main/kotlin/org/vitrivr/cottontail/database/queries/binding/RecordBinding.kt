package org.vitrivr.cottontail.database.queries.binding

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import java.lang.UnsupportedOperationException

/**
 * A [Record] implementation that depends on the existence of [Binding]s for the [Value]s it contains.
 *
 * <strong>Important:</strong> The use of [StandaloneRecord] is discouraged when data volume becomes
 * large, as each [StandaloneRecord] has its own reference to the [ColumnDef]s it contains.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class RecordBinding(override var tupleId: TupleId, override val columns: Array<ColumnDef<*>>, private val values: Array<Binding>) : Record {

    /**
     * Creates a copy of this [RecordBinding]
     */
    override fun copy(): Record = RecordBinding(this.tupleId, this.columns.copyOf(), this.values.copyOf())

    /**
     * Iterates over the [ColumnDef] and [Value] pairs in this [Record] in the order specified by [columns].
     *
     * @param action The action to apply to each [ColumnDef], [Value] pair.
     */
    override fun forEach(action: (ColumnDef<*>, Value?) -> Unit) {
        for ((i, c) in this.columns.withIndex()) {
            action(c, this.values[i].value)
        }
    }

    /**
     * Returns true, if this [StandaloneRecord] contains the specified [ColumnDef] and false otherwise.
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
     * Returns an unmodifiable [Map] of the data contained in this [StandaloneRecord].
     *
     * @return Unmodifiable [Map] of the data in this [StandaloneRecord].
     */
    override fun toMap(): Map<ColumnDef<*>, Value> = Object2ObjectArrayMap(this.columns, this.values.map { it.value }.toTypedArray())

    /**
     * Retrieves the value for the specified [ColumnDef] from this [StandaloneRecord].
     *
     * @param column The [ColumnDef] for which to retrieve the value.
     * @return The value for the [ColumnDef]
     */
    override fun get(column: ColumnDef<*>): Value? = this.get(this.columns.indexOf(column))

    /**
     * Retrieves the value for the specified column index from this [StandaloneRecord].
     *
     * @param columnIndex The index for which to retrieve the value.
     * @return The value for the column index.
     */
    fun get(columnIndex: Int): Value? {
        require(columnIndex in (0 until this.size)) { "The specified column $columnIndex is out of bounds." }
        return this.values[columnIndex].value
    }

    override fun set(column: ColumnDef<*>, value: Value?) {
        throw UnsupportedOperationException("A bound record cannot be updated with new value.")
    }
}