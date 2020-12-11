package org.vitrivr.cottontail.model.recordset

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*

/**
 * A [Record] implementation as returned and processed by Cottontail DB. A [StandaloneRecord] can
 * exist without an enclosing [Recordset], which is necessary for some applications.
 *
 * <strong>Important:</strong> The use of [StandaloneRecord] is discouraged when data volume becomes
 * large, as each [StandaloneRecord] has its own reference to the [ColumnDef]s it contains.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class StandaloneRecord(override var tupleId: Long = Long.MIN_VALUE, override val columns: Array<ColumnDef<*>>, values: Array<Value?> = Array(columns.size) { null }) : Record {

    init {
        /** Sanity check. */
        require(values.size == this.columns.size) { "The number of values must be equal to the number of columns held by the StandaloneRecord (v = ${values.size}, c = ${this.columns.size})" }
        this.columns.forEachIndexed { index, columnDef ->
            columnDef.validateOrThrow(values[index])
        }
    }

    /** Initialize internal [Object2ObjectOpenHashMap] used to map columns to values. */
    private val map = Object2ObjectOpenHashMap(this.columns, values)

    /**
     * Copies this [StandaloneRecord] and returns the copy.
     *
     * @return Copy of this [StandaloneRecord]
     */
    override fun copy(): Record = StandaloneRecord(this.tupleId, this.columns, this.columns.map { this.map[it] }.toTypedArray())

    /**
     * Iterates over the [ColumnDef] and [Value] pairs in this [Record] in the order specified by [columns].
     *
     * @param action The action to apply to each [ColumnDef], [Value] pair.
     */
    override fun forEach(action: (ColumnDef<*>, Value?) -> Unit) {
        for (c in this.columns) action(c, this.map[c])
    }

    /**
     * Returns true, if this [StandaloneRecord] contains the specified [ColumnDef] and false otherwise.
     *
     * @param column The [ColumnDef] specifying the column
     * @return True if record contains the [ColumnDef], false otherwise.
     */
    override fun has(column: ColumnDef<*>): Boolean = this.map.containsKey(column)

    /**
     * Returns an unmodifiable [Map] of the data contained in this [StandaloneRecord].
     *
     * @return Unmodifiable [Map] of the data in this [StandaloneRecord].
     */
    override fun toMap(): Map<ColumnDef<*>, Value?> = Collections.unmodifiableMap(this.map)

    /**
     * Retrieves the value for the specified [ColumnDef] from this [StandaloneRecord].
     *
     * @param column The [ColumnDef] for which to retrieve the value.
     * @return The value for the [ColumnDef]
     */
    override fun get(column: ColumnDef<*>): Value? {
        require(this.map.contains(column)) { "The specified column ${column.name}  (type=${column.type.name}) is not contained in this record." }
        return this.map[column]
    }

    /**
     * Sets the value for the specified [ColumnDef] in this [StandaloneRecord].
     *
     * @param column The [ColumnDef] for which to set the value.
     * @param value The new value for the [ColumnDef]
     */
    override fun set(column: ColumnDef<*>, value: Value?) {
        require(this.map.contains(column)) { "The specified column ${column.name}  (type=${column.type.name})  is not contained in this record." }
        column.validateOrThrow(value)
        this.map[column] = value
    }


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as StandaloneRecord

        if (tupleId != other.tupleId) return false
        if (map != other.map) return false

        return true
    }

    override fun hashCode(): Int {
        var result = tupleId.hashCode()
        result = 31 * result + columns.hashCode()
        result = 31 * result + map.hashCode()
        return result
    }
}