package org.vitrivr.cottontail.core.recordset

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value

/**
 * A [Record] implementation as returned and processed by Cottontail DB. A [StandaloneRecord] can
 * exist without an enclosing [Recordset], i.e., each [StandaloneRecord] contains information
 * about the [ColumnDef] and the [Value]s it contains Internally, it uses a [Map] to store the values.
 *
 * <strong>Important:</strong> The use of [StandaloneRecord] is discouraged when data volume becomes
 * large, as each [StandaloneRecord] has its own reference to the [ColumnDef]s it contains.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class StandaloneRecord(override var tupleId: TupleId, override val columns: Array<ColumnDef<*>>, val values: Array<Value?> = arrayOfNulls(columns.size)) : Record {

    /**
     * Constructor for single entry [StandaloneRecord].
     *
     * @param tupleId The [TupleId] of the [StandaloneRecord].
     * @param column The [ColumnDef] of the [StandaloneRecord].
     * @param value The [Value] of the [StandaloneRecord].
     */
    constructor(tupleId: TupleId, column: ColumnDef<*>, value: Value?) : this(tupleId, arrayOf(column), arrayOf(value))

    init {
        require(this.columns.size == this.values.size) { "Number of values and number of columns must be the same." }
        for ((i, c) in this.columns.withIndex()) {
            require(c.validate(this.values[i])) { "Value ${this.values[i]} is incompatible with column $c." }
        }
    }

    /** Internal cache to speed-up access to pointers. */
    private val map = Object2ObjectArrayMap<ColumnDef<*>,Value?>(this.columns, this.values)

    /**
     * Copies this [StandaloneRecord] and returns the copy. Creates a physical copy of the [values] array.
     *
     * @return Copy of this [StandaloneRecord]
     */
    override fun copy(): Record = StandaloneRecord(this.tupleId, this.columns, this.values.copyOf())

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
    override fun toMap(): Map<ColumnDef<*>, Value?> = Object2ObjectArrayMap(this.columns, this.values)

    /**
     * Retrieves the value for the specified [ColumnDef] from this [StandaloneRecord].
     *
     * @param column The [ColumnDef] for which to retrieve the value.
     * @return The value for the [ColumnDef]
     */
    override fun get(column: ColumnDef<*>): Value? = this.map[column]

    /**
     * Retrieves the value for the specified column index from this [StandaloneRecord].
     *
     * @param index The index for which to retrieve the value.
     * @return The value for the column index.
     */
    override fun get(index: Int): Value? {
        require(index in (0 until this.size)) { "The specified column $index is out of bounds." }
        return this.values[index]
    }

    /**
     * Sets the value for the specified [ColumnDef] in this [StandaloneRecord].
     *
     * @param column The [ColumnDef] for which to set the value.
     * @param value The new value for the [ColumnDef]
     */
    override fun set(column: ColumnDef<*>, value: Value?) = this.map.set(column, value)

    /**
     * Sets the value for the specified column index  in this [StandaloneRecord].
     *
     * @param index The index for which to set the value.
     * @param value The new [Value]
     */
    override fun set(index: Int, value: Value?) {
        require(index in (0 until this.size)) { "The specified column $index is out of bounds." }
        require(this.columns[index].validate(value)) { "Provided value $value is incompatible with column ${this.columns[index]}." }
        this.values[index] = value
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Record) return false
        if (this.tupleId != other.tupleId) return false
        if (this.columns.contentDeepEquals(other.columns)) return false
        for (i in 0 until this.columns.size) {
            if (this[i] != other[i]) return false
        }
        return true
    }

    override fun hashCode(): Int {
        var result = this.tupleId.hashCode()
        result = 31 * result + this.columns.hashCode()
        result = 31 * result + this.values.hashCode()
        return result
    }
}