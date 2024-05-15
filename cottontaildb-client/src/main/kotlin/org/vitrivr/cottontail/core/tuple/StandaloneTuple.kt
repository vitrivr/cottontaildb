package org.vitrivr.cottontail.core.tuple

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value

/**
 * A [Tuple] implementation as returned and processed by Cottontail DB.
 *
 * A [StandaloneTuple] can exist without an enclosing structure, i.e., each [StandaloneTuple] contains information
 * about the schema and the [Value]s it contains Internally, it uses a [Map] to store the values.
 *
 * <strong>Important:</strong> The use of [StandaloneTuple] is discouraged when data volume becomes
 * large, as each [StandaloneTuple] has its own reference to the [ColumnDef]s it contains.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class StandaloneTuple(override var tupleId: TupleId, override val columns: Array<ColumnDef<*>>, val values: Array<Value?> = arrayOfNulls(columns.size)): MutableTuple {

    /**
     * Constructor for single entry [StandaloneTuple].
     *
     * @param tupleId The [TupleId] of the [StandaloneTuple].
     * @param column The [ColumnDef] of the [StandaloneTuple].
     * @param value The [Value] of the [StandaloneTuple].
     */
    constructor(tupleId: TupleId, column: ColumnDef<*>, value: Value?) : this(tupleId, arrayOf(column), arrayOf(value))

    init {
        require(this.columns.size == this.values.size) { "Number of values and number of columns must be the same." }
        for ((c, v) in this.columns.zip(this.values)) {
            require(((v == null && c.nullable) || (v != null && c.type == v.type))) { "Value $v is incompatible with column $c." }
        }
    }

    /**
     * Copies this [StandaloneTuple] and returns the copy. Creates a physical copy of the [values] array.
     *
     * @return Copy of this [StandaloneTuple]
     */
    override fun copy(): Tuple = StandaloneTuple(this.tupleId, this.columns, this.values.copyOf())

    /**
     * Returns true, if this [StandaloneTuple] contains the specified [ColumnDef] and false otherwise.
     *
     * @param column The [ColumnDef] specifying the column
     * @return True if record contains the [ColumnDef], false otherwise.
     */
    override fun has(column: ColumnDef<*>): Boolean = this.columns.contains(column)

    /**
     * Returns column index of the given [ColumnDef] within this [Tuple]. Returns -1 if [ColumnDef] is not contained
     *
     * @param column The [ColumnDef] to check.
     * @return The column index or -1. of [ColumnDef] is not part of this [Tuple].
     */
    override fun indexOf(column: ColumnDef<*>): Int = this.columns.indexOf(column)

    /**
     * Returns column index of the given [Name.ColumnName] within this [Tuple]. Returns -1 if [Name.ColumnName] is not contained
     *
     * @param column The [Name.ColumnName] to check.
     * @return The column index or -1. of [Name.ColumnName] is not part of this [Tuple].
     */
    override fun indexOf(column: Name.ColumnName): Int = this.columns.indexOfFirst { it.name == column }

    /**
     * Retrieves the value for the specified column index from this [StandaloneTuple].
     *
     * @param index The index for which to retrieve the value.
     * @return The value for the column index.
     */
    override fun get(index: Int): Value? = this.values[index]

    /**
     * Returns a [List] of [Value]s contained in this [StandaloneTuple].
     *
     * @return [List] of [Value]s
     */
    override fun values(): List<Value?> = this.values.toList()

    /**
     * Sets the value for the specified column index  in this [StandaloneTuple].
     *
     * @param index The index for which to set the value.
     * @param value The new [Value]
     */
    override fun set(index: Int, value: Value?) {
        val column = this.columns[index]
        require(((value == null && column.nullable) || (value != null && value.type == column.type))) { "Provided value $value is incompatible with column ${this.columns[index]}." }
        this.values[index] = value
    }

    /**
     *
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Tuple) return false
        if (this.tupleId != other.tupleId) return false
        if (this.columns != other.columns) return false
        for (i in 0 until this.columns.size) {
            if (this[i] != other[i]) return false
        }
        return true
    }

    /**
     *
     */
    override fun hashCode(): Int {
        var result = this.tupleId.hashCode()
        result = 31 * result + this.columns.hashCode()
        result = 31 * result + this.values.hashCode()
        return result
    }
}