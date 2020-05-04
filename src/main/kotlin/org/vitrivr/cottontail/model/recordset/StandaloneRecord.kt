package org.vitrivr.cottontail.model.recordset

import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [Record] implementation as returned and processed by Cottontail DB. These types of records can exist
 * without an enclosing [Recordset] and are necessary for some applications.
 *
 * <strong>Important:</strong> The use of [StandaloneRecord] is discouraged when data volume becomes large,
 * as each [StandaloneRecord] has its own reference to the [ColumnDef]s it contains.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class StandaloneRecord(override val tupleId: Long = Long.MIN_VALUE, override val columns: Array<ColumnDef<*>>, init: Array<Value?>? = null) : Record {

    /**
     * Array of column values (one entry per column in the same order).
     *
     * This array is initialized either with the init-array provided with the constructor OR an empty array
     * of NULL/default values for the [org.vitrivr.cottontail.database.column.ColumnType].
     */
    override val values: Array<Value?> = if (init != null) {
        assert(this.columns.size == init.size)
        init.forEachIndexed { index, any -> this.columns[index].validateOrThrow(any) }
        init
    } else Array(this.columns.size) { this.columns[it].defaultValue() }

    /**
     * Copies this [Record] and returns the copy.
     *
     * @return Copy of this [Record]
     */
    override fun copy(): Record = StandaloneRecord(tupleId, columns = columns, init = values.copyOf())

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Record

        if (tupleId != other.tupleId) return false
        if (!columns.contentEquals(other.columns)) return false
        if (!values.contentEquals(other.values)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = tupleId.hashCode()
        result = 31 * result + columns.hashCode()
        result = 31 * result + values.contentHashCode()
        return result
    }
}