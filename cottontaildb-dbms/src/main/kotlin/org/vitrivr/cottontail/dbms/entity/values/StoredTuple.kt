package org.vitrivr.cottontail.dbms.entity.values

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.storage.entries.interfaces.DataFile

/**
 * A [Tuple] implementation that lazily loads [Value]s from a [DataFile] when requested.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class StoredTuple(override val tupleId: TupleId, override val columns: Array<ColumnDef<*>>, val values: Array<StoredValue<*>?>): Tuple {
    override fun copy(): Tuple = StoredTuple(this.tupleId, this.columns, this.values)
    override fun get(index: Int): Value?  = this.values[index]?.value
    override fun values(): List<Value?> = this.values.map { it?.value }
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is StoredTuple) return false

        if (tupleId != other.tupleId) return false
        if (!columns.contentEquals(other.columns)) return false
        if (!values.contentEquals(other.values)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = tupleId.hashCode()
        result = 31 * result + columns.contentHashCode()
        result = 31 * result + values.hashCode()
        return result
    }
}