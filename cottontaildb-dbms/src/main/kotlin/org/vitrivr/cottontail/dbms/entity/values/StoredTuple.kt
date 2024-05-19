package org.vitrivr.cottontail.dbms.entity.values

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.DefaultEntity

/**
 * A [Tuple] implementation used by [DefaultEntity] to fetch an entry stored on disk.
 *
 * [StoredTuple] do not necessarily contain the actual [Value]. Instead, the have [StoredValue]s that point to the actual [Value]s.
 * Details as to how these [StoredValue]s are resolved are dependent on the [ColumnDef] (inline vs. out-of-line storage).
 *
 * To generate a materialized copy of this [StoredTuple], use [StoredTuple.materialize].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class StoredTuple(override val tupleId: TupleId, override val columns: Array<ColumnDef<*>>, val values: Array<StoredValue<*>?>): Tuple {
    override fun copy(): Tuple = StoredTuple(this.tupleId, this.columns, this.values.copyOf())
    override fun get(index: Int): Value?  = this.values[index]?.value
    override fun values(): List<Value?> = this.values.map { it?.value }

    /**
     * Materializes this [StoredTuple] to obtain a point-in-time snapshot of the actual [Value]s.
     *
     * @return [StandaloneTuple]
     */
    fun materialize(): Tuple = StandaloneTuple(this.tupleId, this.columns, this.values.map { it?.value }.toTypedArray())

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