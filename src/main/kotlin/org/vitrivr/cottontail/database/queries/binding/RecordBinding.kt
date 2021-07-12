package org.vitrivr.cottontail.database.queries.binding

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordBinding(override var tupleId: TupleId, override val columns: Array<ColumnDef<*>>, private val values: Array<Binding>) : Record {

    override fun copy(): Record {
        TODO("Not yet implemented")
    }

    override fun forEach(action: (ColumnDef<*>, Value?) -> Unit) {
        TODO("Not yet implemented")
    }

    override fun has(column: ColumnDef<*>): Boolean {
        TODO("Not yet implemented")
    }

    override fun indexOf(column: ColumnDef<*>): Int {
        TODO("Not yet implemented")
    }

    override fun toMap(): Map<ColumnDef<*>, Value?> {
        TODO("Not yet implemented")
    }

    override fun get(column: ColumnDef<*>): Value? {
        TODO("Not yet implemented")
    }

    override fun set(column: ColumnDef<*>, value: Value?) {
        TODO("Not yet implemented")
    }
}