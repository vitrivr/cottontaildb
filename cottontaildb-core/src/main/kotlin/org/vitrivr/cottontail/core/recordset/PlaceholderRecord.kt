package org.vitrivr.cottontail.core.recordset

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.types.Value


/**
 * This is a placeholder [Record] used for context receiver only.
 *
 * @author Ralph Gasser
 * @version 1.0.0.
 */
object PlaceholderRecord: Record {
    override val tupleId: TupleId
        get() = throw IllegalStateException("Cannot access property of placeholder record. This is a programmer's error!")
    override val columns: Array<ColumnDef<*>>
        get() = throw IllegalStateException("Cannot access property of placeholder record. This is a programmer's error!")
    override fun copy(): Record = this
    override fun has(column: ColumnDef<*>): Boolean = false
    override fun indexOf(column: ColumnDef<*>): Int
        = throw IllegalStateException("Cannot access property of placeholder record. This is a programmer's error!")
    override fun toMap(): Map<ColumnDef<*>, Value?> = emptyMap()
    override fun get(index: Int): Value
        = throw IllegalStateException("Cannot access property of placeholder record. This is a programmer's error!")
    override fun get(column: ColumnDef<*>): Value
        = throw IllegalStateException("Cannot access property of placeholder record. This is a programmer's error!")
    override fun set(index: Int, value: Value?)
        = throw IllegalStateException("Cannot access property of placeholder record. This is a programmer's error!")
    override fun set(column: ColumnDef<*>, value: Value?)
        = throw IllegalStateException("Cannot access property of placeholder record. This is a programmer's error!")
}