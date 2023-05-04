package org.vitrivr.cottontail.core.queries.binding

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value


/**
 * This is a placeholder [Record] used for context receiver only.
 *
 * @author Ralph Gasser
 * @version 1.0.0.
 */
object MissingRecord: Record {
    override val tupleId: TupleId
        get() = throw IllegalStateException("Contextual access to missing record is not allowed! This is a programmer's error!")
    override val columns: Array<ColumnDef<*>>
        get() = throw IllegalStateException("Contextual access to missing record is not allowed! This is a programmer's error!")
    override fun copy(): Record = this
    override fun has(column: ColumnDef<*>): Boolean = false
    override fun indexOf(column: ColumnDef<*>): Int = throw IllegalStateException("Contextual access to missing record is not allowed! This is a programmer's error!")
    override fun toMap(): Map<ColumnDef<*>, Value?> = emptyMap()
    override fun get(index: Int): Value = throw IllegalStateException("Contextual access to missing record is not allowed! This is a programmer's error!")
    override fun get(column: ColumnDef<*>): Value = throw IllegalStateException("Contextual access to missing record is not allowed! This is a programmer's error!")
    override fun set(index: Int, value: Value?) = throw IllegalStateException("Contextual access to missing record is not allowed! This is a programmer's error!")
    override fun set(column: ColumnDef<*>, value: Value?) = throw IllegalStateException("Contextual access to missing record is not allowed! This is a programmer's error!")
}