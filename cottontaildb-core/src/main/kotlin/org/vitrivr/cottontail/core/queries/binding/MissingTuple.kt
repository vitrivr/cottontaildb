package org.vitrivr.cottontail.core.queries.binding

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value


/**
 * This is a placeholder [Tuple] used for context receiver only.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object MissingTuple: Tuple {
    override val tupleId: TupleId
        get() = throw IllegalStateException("Contextual access to missing tuple is not allowed! This is a programmer's error!")
    override val columns: Array<ColumnDef<*>>
        get() = throw IllegalStateException("Contextual access to missing tuple is not allowed! This is a programmer's error!")
    override fun copy(): Tuple = this
    override fun has(column: ColumnDef<*>): Boolean = false
    override fun indexOf(column: ColumnDef<*>): Int = throw IllegalStateException("Contextual access to missing tuple is not allowed! This is a programmer's error!")
    override fun indexOf(column: Name.ColumnName): Int = throw IllegalStateException("Contextual access to missing tuple is not allowed! This is a programmer's error!")
    override fun get(index: Int): Value = throw IllegalStateException("Contextual access to missing tuple is not allowed! This is a programmer's error!")
    override fun get(column: ColumnDef<*>): Value = throw IllegalStateException("Contextual access to missing tuple is not allowed! This is a programmer's error!")
    override fun values(): List<Value?> = throw IllegalStateException("Contextual access to missing tuple is not allowed! This is a programmer's error!")
}