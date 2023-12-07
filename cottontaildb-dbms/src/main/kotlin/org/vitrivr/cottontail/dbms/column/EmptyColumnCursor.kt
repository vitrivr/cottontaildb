package org.vitrivr.cottontail.dbms.column

import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value

/**
 * An empty [Cursor] implementation that can be used as placeholder for empty [Column]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object EmptyColumnCursor: Cursor<Value?> {
    override fun moveNext(): Boolean = false
    override fun key(): TupleId = throw NoSuchElementException("The cursor is empty.")
    override fun value(): Value? = throw NoSuchElementException("The cursor is empty.")
    override fun close() { /* No op. */ }
}