package org.vitrivr.cottontail.dbms.entity

import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.TupleId

/**
 * A dummy [Cursor] implementation that represents an empty [DefaultEntity].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object EmptyEntityCursor: Cursor<Record> {
    override fun key(): TupleId = throw UnsupportedOperationException("EmptyEntityCursor cannot return a tupleId. This is a programmer's error!")
    override fun value(): Record = throw UnsupportedOperationException("EmptyEntityCursor cannot return a tupleId. This is a programmer's error!")
    override fun moveNext(): Boolean = false
    override fun movePrevious(): Boolean = false
    override fun close() {/* No op. */ }
}