package org.vitrivr.cottontail.dbms.entity

import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [Cursor] implementation for the [DefaultEntity].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DefaultEntityCursor(private val columns: Array<ColumnDef<*>>, partition: LongRange, entity: DefaultEntity.Tx, context: QueryContext) : Cursor<Tuple> {

    /** The wrapped [Cursor] to iterate over columns. */
    private val cursors: Array<Cursor<out Value?>> = Array(columns.size) {
        entity.columnForName(this.columns[it].name).newTx(entity.context).cursor(partition)
    }

    /**
     * Returns the [TupleId] this [Cursor] is currently pointing to.
     */
    override fun key(): TupleId = this.cursors[0].key()

    /**
     * Returns the [Tuple] this [Cursor] is currently pointing to.
     */
    override fun value(): Tuple = StandaloneTuple(this.cursors[0].key(), this.columns, Array(this.columns.size) { this.cursors[it].value() })

    /**
     * Tries to move this [DefaultEntityCursor]. Returns true on success and false otherwise.
     *
     * @return True on success, false otherwise,
     */
    override fun moveNext(): Boolean = this.cursors.all { it.moveNext() }

    /**
     * Tries to move this [DefaultEntityCursor] to the next [TupleId].
     *
     * @return True on success, false otherwise,
     */
    override fun moveTo(tupleId: TupleId): Boolean = this.cursors.all { it.moveTo(tupleId) }

    /**
     * Tries to move this [DefaultEntityCursor] to the previous [TupleId].
     *
     * @return True on success, false otherwise,
     */
    override fun movePrevious(): Boolean = this.cursors.all { it.movePrevious() }

    /**
     * Closes this [Cursor].
     */
    override fun close() = this.cursors.forEach { it.close() }
}