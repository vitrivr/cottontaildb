package org.vitrivr.cottontail.dbms.entity

import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value

/**
 * A [Cursor] implementation for the [DefaultEntity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class DefaultEntityCursor(entity: DefaultEntity.Tx, columns: Array<ColumnDef<*>>, partition: LongRange, rename: Array<Name.ColumnName> = emptyArray<Name.ColumnName>()) : Cursor<Tuple> {

    init {
        require(rename.isEmpty() || columns.size == rename.size) { "The size of the rename column array does not match the number of scanned columns."}
    }

    /** The wrapped [Cursor] to iterate over columns. */
    private val cursors: Array<Cursor<out Value?>> = Array(columns.size) {
        entity.columnForName(columns[it].name).newTx(entity.context).cursor(partition)
    }

    /** The array of output [ColumnDef] produced by this [DefaultEntityCursor]. */
    private val columns = columns.mapIndexed { index, def ->
        def.copy(name = rename.getOrNull(index) ?: def.name)
    }.toTypedArray()

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