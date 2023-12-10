package org.vitrivr.cottontail.dbms.entity

import jetbrains.exodus.env.BitmapIterator
import jetbrains.exodus.tree.LongIterator
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
 * @version 2.0.0
 */
class DefaultEntityCursor(entity: DefaultEntity.Tx, columns: Array<ColumnDef<*>>, partition: LongRange, rename: Array<Name.ColumnName> = emptyArray<Name.ColumnName>()) : Cursor<Tuple> {

    init {
        require(rename.isEmpty() || columns.size == rename.size) { "The size of the rename column array does not match the number of scanned columns."}
    }

    /** The wrapped [Cursor] to iterate over columns. */
    @Suppress("UNCHECKED_CAST")
    private val txs: Array<Cursor<Value>> = Array(columns.size) {
        (entity.columnForName(columns[it].name).newTx(entity.context).cursor() as Cursor<Value>)
    }

    /** The array of output [ColumnDef] produced by this [DefaultEntityCursor]. */
    private val columns = columns.mapIndexed { index, def ->
        def.copy(name = rename.getOrNull(index) ?: def.name)
    }.toTypedArray()

    /** The transaction snapshot used for the cursor. */
    private val snapshot = entity.context.txn.xodusTx.readonlySnapshot

    /** The [LongIterator] backing this [DefaultEntityCursor]. */
    private val iterator = entity.bitmap.iterator(this.snapshot) as BitmapIterator

    /** The [TupleId] this [DefaultEntityCursor] is currently pointing to. */
    private var current: TupleId = -1

    /** The [TupleId] this [DefaultEntityCursor] is currently pointing to. */
    private val maximum: TupleId = partition.last

    init {
        /* Fast-forward to entry at position. */
        if (partition.first > 0L) {
            this.iterator.getSearchBit(partition.first)
        }
    }

    /**
     * Returns the [TupleId] this [Cursor] is currently pointing to.
     */
    override fun key(): TupleId = this.current

    /**
     * Returns the [Tuple] this [Cursor] is currently pointing to.
     */
    override fun value(): Tuple = StandaloneTuple(this.current, this.columns, Array(this.columns.size) {
        if (this.txs[it].moveTo(this.current)) {
            this.txs[it].value()
        } else {
            null
        }
    })

    /**
     * Tries to move this [DefaultEntityCursor]. Returns true on success and false otherwise.
     *
     * @return True on success, false otherwise,
     */
    override fun moveNext(): Boolean {
        if (this.current < this.maximum && this.iterator.hasNext()) {
            this.current = this.iterator.next()
            return true
        }
        return false
    }

    /**
     * Closes this [Cursor].
     */
    override fun close() {
        this.txs.forEach { it.close() }
        this.iterator.close()
        this.snapshot.abort()
    }
}