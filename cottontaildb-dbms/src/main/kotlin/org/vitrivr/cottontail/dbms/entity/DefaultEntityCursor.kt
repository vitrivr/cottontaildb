package org.vitrivr.cottontail.dbms.entity

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.tree.LongIterator
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.EntityMetadata.Companion.storeName
import org.vitrivr.cottontail.storage.tuple.StoredTupleSerializer

/**
 * A [Cursor] implementation for the [DefaultEntity].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class DefaultEntityCursor(private val entity: DefaultEntity.Tx, val serializer: StoredTupleSerializer, partition: LongRange) : Cursor<Tuple> {

    /** The array of output [ColumnDef] produced by this [DefaultEntityCursor]. */
    private val columns = this.serializer.columns

    /** The transaction snapshot used for the cursor. */
    private val snapshot = this.entity.xodusTx.readonlySnapshot

    /** The [LongIterator] backing this [DefaultEntityCursor]. */
    private val cursor = this.snapshot.environment.openStore(this.entity.dbo.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.snapshot).openCursor(this.snapshot)

    /** The [TupleId] this [DefaultEntityCursor] is currently pointing to. */
    private val maximum: ByteIterable = partition.last.toKey()

    init {
        /* Fast-forward to entry at position. */
        if (partition.first > 0L) {
            if (this.cursor.getSearchKeyRange(partition.first.toKey()) != null) {
                this.cursor.prev
            }
        }
    }

    /**
     * Returns the [TupleId] this [Cursor] is currently pointing to.
     */
    override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.key)

    /**
     * Returns the [Tuple] this [Cursor] is currently pointing to.
     */
    override fun value(): Tuple = this.serializer.fromEntry(this.key(), this.cursor.value).copy(columns = this.columns)

    /**
     * Tries to move this [DefaultEntityCursor]. Returns true on success and false otherwise.
     *
     * @return True on success, false otherwise,
     */
    override fun moveNext(): Boolean = this.cursor.key < this.maximum && this.cursor.next

    /**
     * Closes this [Cursor].
     */
    override fun close() {
        this.cursor.close()
        this.snapshot.abort()
    }
}