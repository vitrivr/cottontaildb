package org.vitrivr.cottontail.dbms.index.va.rebuilder

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractAsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.IndexRebuilderState
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.vitrivr.cottontail.dbms.index.va.VAFIndexConfig
import org.vitrivr.cottontail.dbms.index.va.signature.EquidistantVAFMarks
import org.vitrivr.cottontail.dbms.statistics.columns.VectorValueStatistics

/**
 * An [AbstractAsyncIndexRebuilder] that can be used to concurrently rebuild a [VAFIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class AsyncVAFIndexRebuilder(index: VAFIndex): AbstractAsyncIndexRebuilder<VAFIndex>(index) {

    /** A temporary [Store] used to */
    private val tmpDataStore:Store = this.tmpEnvironment.openStore(this.index.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.tmpTx, true)
        ?: throw DatabaseException.DataCorruptionException("Temporary data store for index ${this.index.name} could not be created.")

    /** Reference to [EquidistantVAFMarks] used by this [AsyncVAFIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private var newMarks: EquidistantVAFMarks? = null

    /** Reference to [EquidistantVAFMarks] used by this [AsyncVAFIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private var indexedColumn: ColumnDef<*>? = null

    /**
     * Internal scan method that is being executed when executing the SCAN stage of this [AsyncVAFIndexRebuilder].
     *
     * @param context1 The [TransactionContext] to execute the SCAN stage in.
     * @return True on success, false otherwise.
     */
    override fun internalScan(context1: TransactionContext): Boolean {
        /* Read basic index properties. */
        val entry = IndexCatalogueEntry.read(this.index.name, this.index.catalogue, context1.xodusTx)
            ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index  ${this.index.name}: Could not read catalogue entry for index.")
        val config = entry.config as VAFIndexConfig
        val column = entry.columns[0]

        /* Tx objects required for index rebuilding. */
        val entityTx = context1.getTx(this.index.parent) as EntityTx
        val columnTx = context1.getTx(entityTx.columnForName(column)) as ColumnTx<*>
        val count = columnTx.count()
        this.indexedColumn = columnTx.columnDef

        /* Generates new marks. */
        this.newMarks = EquidistantVAFMarks(columnTx.statistics() as VectorValueStatistics<*>, config.marksPerDimension)

        /* Creates temporary data store. */
        val dataStore = this.tmpEnvironment.openStore(this.index.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.tmpTx, true)
                ?: throw DatabaseException.DataCorruptionException("Temporary data store for index ${this.index.name} could not be created.")

        /* Iterate over entity and update index with entries. */
        var counter = 1
        columnTx.cursor().use { cursor ->
            while (cursor.hasNext()) {
                if (this.state != IndexRebuilderState.SCANNING) return false
                val value = cursor.value()
                if (value is RealVectorValue<*>) {
                    if (!dataStore.put(this.tmpTx, this.newMarks!!.getSignature(value).toEntry(), cursor.key().toKey())) {
                        return false
                    }

                    /* Data is flushed every once in a while. */
                    if ((counter ++) % 1_000_000 == 0) {
                        LOGGER.debug("Rebuilding index ${this.index.name} (${this.index.type}) still running ($counter / $count)...")
                        if (!this.tmpTx.flush()) {
                            return false
                        }
                    }
                }
            }
        }

        return true
    }

    /**
     * Internal merge method that is being executed when executing the MERGE stage of this [AsyncVAFIndexRebuilder].
     *
     * @param context2 The [TransactionContext] to execute the MERGE stage in.
     * @param store The [Store] to merge data into.
     * @return True on success, false otherwise.
     */
    override fun internalMerge(context2: TransactionContext, store: Store): Boolean {
        val count = this.tmpDataStore.count(this.tmpTx)
        this.tmpDataStore.openCursor(this.tmpTx).use {cursor ->
            var counter = 1
            while (cursor.next) {
                if (this.state != IndexRebuilderState.MERGING) return false
                if (!store.put(context2.xodusTx, cursor.key, cursor.value)) {
                    return false
                }

                /* Data is flushed every once in a while. */
                if ((counter ++) % 1_000_000 == 0) {
                    LOGGER.debug("Merging index ${this.index.name} (${this.index.type}) still running ($counter / $count)...")
                    if (!context2.xodusTx.flush()) {
                        return false
                    }
                }
            }

            /* Update stored VAFMarks. */
            IndexStructCatalogueEntry.write(this.index.name, this.newMarks!!, this.index.catalogue, context2.xodusTx, EquidistantVAFMarks.Binding)
            return true
        }
    }

    /**
     * Internal method that apples a [DataEvent.Insert] from an external transaction to this [AsyncVAFIndexRebuilder].
     *
     * @param event The [DataEvent.Insert] to process.
     * @return True on success, false otherwise.
     */
    override fun applyAsyncInsert(event: DataEvent.Insert): Boolean {
        val value = event.data[this.indexedColumn!!] ?: return true
        return this.tmpDataStore.add(this.tmpTx, event.tupleId.toKey(), this.newMarks!!.getSignature(value as RealVectorValue<*>).toEntry())
    }

    /**
     * Internal method that apples a [DataEvent.Update] from an external transaction to this [AsyncVAFIndexRebuilder].
     *
     * @param event The [DataEvent.Update] to process.
     * @return True on success, false otherwise.
     */
    override fun applyAsyncUpdate(event: DataEvent.Update): Boolean {
        val oldValue = event.data[this.indexedColumn!!]?.first
        val newValue = event.data[this.indexedColumn!!]?.second

        /* Obtain marks and update them. */
        return if (newValue != null) { /* Case 1: New value is not null, i.e., update to new value. */
            this.tmpDataStore.put(this.tmpTx, event.tupleId.toKey(), this.newMarks!!.getSignature(newValue as RealVectorValue<*>).toEntry())
        } else if (oldValue != null) { /* Case 2: New value is null but old value wasn't, i.e., delete index entry. */
            this.tmpDataStore.delete(this.tmpTx, event.tupleId.toKey())
        } else {
            true /* If value is NULL. */
        }
    }

    /**
     * Internal method that apples a [DataEvent.Delete] from an external transaction to this [AsyncVAFIndexRebuilder].
     *
     * @param event The [DataEvent.Delete] to process.
     * @return True on success, false otherwise.
     */
    override fun applyAsyncDelete(event: DataEvent.Delete): Boolean {
        return event.data[this.indexedColumn!!] == null || this.tmpDataStore.delete(this.tmpTx, event.tupleId.toKey())
    }
}