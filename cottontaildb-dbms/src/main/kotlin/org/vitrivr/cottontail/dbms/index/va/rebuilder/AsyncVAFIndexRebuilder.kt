package org.vitrivr.cottontail.dbms.index.va.rebuilder

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.RealVectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractAsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.IndexRebuilderState
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.vitrivr.cottontail.dbms.index.va.VAFIndexConfig
import org.vitrivr.cottontail.dbms.index.va.signature.EquidistantVAFMarks
import org.vitrivr.cottontail.dbms.index.va.signature.VAFSignature
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.index.IndexStatistic
import org.vitrivr.cottontail.dbms.statistics.values.RealVectorValueStatistics
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * An [AbstractAsyncIndexRebuilder] that can be used to concurrently rebuild a [VAFIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class AsyncVAFIndexRebuilder(index: VAFIndex, context: QueryContext): AbstractAsyncIndexRebuilder<VAFIndex>(index, context.catalogue, context.txn.manager) {

    /** A temporary [Store] used to */
    private val tmpDataStore: Store = this.tmpEnvironment.openStore(this.index.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.tmpTx, true)
        ?: throw DatabaseException.DataCorruptionException("Temporary data store for index ${this.index.name} could not be created.")

    /** Reference to [EquidistantVAFMarks] used by this [AsyncVAFIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private val newMarks: EquidistantVAFMarks

    /** Reference to [EquidistantVAFMarks] used by this [AsyncVAFIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private val indexedColumn: ColumnDef<*>

    /** A [ConcurrentLinkedQueue] that acts as log for side-channel events. TODO: Make persistent. */
    private val log = ConcurrentLinkedQueue<VAFIndexingEvent>()

    init {
        /* Read basic index properties. */
        val entry = IndexCatalogueEntry.read(this.index.name, this.index.catalogue, context.txn.xodusTx)
            ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index  ${this.index.name}: Could not read catalogue entry for index.")
        val config = entry.config as VAFIndexConfig
        val column = entry.columns[0]

        /* Tx objects required for index rebuilding. */
        val entityTx = this.index.parent.newTx(context)
        val columnTx = entityTx.columnForName(column).newTx(context)
        this.indexedColumn = columnTx.columnDef

        /* Generates new marks. */
        this.newMarks = EquidistantVAFMarks(columnTx.statistics() as RealVectorValueStatistics<*>, config.marksPerDimension)
    }

    /**
     * Internal scan method that is being executed when executing the BUILD stage of this [AsyncVAFIndexRebuilder].
     *
     * @param context The [QueryContext] to execute the BUILD stage in.
     * @return True on success, false otherwise.
     */
    override fun internalBuild(context: QueryContext): Boolean {
        /* Read basic index properties. */
        val entry = IndexCatalogueEntry.read(this.index.name, this.index.catalogue, context.txn.xodusTx)
            ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index  ${this.index.name}: Could not read catalogue entry for index.")
        val column = entry.columns[0]

        /* Tx objects required for index rebuilding. */
        val entityTx = this.index.parent.newTx(context)
        val columnTx = entityTx.columnForName(column).newTx(context)
        val count = columnTx.count()

        /* Iterate over entity and update index with entries. */
        var counter = 0
        columnTx.cursor().use { cursor ->
            while (cursor.hasNext()) {
                if (this.state != IndexRebuilderState.REBUILDING) return false
                val value = cursor.value()
                if (value is RealVectorValue<*>) {
                    if (!this.tmpDataStore.add(this.tmpTx, cursor.key().toKey(), this.newMarks.getSignature(value).toEntry())) {
                        return false
                    }

                    if ((++counter) % 1_000_000 == 0) {
                        LOGGER.debug("Rebuilding index (SCAN) ${this.index.name} (${this.index.type}) still running ($counter / $count)...")
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
     * @param context The [QueryContext] to execute the MERGE stage in.
     * @param store The [Store] to merge data into.
     * @return True on success, false otherwise.
     */
    override fun internalReplace(context: QueryContext, store: Store): Boolean {
        /* Begin replacement process. */
        val count = this.tmpDataStore.count(this.tmpTx)
        this.tmpDataStore.openCursor(this.tmpTx).use {cursor ->
            var counter = 0
            while (cursor.next) {
                if (this.state != IndexRebuilderState.REPLACING) return false
                if (!store.add(context.txn.xodusTx, cursor.key, cursor.value)) {
                    return false
                }

                /* Data is flushed every once in a while. */
                if ((++counter) % 1_000_000 == 0) {
                    LOGGER.debug("Rebuilding index (MERGE) ${this.index.name} (${this.index.type}) still running ($counter / $count)...")
                    if (!context.txn.xodusTx.flush()) {
                        return false
                    }
                }
            }
        }

        /* Update stored VAFMarks. */
        IndexStructCatalogueEntry.write(this.index.name, this.newMarks, this.index.catalogue, context.txn.xodusTx, EquidistantVAFMarks.Binding)

        /* Reset to default efficiency of VAF after rebuild. */
        this.index.catalogue.indexStatistics.updatePersistently(this.index.name, IndexStatistic(VAFIndex.FILTER_EFFICIENCY_CACHE_KEY, VAFIndex.DEFAULT_FILTER_EFFICIENCY.toString()), context.txn.xodusTx)
        return true
    }

    /**
     * Processes a [DataEvent] received from the side-channel, converts it to a [VAFIndexingEvent] and appends it to the log.
     *
     * @param event The [DataEvent] that should be processed,
     */
    override fun processSideChannelEvent(event: DataEvent): Boolean = when(event) {
        /* Process side-channel INSERT. */
        is DataEvent.Insert -> {
            val value = event.data[this.indexedColumn]
            if (value is RealVectorValue<*>) {
                this.log.offer(VAFIndexingEvent.Set(event.tupleId, this.newMarks.getSignature(value)))
            }
            true
        }

        /* Process side-channel DELETE. */
        is DataEvent.Delete -> {
            val value = event.data[this.indexedColumn]
            if (value != null) {
                this.log.offer(VAFIndexingEvent.Unset(event.tupleId))
            }
            true
        }

        /* Process side-channel UPDATE. */
        is DataEvent.Update -> {
            /* Extract value and perform sanity check. */
            val oldValue = event.data[this.indexedColumn]?.first
            val newValue = event.data[this.indexedColumn]?.second

            /* Obtain marks and update them. */
            if (newValue is RealVectorValue<*>) {               /* Case 1: New value is not null, i.e., update to new value. */
                this.log.offer(VAFIndexingEvent.Set(event.tupleId, this.newMarks.getSignature(newValue)))
            } else if (oldValue is RealVectorValue<*>) {        /* Case 2: New value is null but old value wasn't, i.e., delete index entry. */
                this.log.offer(VAFIndexingEvent.Unset(event.tupleId))
            }
            true
        }
    }

    /**
     * Drains and processes all [VAFIndexingEvent]s that are currently waiting on the [log].
     *
     * @return True on success, false otherwise.
     */
    override fun drainAndMergeLog(): Boolean {
        var next = this.log.poll()
        while (next != null) {
            val success = when (next) {
                is VAFIndexingEvent.Set -> this.tmpDataStore.put(this.tmpTx, next.tupleId.toKey(), next.signature.toEntry())
                is VAFIndexingEvent.Unset -> this.tmpDataStore.delete(this.tmpTx, next.tupleId.toKey())
            }
            if (!success) return false
            next = this.log.poll()
        }
        return true
    }

    /**
     * An internal helper interface used by the [AsyncVAFIndexRebuilder].
     *
     * Every [DataEvent] is converted to a [VAFIndexingEvent.Set] or [VAFIndexingEvent.Unset].
     * Order of operations is ensured by the log.
     */
    private sealed interface VAFIndexingEvent {
        val tupleId: TupleId

        /**
         * A log entry that signifies the appending of a [VAFSignature] to the [VAFIndex].
         */
        data class Set(override val tupleId: TupleId, val signature: VAFSignature): VAFIndexingEvent

        /**
         * A log entry that signifies the removal of a [VAFSignature] from the [VAFIndex].
         */
        data class Unset(override val tupleId: TupleId): VAFIndexingEvent
    }
}