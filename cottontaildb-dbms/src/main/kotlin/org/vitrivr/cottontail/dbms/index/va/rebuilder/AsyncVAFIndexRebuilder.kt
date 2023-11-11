package org.vitrivr.cottontail.dbms.index.va.rebuilder

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.RealVectorValue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractAsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.IndexRebuilderState
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.vitrivr.cottontail.dbms.index.va.VAFIndexConfig
import org.vitrivr.cottontail.dbms.index.va.signature.EquidistantVAFMarks
import org.vitrivr.cottontail.dbms.index.va.signature.VAFSignature
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.storage.IndexStatistic
import org.vitrivr.cottontail.dbms.statistics.values.RealVectorValueStatistics
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * An [AbstractAsyncIndexRebuilder] that can be used to concurrently rebuild a [VAFIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class AsyncVAFIndexRebuilder(index: VAFIndex, context: QueryContext): AbstractAsyncIndexRebuilder<VAFIndex>(index, context.transaction.manager) {

    /** A temporary [Store] used to */
    private val store: Store = this.environment.openStore(this.index.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)

    /** Reference to [EquidistantVAFMarks] used by this [AsyncVAFIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private val newMarks: EquidistantVAFMarks

    /** Reference to [EquidistantVAFMarks] used by this [AsyncVAFIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private val indexedColumn: ColumnDef<*>

    /** A [ConcurrentLinkedQueue] that acts as log for side-channel events. TODO: Make persistent. */
    private val log = ConcurrentLinkedQueue<VAFIndexingEvent>()

    init {
        /* Read basic index properties. */
        val indexTx = context.transaction.indexTx(index.name, AccessMode.READ)
        val config = indexTx.config as VAFIndexConfig
        this.indexedColumn = indexTx.columns[0]

        /* Tx objects required for index rebuilding. */
        val columnTx = context.transaction.columnTx(this.indexedColumn.name, AccessMode.READ)

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
        /* Tx objects required for index rebuilding. */
        val columnTx = context.transaction.columnTx(this.indexedColumn.name, AccessMode.READ)

        /* Iterate over entity and update index with entries. */
        var counter = 0
        columnTx.cursor().use { cursor ->
            while (cursor.hasNext()) {
                if (this.state != IndexRebuilderState.REBUILDING) return false
                val value = cursor.value()
                if (value is RealVectorValue<*>) {
                    if (!this.store.add(this.xodusTx, cursor.key().toKey(), this.newMarks.getSignature(value).toEntry())) {
                        return false
                    }

                    if ((++counter) % 1_000_000 == 0) {
                        LOGGER.debug("Rebuilding index (SCAN) {} ({}) still running ({})...", this.index.name, this.index.type, counter)
                        if (!this.xodusTx.flush()) {
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
     * @param indexTx The [IndexTx] to merge data into.
     * @return True on success, false otherwise.
     */
    override fun internalReplace(indexTx: IndexTx): Boolean {
        require(indexTx is VAFIndex.Tx) { "AsyncVAFIndexRebuilder only supports VAFIndex.Tx implementations." }

        /* Begin replacement process. */
        val count = this.store.count(this.xodusTx)
        this.store.openCursor(this.xodusTx).use { cursor ->
            var counter = 0
            while (cursor.next) {
                if (this.state != IndexRebuilderState.REPLACING) return false
                if (!indexTx.store.add(indexTx.xodusTx, cursor.key, cursor.value)) {
                    return false
                }

                /* Data is flushed every once in a while. */
                if ((++counter) % 1_000_000 == 0) {
                    LOGGER.debug("Rebuilding index (MERGE) {} ({}) still running ({} / {})...", this.index.name, this.index.type, counter, count)
                    if (!indexTx.xodusTx.flush()) {
                        return false
                    }
                }
            }
        }

        /* Update stored VAFMarks. */
        indexTx.writeMarks(this.newMarks)

        /* Reset to default efficiency of VAF after rebuild. */
        indexTx.transaction.manager.statistics[this.index.name] = IndexStatistic(mapOf(VAFIndex.FILTER_EFFICIENCY_CACHE_KEY to VAFIndex.DEFAULT_FILTER_EFFICIENCY.toString()))
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
                is VAFIndexingEvent.Set -> this.store.put(this.xodusTx, next.tupleId.toKey(), next.signature.toEntry())
                is VAFIndexingEvent.Unset -> this.store.delete(this.xodusTx, next.tupleId.toKey())
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