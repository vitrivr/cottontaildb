package org.vitrivr.cottontail.dbms.index.va.rebuilder

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.RealVectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructuralMetadata
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractAsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.IndexRebuilderState
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.vitrivr.cottontail.dbms.index.va.signature.EquidistantVAFMarks
import org.vitrivr.cottontail.dbms.index.va.signature.VAFSignature
import org.vitrivr.cottontail.dbms.statistics.index.IndexStatistic
import org.vitrivr.cottontail.server.Instance
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * An [AbstractAsyncIndexRebuilder] that can be used to concurrently rebuild a [VAFIndex].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class AsyncVAFIndexRebuilder(index: VAFIndex, instance: Instance): AbstractAsyncIndexRebuilder<VAFIndex>(index, instance) {

    /** A temporary [Store] used to */
    private val tmpDataStore: Store = this.tmpEnvironment.openStore(this.index.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.tmpTx, true)
        ?: throw DatabaseException.DataCorruptionException("Temporary data store for index ${this.index.name} could not be created.")

    /** Reference to [EquidistantVAFMarks] used by this [AsyncVAFIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private lateinit var newMarks: EquidistantVAFMarks

    /** Reference to [EquidistantVAFMarks] used by this [AsyncVAFIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private lateinit var indexedColumn: ColumnDef<*>

    /** A [ConcurrentLinkedQueue] that acts as log for side-channel events. TODO: Make persistent. */
    private val log = ConcurrentLinkedQueue<VAFIndexingEvent>()

    /**
     * Internal scan method that is being executed when executing the BUILD stage of this [AsyncVAFIndexRebuilder].
     *
     * @param indexTx The [AbstractIndex.Tx] to execute the REPLACE stage with.
     * @return True on success, false otherwise.
     */
    override fun internalBuild(indexTx: AbstractIndex.Tx): Boolean {
        require(indexTx is VAFIndex.Tx) { "VAFIndexRebuilder can only be used with VAFIndex.Tx instances." }
        this.newMarks = VAFIndex.obtainMarks(indexTx)
        this.indexedColumn = indexTx.columns[0]

        /* Tx objects required for index rebuilding. */
        val count = indexTx.parent.count()

        /* Iterate over entity and update index with entries. */
        var counter = 0
        indexTx.parent.cursor(indexTx.columns).use { cursor ->
            while (cursor.hasNext()) {
                if (this.state != IndexRebuilderState.REBUILDING) return false
                val value = cursor.value()[0]
                if (value is RealVectorValue<*>) {
                    if (!this.tmpDataStore.add(this.tmpTx, cursor.key().toKey(), this.newMarks.getSignature(value).toEntry())) {
                        return false
                    }

                    if ((++counter) % 1_000_000 == 0) {
                        LOGGER.debug("Rebuilding index (SCAN) {} ({}) still running ({} / {})...", this.index.name, this.index.type, counter, count)
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
     * @param indexTx The [AbstractIndex.Tx] to execute the REPLACE stage with.
     * @param store The [Store] to merge data into.
     * @return True on success, false otherwise.
     */
    override fun internalReplace(indexTx: AbstractIndex.Tx, store: Store): Boolean {
        require(indexTx is VAFIndex.Tx) { "VAFIndexRebuilder can only be used with VAFIndex.Tx instances." }

        /* Begin replacement process. */
        val count = this.tmpDataStore.count(this.tmpTx)
        this.tmpDataStore.openCursor(this.tmpTx).use {cursor ->
            var counter = 0
            while (cursor.next) {
                if (this.state != IndexRebuilderState.REPLACING) return false
                if (!store.add(indexTx.xodusTx, cursor.key, cursor.value)) {
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
        IndexStructuralMetadata.write(indexTx, this.newMarks, EquidistantVAFMarks.Binding)

        /* Reset to default efficiency of VAF after rebuild. */
        indexTx.context.statistics[this.index.name] = listOf(IndexStatistic(VAFIndex.FILTER_EFFICIENCY_CACHE_KEY, VAFIndex.DEFAULT_FILTER_EFFICIENCY.toString()))
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
            val value = event.tuple[this.indexedColumn.name]
            if (value is RealVectorValue<*>) {
                this.log.offer(VAFIndexingEvent.Set(event.tuple.tupleId, this.newMarks.getSignature(value)))
            }
            true
        }

        /* Process side-channel DELETE. */
        is DataEvent.Delete -> {
            val value = event.oldTuple[this.indexedColumn.name]
            if (value != null) {
                this.log.offer(VAFIndexingEvent.Unset(event.oldTuple.tupleId))
            }
            true
        }

        /* Process side-channel UPDATE. */
        is DataEvent.Update -> {
            /* Extract value and perform sanity check. */
            val oldValue = event.oldTuple[this.indexedColumn.name]
            val newValue = event.newTuple[this.indexedColumn.name]

            /* Obtain marks and update them. */
            if (newValue is RealVectorValue<*>) {               /* Case 1: New value is not null, i.e., update to new value. */
                this.log.offer(VAFIndexingEvent.Set(event.newTuple.tupleId, this.newMarks.getSignature(newValue)))
            } else if (oldValue is RealVectorValue<*>) {        /* Case 2: New value is null but old value wasn't, i.e., delete index entry. */
                this.log.offer(VAFIndexingEvent.Unset(event.oldTuple.tupleId))
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