package org.vitrivr.cottontail.dbms.index.pq.rebuilder

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.RealVectorValue
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructuralMetadata
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractAsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.IndexRebuilderState
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SerializableSingleStageProductQuantizer
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SingleStageQuantizer
import org.vitrivr.cottontail.dbms.index.pq.signature.SPQSignature
import org.vitrivr.cottontail.dbms.index.va.rebuilder.AsyncVAFIndexRebuilder
import org.vitrivr.cottontail.server.Instance
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * An [AbstractAsyncIndexRebuilder] that can be used to concurrently rebuild a  [PQIndex].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class AsyncPQIndexRebuilder(index: PQIndex, instance: Instance): AbstractAsyncIndexRebuilder<PQIndex>(index, instance) {

    /** The (temporary) Xodus [Store] used to store [SPQSignature]s. */
    private val tmpDataStore: Store = this.tmpEnvironment.openStore(this.index.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.tmpTx, true)
        ?: throw DatabaseException.DataCorruptionException("Temporary data store for index ${this.index.name} could not be created.")

    /** Reference to [SingleStageQuantizer] used by this [AsyncPQIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private lateinit var newQuantizer: SingleStageQuantizer

    /** The [ColumnDef] of the indexed column. */
    private lateinit var indexedColumn: ColumnDef<*>

    /** A [ConcurrentLinkedQueue] that acts as log for side-channel events. */
    private val log = ConcurrentLinkedQueue<PQIndexingEvent>()

    /**
     * Internal scan method that is being executed when executing the BUILD stage of this [AsyncPQIndexRebuilder].
     *
     * @param indexTx The [AbstractIndex.Tx] to execute the BUILD stage with.
     * @return True on success, false otherwise.
     */
    override fun internalBuild(indexTx: AbstractIndex.Tx): Boolean {
        /* Train a new quantizer. */
        this.newQuantizer = PQIndex.trainQuantizer(indexTx)
        this.indexedColumn = indexTx.columns[0]

        /* Tx objects required for index rebuilding. */
        val count = indexTx.parent.count()
        val column = indexTx.columns[0]

        /* Iterate over entity and update index with entries. */
        var counter = 0
        indexTx.parent.cursor().use { cursor ->
            while (cursor.hasNext()) {
                if (this.state != IndexRebuilderState.REBUILDING) return false
                val value = cursor.value()[column]
                if (value is VectorValue<*>) {
                    if (!this.tmpDataStore.add(this.tmpTx, cursor.key().toKey(), this.newQuantizer.quantize(value).toEntry())) {
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
        /* Start replacement process. */
        val count = this.tmpDataStore.count(this.tmpTx)
        this.tmpDataStore.openCursor(this.tmpTx).use { cursor ->
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

        /* Update stored ProductQuantizer. */
        IndexStructuralMetadata.write(indexTx, this.newQuantizer.toSerializableProductQuantizer(), SerializableSingleStageProductQuantizer.Binding)
        return true
    }

    /**
     * Processes a [DataEvent] received from the side-channel, converts it to a [PQIndexingEvent] and appends it to the log.
     *
     * @param event The [DataEvent] that should be processed,
     */
    override fun processSideChannelEvent(event: DataEvent): Boolean = when(event) {
        /* Process side-channel INSERT. */
        is DataEvent.Insert -> {
            val value = event.tuple[this.indexedColumn.name]
            if (value is RealVectorValue<*>) {
                this.log.offer(PQIndexingEvent.Set(event.tuple.tupleId, this.newQuantizer.quantize(value)))
            }
            true
        }

        /* Process side-channel DELETE. */
        is DataEvent.Delete -> {
            val value = event.oldTuple[this.indexedColumn.name]
            if (value != null) {
                this.log.offer(PQIndexingEvent.Unset(event.oldTuple.tupleId))
            }
            true
        }

        /* Process side-channel UPDATE. */
        is DataEvent.Update -> {
            /* Extract value and perform sanity check. */
            val oldValue = event.oldTuple[this.indexedColumn.name]
            val newValue = event.newTuple[this.indexedColumn.name]

            /* Obtain marks and update them. */
            if (newValue is RealVectorValue<*>) {                   /* Case 1: New value is not null, i.e., update to new value. */
                this.log.offer(PQIndexingEvent.Set(event.newTuple.tupleId, this.newQuantizer.quantize(newValue)))
            } else if (oldValue is RealVectorValue<*>) {        /* Case 2: New value is null but old value wasn't, i.e., delete index entry. */
                this.log.offer(PQIndexingEvent.Unset(event.oldTuple.tupleId))
            }
            true
        }
    }

    /**
     * Drains and processes all [PQIndexingEvent]s that are currently waiting on the [log].
     *
     * @return True on success, false otherwise.
     */
    override fun drainAndMergeLog(): Boolean {
        var next = this.log.poll()
        while (next != null) {
            val success = when (next) {
                is PQIndexingEvent.Set -> this.tmpDataStore.put(this.tmpTx, next.tupleId.toKey(), next.signature.toEntry())
                is PQIndexingEvent.Unset -> this.tmpDataStore.delete(this.tmpTx, next.tupleId.toKey())
            }
            if (!success) return false
            next = this.log.poll()
        }
        return true
    }

    /**
     * An internal helper interface used by the [AsyncPQIndexRebuilder].
     *
     * Every [DataEvent] is converted to a [PQIndexingEvent.Set] or [PQIndexingEvent.Unset].
     * Order of operations is ensured by the log.
     */
    private sealed interface PQIndexingEvent {
        val tupleId: TupleId

        /**
         * A log entry that signifies the appending of a [SPQSignature] to the index.
         */
        data class Set(override val tupleId: TupleId, val signature: SPQSignature): PQIndexingEvent

        /**
         * A log entry that signifies the removal of a [SPQSignature] from the index.
         */
        data class Unset(override val tupleId: TupleId): PQIndexingEvent
    }
}