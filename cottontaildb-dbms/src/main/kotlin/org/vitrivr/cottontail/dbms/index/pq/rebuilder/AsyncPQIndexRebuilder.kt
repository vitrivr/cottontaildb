package org.vitrivr.cottontail.dbms.index.pq.rebuilder

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractAsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.IndexRebuilderState
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.index.pq.PQIndexConfig
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SingleStageQuantizer
import org.vitrivr.cottontail.dbms.index.pq.signature.SPQSignature
import org.vitrivr.cottontail.dbms.index.va.rebuilder.AsyncVAFIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * An [AbstractAsyncIndexRebuilder] that can be used to concurrently rebuild a  [PQIndex].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class AsyncPQIndexRebuilder(index: PQIndex, context: QueryContext): AbstractAsyncIndexRebuilder<PQIndex>(index, context.transaction.manager) {

    /** The (temporary) Xodus [Store] used to store [SPQSignature]s. */
    private val store: Store = this.environment.openStore(this.index.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.xodusTx, true)
        ?: throw DatabaseException.DataCorruptionException("Temporary data store for index ${this.index.name} could not be created.")

    /** Reference to [SingleStageQuantizer] used by this [AsyncPQIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private val newQuantizer: SingleStageQuantizer

    /** Reference to [ColumnDef] indexed by this [AsyncPQIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private val indexedColumn: ColumnDef<*>

    /** A [ConcurrentLinkedQueue] that acts as log for side-channel events. TODO: Make persistent. */
    private val log = ConcurrentLinkedQueue<PQIndexingEvent>()

    init {
        /* Read basic index properties. */
        val indexTx = context.transaction.indexTx(index.name, AccessMode.READ)
        val config = indexTx.config as PQIndexConfig
        this.indexedColumn = indexTx.columns[0]

        /* Tx objects required for index rebuilding. */
        val entityTx = context.transaction.entityTx(this.index.parent.name, AccessMode.READ)
        val columnTx = context.transaction.columnTx(this.indexedColumn.name, AccessMode.READ)
        val count = entityTx.count()

        /* Generate and obtain signature and distance function. */
        val signature = Signature.Closed(config.distance, arrayOf(this.indexedColumn.type, this.indexedColumn.type), Types.Double)
        val distanceFunction: VectorDistance<*> = this.index.catalogue.functions.obtain(signature) as VectorDistance<*>

        /* Generates new product quantize. */
        val fraction = ((3.0f * config.numCentroids) / count)
        val seed = System.currentTimeMillis()
        val learningData = DataCollectionUtilities.acquireLearningData(columnTx, fraction, seed)
        this.newQuantizer = SingleStageQuantizer.learnFromData(distanceFunction, learningData, config)
    }

    /**
     * Internal scan method that is being executed when executing the BUILD stage of this [AsyncPQIndexRebuilder].
     *
     * @param context The [QueryContext] to execute the BUILD stage in.
     * @return True on success, false otherwise.
     */
    override fun internalBuild(context: QueryContext): Boolean {
        /* Tx objects required for index rebuilding. */
        val entityTx = context.transaction.entityTx(this.index.parent.name, AccessMode.READ)
        val columnTx = context.transaction.columnTx(this.indexedColumn.name, AccessMode.READ)
        val count = entityTx.count()

        /* Iterate over entity and update index with entries. */
        var counter = 0
        columnTx.cursor().use { cursor ->
            while (cursor.hasNext()) {
                if (this.state != IndexRebuilderState.REBUILDING) return false
                val value = cursor.value()
                if (value is VectorValue<*>) {
                    if (!this.store.add(this.xodusTx, cursor.key().toKey(), this.newQuantizer.quantize(value).toEntry())) {
                        return false
                    }

                    if ((++counter) % 1_000_000 == 0) {
                        LOGGER.debug("Rebuilding index (SCAN) {} ({}) still running ({} / {})...", this.index.name, this.index.type, counter, count)
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
        require(indexTx is PQIndex.Tx) { "AsyncPQIndexRebuilder only supports PQIndex.Tx implementations." }

        /* Start replacement process. */
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

        /* Update stored ProductQuantizer. */
        indexTx.updateQuantizer(this.newQuantizer.toSerializableProductQuantizer())
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
            val value = event.data[this.indexedColumn]
            if (value is VectorValue<*>) {
                this.log.offer(PQIndexingEvent.Set(event.tupleId, this.newQuantizer.quantize(value)))
            }
            true
        }

        /* Process side-channel DELETE. */
        is DataEvent.Delete -> {
            val value = event.data[this.indexedColumn]
            if (value != null) {
                this.log.offer(PQIndexingEvent.Unset(event.tupleId))
            }
            true
        }

        /* Process side-channel UPDATE. */
        is DataEvent.Update -> {
            /* Extract value and perform sanity check. */
            val oldValue = event.data[this.indexedColumn]?.first
            val newValue = event.data[this.indexedColumn]?.second

            /* Obtain marks and update them. */
            if (newValue is VectorValue<*>) {                   /* Case 1: New value is not null, i.e., update to new value. */
                this.log.offer(PQIndexingEvent.Set(event.tupleId, this.newQuantizer.quantize(newValue)))
            } else if (oldValue is VectorValue<*>) {        /* Case 2: New value is null but old value wasn't, i.e., delete index entry. */
                this.log.offer(PQIndexingEvent.Unset(event.tupleId))
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
                is PQIndexingEvent.Set -> this.store.put(this.xodusTx, next.tupleId.toKey(), next.signature.toEntry())
                is PQIndexingEvent.Unset -> this.store.delete(this.xodusTx, next.tupleId.toKey())
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