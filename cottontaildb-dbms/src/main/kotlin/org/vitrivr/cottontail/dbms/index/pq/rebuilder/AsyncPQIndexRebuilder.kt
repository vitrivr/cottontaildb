package org.vitrivr.cottontail.dbms.index.pq.rebuilder

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractAsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.IndexRebuilderState
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.index.pq.PQIndexConfig
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SerializableSingleStageProductQuantizer
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SingleStageQuantizer
import org.vitrivr.cottontail.dbms.index.pq.signature.SPQSignature
import org.vitrivr.cottontail.dbms.index.va.rebuilder.AsyncVAFIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * An [AbstractAsyncIndexRebuilder] that can be used to concurrently rebuild a  [PQIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class AsyncPQIndexRebuilder(index: PQIndex, context: QueryContext): AbstractAsyncIndexRebuilder<PQIndex>(index, context.catalogue, context.txn.manager) {

    /** The (temporary) Xodus [Store] used to store [SPQSignature]s. */
    private val tmpDataStore: Store = this.tmpEnvironment.openStore(this.index.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.tmpTx, true)
        ?: throw DatabaseException.DataCorruptionException("Temporary data store for index ${this.index.name} could not be created.")

    /** Reference to [SingleStageQuantizer] used by this [AsyncPQIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private val newQuantizer: SingleStageQuantizer

    /** Reference to [ColumnDef] indexed by this [AsyncPQIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private val indexedColumn: ColumnDef<*>?

    /** A [ConcurrentLinkedQueue] that acts as log for side-channel events. TODO: Make persistent. */
    private val log = ConcurrentLinkedQueue<PQIndexingEvent>()

    init {
        /* Read basic index properties. */
        val entry = IndexCatalogueEntry.read(this.index.name, this.index.catalogue, context.txn.xodusTx)
            ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index  ${this.index.name}: Could not read catalogue entry for index.")
        val config = entry.config as PQIndexConfig
        val column = entry.columns[0]

        /* Tx objects required for index rebuilding. */
        val entityTx = this.index.parent.newTx(context)
        val columnTx = entityTx.columnForName(column).newTx(context)
        val count = columnTx.count()
        this.indexedColumn = columnTx.columnDef

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
     * Internal, modified rebuild method. This method basically scans the entity and writes all the changes to the surrounding snapshot.
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
                if (this.state != IndexRebuilderState.SCANNING) return false
                val value = cursor.value()
                if (value is VectorValue<*>) {
                    /* Add signature. */
                    val sig = this.newQuantizer.quantize(value)
                    if (!this.tmpDataStore.add(this.tmpTx, cursor.key().toKey(), sig.toEntry())) {
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
        /* Start replacement process. */
        val count = this.tmpDataStore.count(this.tmpTx)
        this.tmpDataStore.openCursor(this.tmpTx).use { cursor ->
            var counter = 0
            while (cursor.next) {
                if (this.state != IndexRebuilderState.MERGING) return false
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

        /* Update stored ProductQuantizer. */
        IndexStructCatalogueEntry.write(this.index.name, this.newQuantizer.toSerializableProductQuantizer(), this.index.catalogue, context.txn.xodusTx, SerializableSingleStageProductQuantizer.Binding)
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
            } else if (oldValue is RealVectorValue<*>) {        /* Case 2: New value is null but old value wasn't, i.e., delete index entry. */
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