package org.vitrivr.cottontail.dbms.index.pq.rebuilder

import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import jetbrains.exodus.bindings.LongBinding
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
import java.util.*

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

    /** List of all deleted [TupleId]s. */
    private val deletedTupleIds = Collections.synchronizedSet(LongOpenHashSet())

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
                    this.tmpDataStore.put(this.tmpTx, cursor.key().toKey(), sig.toEntry())

                    if ((++counter) % 1_000_000 == 0) {
                        LOGGER.debug("Rebuilding index (SCAN) ${this.index.name} (${this.index.type}) still running ($counter / $count)...")
                        if (!this.tmpTx.flush()) {
                            return false
                        }
                    }
                }

                /* Drain and process all events that appear on the side-channel; we do this every round. */
                if (!this.processSideChannelEvents()) {
                    return false
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
                if (LongBinding.compressedEntryToLong(cursor.key) !in this.deletedTupleIds) {
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
        }

        /* Update stored ProductQuantizer. */
        IndexStructCatalogueEntry.write(this.index.name, this.newQuantizer.toSerializableProductQuantizer(), this.index.catalogue, context.txn.xodusTx, SerializableSingleStageProductQuantizer.Binding)
        return true
    }

    /**
     * Drains and processes all [DataEvent]s that are currently waiting on the [sideChannelQueue].
     *
     * @return True on success, false otherwise.
     */
    override fun processSideChannelEvents(): Boolean {
        val local = LinkedList<DataEvent>()
        this.sideChannelQueue.drainTo(local)
        for (event in local) {
            when(event) {
                /* Process side-channel INSERT. */
                is DataEvent.Insert -> {
                    val value = event.data[this.indexedColumn]
                    if (value is VectorValue<*>) {
                        val sig = this.newQuantizer.quantize(value)
                        if (!this.tmpDataStore.add(this.tmpTx, event.tupleId.toKey(), sig.toEntry())) {
                            return false
                        }
                    }
                }

                /* Process side-channel DELETE. */
                is DataEvent.Delete -> {
                    val value = event.data[this.indexedColumn]
                    if (value != null) {
                        if (!this.tmpDataStore.delete(this.tmpTx, event.tupleId.toKey())) {
                            if (!this.deletedTupleIds.add(event.tupleId)) {
                                return false
                            }
                        }
                    }
                }

                /* Process side-channel UPDATE. */
                is DataEvent.Update -> {
                    /* Extract value and perform sanity check. */
                    val oldValue = event.data[this.indexedColumn]?.first
                    val newValue = event.data[this.indexedColumn]?.second

                    /* Obtain marks and update them. */
                    if (newValue is RealVectorValue<*>) {               /* Case 1: New value is not null, i.e., update to new value. */
                        val newSig = this.newQuantizer.quantize(newValue as VectorValue<*>)
                        if (this.tmpDataStore.put(this.tmpTx, event.tupleId.toKey(), newSig.toEntry())) {
                            return false
                        }
                        this.deletedTupleIds.remove(event.tupleId)
                    } else if (oldValue is RealVectorValue<*>) {        /* Case 2: New value is null but old value wasn't, i.e., delete index entry. */
                        if (!this.tmpDataStore.delete(this.tmpTx, event.tupleId.toKey())) {
                            this.deletedTupleIds.add(event.tupleId)     /* Defer delete into the future (if worker has not inserted tupleId yet). */
                        }
                    }
                }
            }
        }
        return true
    }
}