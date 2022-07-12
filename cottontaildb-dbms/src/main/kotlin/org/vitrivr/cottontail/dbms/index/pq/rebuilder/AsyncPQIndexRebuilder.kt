package org.vitrivr.cottontail.dbms.index.pq.rebuilder

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
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
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.index.pq.PQIndexConfig
import org.vitrivr.cottontail.dbms.index.pq.signature.PQSignature
import org.vitrivr.cottontail.dbms.index.pq.signature.ProductQuantizer
import org.vitrivr.cottontail.dbms.index.pq.signature.SerializableProductQuantizer
import org.vitrivr.cottontail.dbms.index.va.rebuilder.AsyncVAFIndexRebuilder

class AsyncPQIndexRebuilder(index: PQIndex): AbstractAsyncIndexRebuilder<PQIndex>(index) {
    /** The (temporary) Xodus [Store] used to store [PQSignature]s. */
    private val tmpDataStore: Store = this.tmpEnvironment.openStore(this.index.name.storeName(), StoreConfig.WITH_DUPLICATES, this.tmpTx, true)
        ?: throw DatabaseException.DataCorruptionException("Temporary data store for index ${this.index.name} could not be created.")

    /** Reference to [ProductQuantizer] used by this [AsyncPQIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private var newQuantizer: ProductQuantizer? = null

    /** Reference to [ColumnDef] indexed by this [AsyncPQIndexRebuilder] (only available after [IndexRebuilderState.INITIALIZED]). */
    private var indexedColumn: ColumnDef<*>? = null

    /**
     * Internal, modified rebuild method. This method basically scans the entity and writes all the changes to the surrounding snapshot.
     */
    override fun internalScan(context1: TransactionContext): Boolean {
        /* Read basic index properties. */
        val entry = IndexCatalogueEntry.read(this.index.name, this.index.catalogue, context1.xodusTx)
            ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index  ${this.index.name}: Could not read catalogue entry for index.")
        val config = entry.config as PQIndexConfig
        val column = entry.columns[0]

        /* Tx objects required for index rebuilding. */
        val entityTx = context1.getTx(this.index.parent) as EntityTx
        val columnTx = context1.getTx(entityTx.columnForName(column)) as ColumnTx<*>
        this.indexedColumn = columnTx.columnDef

        /* Generate and obtain signature and distance function. */
        val signature = Signature.Closed(config.distance, arrayOf(this.indexedColumn!!.type, this.indexedColumn!!.type), Types.Double)
        val distanceFunction: VectorDistance<*> = this.index.catalogue.functions.obtain(signature) as VectorDistance<*>

        /* Generates new product quantizer. */
        this.newQuantizer = ProductQuantizer.learnFromData(distanceFunction, PQIndexRebuilderUtilites.acquireLearningData(columnTx, config), config)

        /* Iterate over entity and update index with entries. */
        columnTx.cursor().use { cursor ->
            while (cursor.hasNext()) {
                if (this.state != IndexRebuilderState.SCANNING) return false
                val value = cursor.value()
                if (value is RealVectorValue<*>) {
                    val sig = this.newQuantizer!!.quantize(value)
                    this.tmpDataStore.add(this.tmpTx, PQSignature.Binding.valueToEntry(sig), cursor.key().toKey())
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
        this.tmpDataStore.openCursor(this.tmpTx).use { cursor ->
            while (cursor.next) {
                if (this.state != IndexRebuilderState.MERGING) return false
                store.put(context2.xodusTx, cursor.key, cursor.value)
            }
        }

        /* Update stored ProductQuantizer. */
        return IndexStructCatalogueEntry.write(this.index.name, this.newQuantizer!!.toSerializableProductQuantizer(), this.index.catalogue, context2.xodusTx, SerializableProductQuantizer.Binding)
    }

    /**
     * Internal method that apples a [DataEvent.Insert] from an external transaction to this [PQIndexRebuilder].
     *
     * @param event The [DataEvent.Insert] to process.
     * @return True on success, false otherwise.
     */
    override fun applyAsyncInsert(event: DataEvent.Insert): Boolean {
        /* Extract value and perform sanity check. */
        val value = event.data[this.indexedColumn] ?: return true

        /* If value is NULL, return true. NULL values are simply ignored by the PQIndex. */
        val sig = this.newQuantizer!!.quantize(value as VectorValue<*>)
        return this.tmpDataStore.put(this.tmpTx, PQSignature.Binding.valueToEntry(sig), event.tupleId.toKey())
    }

    /**
     * Internal method that apples a [DataEvent.Update] from an external transaction to this [PQIndexRebuilder].
     *
     * @param event The [DataEvent.Update] to process.
     * @return True on success, false otherwise.
     */
    override fun applyAsyncUpdate(event: DataEvent.Update): Boolean {
        /* Extract value and perform sanity check. */
        val oldValue = event.data[this.indexedColumn]?.first
        val newValue = event.data[this.indexedColumn]?.second

        /* Remove signature to tuple ID mapping. */
        if (oldValue != null) {
            val oldSig = this.newQuantizer!!.quantize(oldValue as VectorValue<*>)
            val cursor = this.tmpDataStore.openCursor(this.tmpTx)
            if (cursor.getSearchBoth(PQSignature.Binding.valueToEntry(oldSig), event.tupleId.toKey())) {
                cursor.deleteCurrent()
            }
            cursor.close()
        }

        /* Generate signature and store it. */
        if (newValue != null) {
            val newSig = this.newQuantizer!!.quantize(newValue as VectorValue<*>)
            return this.tmpDataStore.put(this.tmpTx, PQSignature.Binding.valueToEntry(newSig), event.tupleId.toKey())
        }
        return true
    }

    /**
     * Internal method that apples a [DataEvent.Delete] from an external transaction to this [PQIndexRebuilder].
     *
     * @param event The [DataEvent.Delete] to process.
     * @return True on success, false otherwise.
     */
    override fun applyAsyncDelete(event: DataEvent.Delete): Boolean {
        val oldValue = event.data[this.indexedColumn] ?: return true
        val sig = this.newQuantizer!!.quantize(oldValue as VectorValue<*>)
        val cursor = this.tmpDataStore.openCursor(this.tmpTx)
        if (cursor.getSearchBoth(PQSignature.Binding.valueToEntry(sig), event.tupleId.toKey())) {
            cursor.deleteCurrent()
        }
        cursor.close()
        return true
    }
}