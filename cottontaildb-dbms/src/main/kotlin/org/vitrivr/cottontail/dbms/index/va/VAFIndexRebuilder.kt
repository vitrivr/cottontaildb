package org.vitrivr.cottontail.dbms.index.va

import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.IndexRebuilder
import org.vitrivr.cottontail.dbms.index.IndexRebuilderState
import org.vitrivr.cottontail.dbms.index.IndexState
import org.vitrivr.cottontail.dbms.index.va.signature.EquidistantVAFMarks
import org.vitrivr.cottontail.dbms.statistics.columns.VectorValueStatistics

/**
 * An [IndexRebuilder] that can be used to concurrently rebuild a [VAFIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class VAFIndexRebuilder(index: VAFIndex): IndexRebuilder<VAFIndex>(index) {

    /** */
    private val tmpDataStore = this.tmpEnvironment.openStore(this.index.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.tmpTx, true)
        ?: throw DatabaseException.DataCorruptionException("Temporary data store for index ${this.index.name} could not be created.")

    /** */
    private var newMarks: EquidistantVAFMarks? = null

    /** */
    private var indexedColumn: Name.ColumnName? = null

    /**
     * Internal, modified rebuild method. This method basically scans the entity and writes all the changes to the surrounding snapshot.
     */
    override fun internalScan(context: TransactionContext, async: Boolean) {
        VAFIndex.LOGGER.debug("Scanning VAF index {} for rebuild.", this.index.name)

        /* Prepares necessary Tx objects. */
        val indexTx = context.getTx(this.index) as VAFIndex.Tx
        val entityTx = context.getTx(this.index.parent) as EntityTx
        val columnTx = context.getTx(entityTx.columnForName(indexTx.columns[0].name)) as ColumnTx<*>

        /* Generates new marks. */
        val marks = EquidistantVAFMarks(columnTx.statistics() as VectorValueStatistics<*>, indexTx.config.marksPerDimension)
        this.newMarks = marks
        this.indexedColumn = indexTx.columns[0].name

        /* Creates temporary data store. */
        val dataStore = this.tmpEnvironment.openStore(this.index.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.tmpTx, true)
                ?: throw DatabaseException.DataCorruptionException("Temporary data store for index ${this.index.name} could not be created.")

        /* Iterate over entity and update index with entries. */
        columnTx.cursor().use { cursor ->
            while (cursor.hasNext() && this.state == IndexRebuilderState.INITIALIZED) {
                val value = cursor.value()
                if (value is RealVectorValue<*>) {
                    dataStore.put(this.tmpTx, cursor.key().toKey(), marks.getSignature(value).toEntry())
                }
            }
        }

        /* Update catalogue entry for index. */
        VAFIndex.LOGGER.debug("Scanning VAF index {} completed!", this.index.name)
    }

    /**
     * Merges this [VAFIndexRebuilder] with the surrounding [VAFIndex].
     */
    override fun internalMerge(context: TransactionContext) {
        VAFIndex.LOGGER.debug("Merging changes with VAF index {}.", this.index.name)

        /* Obtain index and clear it. */
        val indexTx = context.getTx(this.index) as VAFIndex.Tx
        indexTx.clear()

        /* Transfer data. */
        val dataStore = this.index.catalogue.environment.openStore(this.index.name.storeName(), StoreConfig.USE_EXISTING, context.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this.index.name} is missing.")
        val cursor = this.tmpDataStore.openCursor(this.tmpTx)
        while (cursor.next && this.state == IndexRebuilderState.SCANNED) {
            dataStore.putRight(context.xodusTx, cursor.key, cursor.value)
        }

        /* Update index state. */
        indexTx.updateState(IndexState.CLEAN, indexTx.config.copy(marks = this.newMarks))
        VAFIndex.LOGGER.debug("Rebuilding VAF index {} completed!", this.index)
    }

    /**
     * Internal method that apples a [DataEvent.Insert] from an external transaction to this [VAFIndexRebuilder].
     *
     * @param event The [DataEvent.Insert] to process.
     * @return True on success, false otherwise.
     */
    override fun applyInsert(event: DataEvent.Insert): Boolean {
        val value = event.data[this.indexedColumn] ?: return true
        return this.tmpDataStore.add(this.tmpTx, event.tupleId.toKey(), this.newMarks!!.getSignature(value as RealVectorValue<*>).toEntry())
    }

    /**
     * Internal method that apples a [DataEvent.Update] from an external transaction to this [VAFIndexRebuilder].
     *
     * @param event The [DataEvent.Update] to process.
     * @return True on success, false otherwise.
     */
    override fun applyUpdate(event: DataEvent.Update): Boolean {
        val oldValue = event.data[this.indexedColumn]?.first
        val newValue = event.data[this.indexedColumn]?.second

        /* Obtain marks and update them. */
        return if (newValue != null) { /* Case 1: New value is not null, i.e., update to new value. */
            this.tmpDataStore.put(this.tmpTx, event.tupleId.toKey(), this.newMarks?.getSignature(newValue as RealVectorValue<*>).toEntry())
        } else if (oldValue != null) { /* Case 2: New value is null but old value wasn't, i.e., delete index entry. */
            this.tmpDataStore.delete(this.tmpTx, event.tupleId.toKey())
        } else {
            true /* If value is NULL. */
        }
    }

    /**
     * Internal method that apples a [DataEvent.Delete] from an external transaction to this [VAFIndexRebuilder].
     *
     * @param event The [DataEvent.Delete] to process.
     * @return True on success, false otherwise.
     */
    override fun applyDelete(event: DataEvent.Delete): Boolean {
        return event.data[this.indexedColumn] == null || this.tmpDataStore.delete(this.tmpTx, event.tupleId.toKey())
    }
}