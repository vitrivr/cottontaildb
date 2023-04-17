package org.vitrivr.cottontail.dbms.index.pq.rebuilder

import jetbrains.exodus.bindings.ShortBinding
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructCatalogueEntry
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.pq.IVFPQIndex
import org.vitrivr.cottontail.dbms.index.pq.IVFPQIndexConfig
import org.vitrivr.cottontail.dbms.index.pq.quantizer.MultiStageQuantizer
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SerializableMultiStageProductQuantizer
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [AbstractIndexRebuilder] for the [IVFPQIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IVFPQIndexRebuilder(index: IVFPQIndex, context: QueryContext): AbstractIndexRebuilder<IVFPQIndex>(index, context) {

    /**
     * Starts the index rebuilding process for this [IVFPQIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(): Boolean {
        /* Read basic index properties. */
        val entry = IndexCatalogueEntry.read(this.index.name, this.index.catalogue, this.context.txn.xodusTx)
            ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index  ${this.index.name}: Could not read catalogue entry for index.")
        val config = entry.config as IVFPQIndexConfig
        val column = entry.columns[0]

        /* Tx objects required for index rebuilding. */
        val entityTx = this.index.parent.newTx(this.context)
        val columnTx = entityTx.columnForName(column).newTx(this.context)
        val dataStore = this.tryClearAndOpenStore() ?: return false
        val count = columnTx.count()

        /* Obtain PQ data structure. */
        val type = columnTx.columnDef.type as Types.Vector<*,*>
        val distanceFunction = this.index.catalogue.functions.obtain(Signature.SemiClosed(config.distance, arrayOf(Argument.Typed(type), Argument.Typed(type)))) as VectorDistance<*>
        val fraction = ((3.0f * config.numCentroids) / count)
        val seed = System.currentTimeMillis()
        val learningData = DataCollectionUtilities.acquireLearningData(columnTx, fraction, seed)
        val quantizer = MultiStageQuantizer.learnFromData(distanceFunction, learningData, config)

        /* Iterate over column and update index with entries. */
        var counter = 0
        columnTx.cursor().use { cursor ->
            while (cursor.moveNext()) {
                val value = cursor.value()
                if (value is VectorValue<*>) {
                    val signature = quantizer.quantize(cursor.key(), value)
                    if (!dataStore.put(this.context.txn.xodusTx, ShortBinding.shortToEntry(signature.first), signature.second.toEntry())) {
                        return false
                    }

                    /* Data is flushed every once in a while. */
                    if ((++counter) % 1_000_000 == 0) {
                        LOGGER.debug("Rebuilding index ${this.index.name} (${this.index.type}) still running ($counter / $count)...")
                        if (!this.context.txn.xodusTx.flush()) {
                            return false
                        }
                    }
                }
            }
        }

        /* Update stored ProductQuantizer. */
        IndexStructCatalogueEntry.write(this.index.name, quantizer.toSerializableProductQuantizer(), this.index.catalogue, this.context.txn.xodusTx, SerializableMultiStageProductQuantizer.Binding)
        return true
    }
}