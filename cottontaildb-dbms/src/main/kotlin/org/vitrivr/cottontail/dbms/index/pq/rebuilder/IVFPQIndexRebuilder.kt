package org.vitrivr.cottontail.dbms.index.pq.rebuilder

import jetbrains.exodus.bindings.ShortBinding
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructuralMetadata
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata
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
 * @version 2.0.0
 */
class IVFPQIndexRebuilder(index: IVFPQIndex, context: QueryContext): AbstractIndexRebuilder<IVFPQIndex>(index, context) {

    /**
     * Starts the index rebuilding process for this [IVFPQIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(indexTx: AbstractIndex.Tx): Boolean {
        require(indexTx is IVFPQIndex.Tx) { "IVFPQIndexRebuilder can only be accessed with a IVFPQIndex.Tx!" }

        /* Read basic index properties. */
        val indexMetadataStore = IndexMetadata.store(indexTx.parent.parent.xodusTx)
        val indexEntryRaw = indexMetadataStore.get(indexTx.parent.parent.xodusTx, NameBinding.Index.toEntry(this@IVFPQIndexRebuilder.index.name)) ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index ${this@IVFPQIndexRebuilder.index.name}: Could not read catalogue entry for index.")
        val indexEntry = IndexMetadata.fromEntry(indexEntryRaw)
        val config = indexEntry.config as IVFPQIndexConfig
        val column = indexTx.columns[0]

        /* Tx objects required for index rebuilding. */
        val dataStore = this.tryClearAndOpenStore(indexTx) ?: return false
        val count = indexTx.parent.count()

        /* Obtain PQ data structure. */
        val type = column.type as Types.Vector<*,*>
        val distanceFunction = this.index.catalogue.functions.obtain(Signature.SemiClosed(config.distance, arrayOf(Argument.Typed(type), Argument.Typed(type)))) as VectorDistance<*>
        val fraction = ((3.0f * config.numCentroids) / count)
        val seed = System.currentTimeMillis()
        val learningData = DataCollectionUtilities.acquireLearningData(indexTx.parent, column, fraction, seed)
        val quantizer = MultiStageQuantizer.learnFromData(distanceFunction, learningData, config)

        /* Iterate over column and update index with entries. */
        var counter = 0
        indexTx.parent.cursor(indexTx.columns).use { cursor ->
            while (cursor.moveNext()) {
                val value = cursor.value()[0]
                if (value is VectorValue<*>) {
                    val signature = quantizer.quantize(cursor.key(), value)
                    if (!dataStore.put(indexTx.xodusTx, ShortBinding.shortToEntry(signature.first), signature.second.toEntry())) {
                        return false
                    }

                    /* Data is flushed every once in a while. */
                    if ((++counter) % 1_000_000 == 0) {
                        LOGGER.debug("Rebuilding index {} ({}) still running ({} / {})...", this.index.name, this.index.type, counter, count)
                        if (!indexTx.xodusTx.flush()) {
                            return false
                        }
                    }
                }
            }
        }

        /* Update stored ProductQuantizer. */
        IndexStructuralMetadata.write(indexTx, quantizer.toSerializableProductQuantizer(), SerializableMultiStageProductQuantizer.Binding)
        return true
    }
}