package org.vitrivr.cottontail.dbms.index.pq.rebuilder

import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructuralMetadata
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SerializableSingleStageProductQuantizer
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [AbstractIndexRebuilder] for the [PQIndex].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class PQIndexRebuilder(index: PQIndex, context: QueryContext): AbstractIndexRebuilder<PQIndex>(index, context) {

    /**
     * Starts the index rebuilding process for this [PQIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(indexTx: AbstractIndex.Tx): Boolean {
        require(indexTx is PQIndex.Tx) { "PQIndexRebuilder can only be accessed with a PQIndex.Tx!" }

        /* Tx objects required for index rebuilding. */
        val dataStore = this.tryClearAndOpenStore(indexTx) ?: return false
        val count = indexTx.parent.count()

        /* Train new quantizer. */
        val quantizer = PQIndex.trainQuantizer(indexTx)

        /* Iterate over column and update index with entries. */
        var counter = 0
        indexTx.parent.cursor(indexTx.columns).use { cursor ->
            while (cursor.moveNext()) {
                val value = cursor.value()[0]
                if (value is VectorValue<*>) {
                    if (!dataStore.put(indexTx.parent.xodusTx, cursor.key().toKey(), quantizer.quantize(value).toEntry())) {
                        return false
                    }

                    /* Data is flushed every once in a while. */
                    if ((++counter) % 1_000_000 == 0) {
                        LOGGER.debug("Rebuilding index {} ({}) still running ({} / {})...", this.index.name, this.index.type, counter, count)
                        if (!indexTx.parent.xodusTx.flush()) {
                            return false
                        }
                    }
                }
            }
        }

        /* Update stored ProductQuantizer. */
        IndexStructuralMetadata.write(indexTx, quantizer.toSerializableProductQuantizer(), SerializableSingleStageProductQuantizer.Binding)
        return true
    }
}