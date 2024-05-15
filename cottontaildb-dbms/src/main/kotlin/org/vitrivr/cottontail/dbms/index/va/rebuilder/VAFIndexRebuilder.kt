package org.vitrivr.cottontail.dbms.index.va.rebuilder

import org.vitrivr.cottontail.core.types.RealVectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructuralMetadata
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.vitrivr.cottontail.dbms.index.va.VAFIndexConfig
import org.vitrivr.cottontail.dbms.index.va.signature.EquidistantVAFMarks
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.values.RealVectorValueStatistics

/**
 * An [AbstractIndexRebuilder] for the [VAFIndex].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class VAFIndexRebuilder(index: VAFIndex, context: QueryContext): AbstractIndexRebuilder<VAFIndex>(index, context) {

    /**
     * Starts the index rebuilding process for this [VAFIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(indexTx: AbstractIndex.Tx): Boolean {
        require(indexTx is VAFIndex.Tx) { "VAFIndexRebuilder can only be accessed with a VAFIndex.Tx!" }

        /* Read basic index properties. */
        val indexMetadataStore = IndexMetadata.store(indexTx.parent.parent.parent.xodusTx)
        val indexEntryRaw = indexMetadataStore.get(indexTx.parent.parent.parent.xodusTx, NameBinding.Index.toEntry(this@VAFIndexRebuilder.index.name)) ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index ${this@VAFIndexRebuilder.index.name}: Could not read catalogue entry for index.")
        val indexEntry = IndexMetadata.fromEntry(indexEntryRaw)
        val config = indexEntry.config as VAFIndexConfig


        /* Tx objects required for index rebuilding. */
        val dataStore = this.tryClearAndOpenStore(indexTx) ?: return false
        val count = indexTx.parent.count()

        /* Obtain new marks. */
        val marks = EquidistantVAFMarks(indexTx.parent.statistics(indexTx.columns[0]) as RealVectorValueStatistics<*>, config.marksPerDimension)

        /* Iterate over entity and update index with entries. */
        var counter = 1
        indexTx.parent.cursor(indexTx.columns).use { cursor ->
            while (cursor.hasNext()) {
                val value = cursor.value()[0]
                if (value is RealVectorValue<*>) {
                    if (!dataStore.put(indexTx.xodusTx, cursor.key().toKey(), marks.getSignature(value).toEntry())) {
                        return false
                    }

                    /* Data is flushed every once in a while. */
                    if ((counter ++) % 1_000_000 == 0) {
                        LOGGER.debug("Rebuilding index {} ({}) still running ({} / {})...", this.index.name, this.index.type, counter, count)
                        if (!indexTx.xodusTx.flush()) {
                            return false
                        }
                    }
                }
            }
        }

        /* Update stored VAFMarks. */
        IndexStructuralMetadata.write(indexTx, marks, EquidistantVAFMarks.Binding)
        return true
    }
}