package org.vitrivr.cottontail.dbms.index.va.rebuilder

import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.vitrivr.cottontail.dbms.index.va.VAFIndexConfig
import org.vitrivr.cottontail.dbms.index.va.signature.EquidistantVAFMarks
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.metricsData.RealVectorValueMetrics
import org.vitrivr.cottontail.dbms.statistics.values.RealVectorValueStatistics

/**
 * An [AbstractIndexRebuilder] for the [VAFIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class VAFIndexRebuilder(index: VAFIndex, context: QueryContext): AbstractIndexRebuilder<VAFIndex>(index, context) {

    /**
     * Starts the index rebuilding process for this [VAFIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(): Boolean {
        /* Read basic index properties. */
        val entry = IndexCatalogueEntry.read(this.index.name, this.index.catalogue, this.context.txn.xodusTx)
            ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index  ${this.index.name}: Could not read catalogue entry for index.")
        val config = entry.config as VAFIndexConfig
        val column = entry.columns[0]

        /* Tx objects required for index rebuilding. */
        val entityTx = this.index.parent.newTx(this.context)
        val columnTx = entityTx.columnForName(column).newTx(this.context) as ColumnTx<*>
        val dataStore = this.tryClearAndOpenStore() ?: return false
        val count = columnTx.count()

        /* Obtain new marks. */
        val marks = EquidistantVAFMarks(columnTx.statistics() as RealVectorValueMetrics<*>, config.marksPerDimension)

        /* Iterate over entity and update index with entries. */
        var counter = 1
        columnTx.cursor().use { cursor ->
            while (cursor.hasNext()) {
                val value = cursor.value()
                if (value is RealVectorValue<*>) {
                    if (!dataStore.put(this.context.txn.xodusTx, cursor.key().toKey(), marks.getSignature(value).toEntry())) {
                        return false
                    }

                    /* Data is flushed every once in a while. */
                    if ((counter ++) % 1_000_000 == 0) {
                        LOGGER.debug("Rebuilding index ${this.index.name} (${this.index.type}) still running ($counter / $count)...")
                        if (!this.context.txn.xodusTx.flush()) {
                            return false
                        }
                    }
                }
            }
        }

        /* Update stored VAFMarks. */
        IndexStructCatalogueEntry.write(this.index.name, marks, this.index.catalogue, this.context.txn.xodusTx, EquidistantVAFMarks.Binding)
        return true
    }
}