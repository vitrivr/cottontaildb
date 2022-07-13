package org.vitrivr.cottontail.dbms.index.pq.rebuilder

import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.index.pq.PQIndexConfig
import org.vitrivr.cottontail.dbms.index.pq.signature.PQSignature
import org.vitrivr.cottontail.dbms.index.pq.signature.ProductQuantizer
import org.vitrivr.cottontail.dbms.index.pq.signature.SerializableProductQuantizer

/**
 * An [AbstractIndexRebuilder] for the [PQIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class PQIndexRebuilder(index: PQIndex, context: TransactionContext): AbstractIndexRebuilder<PQIndex>(index, context) {

    /**
     * Starts the index rebuilding process for this [PQIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(): Boolean {
        /* Read basic index properties. */
        val entry = IndexCatalogueEntry.read(this.index.name, this.index.catalogue, this.context.xodusTx)
            ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index  ${this.index.name}: Could not read catalogue entry for index.")
        val config = entry.config as PQIndexConfig
        val column = entry.columns[0]

        /* Tx objects required for index rebuilding. */
        val entityTx = this.context.getTx(this.index.parent) as EntityTx
        val columnTx = this.context.getTx(entityTx.columnForName(column)) as ColumnTx<*>
        val dataStore = this.tryClearAndOpenStore() ?: return false

        /* Obtain PQ data structure. */
        val type = columnTx.columnDef.type as Types.Vector<*,*>
        val distanceFunction = this.index.catalogue.functions.obtain(Signature.SemiClosed(config.distance, arrayOf(Argument.Typed(type), Argument.Typed(type)))) as VectorDistance<*>
        val learningData = PQIndexRebuilderUtilites.acquireLearningData(columnTx, config)
        val quantizer = ProductQuantizer.learnFromData(distanceFunction, learningData, config)

        /* Iterate over column and update index with entries. */
        columnTx.cursor().use { cursor ->
            while (cursor.moveNext()) {
                val value = cursor.value()
                if (value is VectorValue<*>) {
                    val sig = quantizer.quantize(value)
                    dataStore.put(this.context.xodusTx, PQSignature.Binding.valueToEntry(sig), cursor.key().toKey())
                }
            }
        }

        /* Update stored ProductQuantizer. */
        return IndexStructCatalogueEntry.write(this.index.name, quantizer.toSerializableProductQuantizer(), this.index.catalogue, context.xodusTx, SerializableProductQuantizer.Binding)
    }
}