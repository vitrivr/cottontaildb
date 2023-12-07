package org.vitrivr.cottontail.dbms.index.lsh

import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [AbstractIndexRebuilder] for the [LSHIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LSHIndexRebuilder(index: LSHIndex, context: QueryContext): AbstractIndexRebuilder<LSHIndex>(index, context) {

    /**
     * Performs the index rebuilding process for this [LSHIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(): Boolean {
        /* Read basic index properties. */
        val indexMetadataStore = IndexMetadata.store(this.index.catalogue, context.txn.xodusTx)
        val indexEntryRaw = indexMetadataStore.get(context.txn.xodusTx, NameBinding.Index.toEntry(this@LSHIndexRebuilder.index.name)) ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index ${this@LSHIndexRebuilder.index.name}: Could not read catalogue entry for index.")
        val indexEntry = IndexMetadata.fromEntry(indexEntryRaw)
        val config = indexEntry.config as LSHIndexConfig
        val column = this.index.name.entity().column(indexEntry.columns[0])

        /* Tx objects required for index rebuilding. */
        val entityTx = this.index.parent.newTx(this.context)
        val columnTx = entityTx.columnForName(column).newTx(this.context)
        val dataStore = this.tryClearAndOpenStore() ?: return false

        /* Generate a new LSHSignature for each entry in the entity and adds it to the index. */
        val generator = config.generator(columnTx.columnDef.type.logicalSize)
        val cursor = columnTx.cursor()
        val wrappedStore = LSHDataStore(dataStore)
        while (cursor.moveNext()) {
            val tupleId = cursor.key()
            val value = cursor.next()
            if (value is VectorValue<*>) {
                wrappedStore.addMapping(this.context.txn.xodusTx, generator.generate(value), tupleId)
            }
        }

        /* Close cursor. */
        cursor.close()

        return true
    }
}