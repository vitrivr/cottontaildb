package org.vitrivr.cottontail.dbms.index.lsh

import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.hash.UQBTreeIndex
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [AbstractIndexRebuilder] for the [LSHIndex].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class LSHIndexRebuilder(index: LSHIndex, context: QueryContext): AbstractIndexRebuilder<LSHIndex>(index, context) {

    /**
     * Performs the index rebuilding process for this [LSHIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(indexTx: AbstractIndex.Tx): Boolean {
        /* Read basic index properties. */
        val indexMetadataStore = IndexMetadata.store(indexTx.parent.parent.parent.xodusTx)
        val indexEntryRaw = indexMetadataStore.get(indexTx.parent.xodusTx, NameBinding.Index.toEntry(this@LSHIndexRebuilder.index.name)) ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index ${this@LSHIndexRebuilder.index.name}: Could not read catalogue entry for index.")
        val indexEntry = IndexMetadata.fromEntry(indexEntryRaw)
        val config = indexEntry.config as LSHIndexConfig
        val column = this.index.name.entity().column(indexEntry.columns[0])

        /* Tx objects required for index rebuilding. */
        val columnTx = indexTx.parent.columnForName(column).newTx(indexTx.parent)
        val dataStore = this.tryClearAndOpenStore(indexTx) ?: return false

        /* Generate a new LSHSignature for each entry in the entity and adds it to the index. */
        val generator = config.generator(columnTx.columnDef.type.logicalSize)
        columnTx.cursor().use { cursor ->
            val wrappedStore = LSHDataStore(dataStore)
            while (cursor.moveNext()) {
                val tupleId = cursor.key()
                val value = cursor.next()
                if (value is VectorValue<*>) {
                    wrappedStore.addMapping(indexTx.xodusTx, generator.generate(value), tupleId)
                }
            }
        }
        return true
    }
}