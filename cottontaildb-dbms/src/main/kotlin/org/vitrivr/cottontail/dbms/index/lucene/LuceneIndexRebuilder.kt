package org.vitrivr.cottontail.dbms.index.lucene

import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.storage.lucene.XodusDirectory

/**
 * An [AbstractIndexRebuilder] for the [LuceneIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LuceneIndexRebuilder(index: LuceneIndex, context: QueryContext): AbstractIndexRebuilder<LuceneIndex>(index, context) {
    /**
     * Starts the index rebuilding process for this [LuceneIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(): Boolean {
        /* Read basic index properties. */
        val indexMetadataStore = IndexMetadata.store(this.index.catalogue, context.txn.xodusTx)
        val indexEntryRaw = indexMetadataStore.get(context.txn.xodusTx, NameBinding.Index.toEntry(this@LuceneIndexRebuilder.index.name)) ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index ${this@LuceneIndexRebuilder.index.name}: Could not read catalogue entry for index.")
        val indexEntry = IndexMetadata.fromEntry(indexEntryRaw)
        val column = this.index.name.entity().column(indexEntry.columns[0])

        /* Obtain Tx for parent entity. */
        val entityTx = this.index.parent.newTx(this.context)
        val columnTx = entityTx.columnForName(column).newTx(this.context)

        /* The [Directory] containing the data for this [LuceneIndex]. */
        LuceneIndexDataStore(XodusDirectory(this.index.catalogue.transactionManager.vfs, this.index.name.toString(), this.context.txn.xodusTx), column).use { store ->
            /* Delete all entries. */
            store.indexWriter.deleteAll()

            /* Iterate over entity and update index with entries. */
            columnTx.cursor().use { cursor ->
                while (cursor.moveNext()) {
                    val value = cursor.value()
                    if (value is StringValue) {
                        store.addDocument(cursor.key(), value)
                        if (!this.context.txn.xodusTx.flush()) {
                            return false
                        }
                    }
                }
            }
            return true
        }
    }
}