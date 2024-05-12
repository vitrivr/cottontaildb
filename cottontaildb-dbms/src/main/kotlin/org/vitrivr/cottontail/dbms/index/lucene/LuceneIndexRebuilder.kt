package org.vitrivr.cottontail.dbms.index.lucene

import jetbrains.exodus.vfs.VirtualFileSystem
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.hash.UQBTreeIndex
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex.Tx
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.storage.lucene.XodusDirectory

/**
 * An [AbstractIndexRebuilder] for the [LuceneIndex].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class LuceneIndexRebuilder(index: LuceneIndex, context: QueryContext): AbstractIndexRebuilder<LuceneIndex>(index, context) {
    /**
     * Starts the index rebuilding process for this [LuceneIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(indexTx: AbstractIndex.Tx): Boolean {
        require(indexTx is LuceneIndex.Tx) { "LuceneIndexRebuilder can only be accessed with a LuceneIndex.Tx!" }

        /* Read basic index properties. */
        val indexMetadataStore = IndexMetadata.store(indexTx.parent.parent.parent.xodusTx)
        val indexEntryRaw = indexMetadataStore.get(indexTx.parent.parent.parent.xodusTx, NameBinding.Index.toEntry(this@LuceneIndexRebuilder.index.name)) ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index ${this@LuceneIndexRebuilder.index.name}: Could not read catalogue entry for index.")
        val indexEntry = IndexMetadata.fromEntry(indexEntryRaw)
        val column = this.index.name.entity().column(indexEntry.columns[0])

        /* Obtain Tx for parent entity. */
        val columnTx = indexTx.parent.columnForName(column).newTx(indexTx.parent)

        /* The [Directory] containing the data for this [LuceneIndex]. */
        /** A [VirtualFileSystem] that can be used with this [Tx]. */
        val vfs = VirtualFileSystem(indexTx.parent.dbo.environment)
        try {
            LuceneIndexDataStore(XodusDirectory(vfs, this.index.name.toString(), indexTx.xodusTx), column).use { store ->
                /* Delete all entries. */
                store.indexWriter.deleteAll()

                /* Iterate over entity and update index with entries. */
                columnTx.cursor().use { cursor ->
                    while (cursor.moveNext()) {
                        val value = cursor.value()
                        if (value is StringValue) {
                            store.addDocument(cursor.key(), value)
                            if (!indexTx.xodusTx.flush()) {

                                return false
                            }
                        }
                    }
                }

                return true
            }
        } finally {
            vfs.shutdown()
        }
    }
}