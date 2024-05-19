package org.vitrivr.cottontail.dbms.index.lucene

import jetbrains.exodus.vfs.VirtualFileSystem
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
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
        val column = indexTx.columns[0]

        /* The [Directory] containing the data for this [LuceneIndex]. */
        /** A [VirtualFileSystem] that can be used with this [Tx]. */
        LuceneIndexDataStore(XodusDirectory(this.index.name.toString(), indexTx.xodusTx), column.name).use { store ->
            /* Delete all entries. */
            store.indexWriter.deleteAll()

            /* Iterate over entity and update index with entries. */
            indexTx.parent.cursor(indexTx.columns).use { cursor ->
                while (cursor.moveNext()) {
                    val value = cursor.value()[0]
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
    }
}