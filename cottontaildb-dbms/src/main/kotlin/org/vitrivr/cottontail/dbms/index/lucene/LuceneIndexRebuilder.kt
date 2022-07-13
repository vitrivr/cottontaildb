package org.vitrivr.cottontail.dbms.index.lucene

import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.storage.lucene.XodusDirectory

/**
 * An [AbstractIndexRebuilder] for the [LuceneIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LuceneIndexRebuilder(index: LuceneIndex, context: TransactionContext): AbstractIndexRebuilder<LuceneIndex>(index, context) {
    /**
     * Starts the index rebuilding process for this [LuceneIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    @Suppress("UNCHECKED_CAST")
    override fun rebuildInternal(): Boolean {
        /* Read basic index properties. */
        val entry = IndexCatalogueEntry.read(this.index.name, this.index.catalogue, this.context.xodusTx)
            ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index  ${this.index.name}: Could not read catalogue entry for index.")
        val column = entry.columns[0]


        /* Obtain Tx for parent entity. */
        val entityTx = this.context.getTx(this.index.parent) as EntityTx
        val columnTx = this.context.getTx(entityTx.columnForName(column)) as ColumnTx<StringValue>

        /* The [Directory] containing the data for this [LuceneIndex]. */
        LuceneIndexDataStore(XodusDirectory(this.index.catalogue.vfs, this.index.name.toString(), this.context.xodusTx), column).use { store ->
            /* Delete all entries. */
            store.indexWriter.deleteAll()

            /* Iterate over entity and update index with entries. */
            columnTx.cursor().use { cursor ->
                while (cursor.moveNext()) {
                    val value = cursor.value()
                    if (value is StringValue) {
                        store.addDocument(cursor.key(), value)
                        if (!this.context.xodusTx.flush()) {
                            return false
                        }
                    }
                }
            }
            return true
        }
    }
}