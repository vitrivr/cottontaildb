package org.vitrivr.cottontail.dbms.index.lucene

import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.index.basic.DefaultIndex
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

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

    override fun rebuildInternal(indexTx: DefaultIndex.Tx): Boolean {
        require(indexTx is LuceneIndex.Tx) { "LuceneIndexRebuilder only supports LuceneIndex.Tx implementations." }

        /* Obtain Tx for parent entity. */
        val columnTx = indexTx.transaction.columnTx(indexTx.columns[0].name, AccessMode.READ)

        indexTx.truncate()

        /* Iterate over entity and update index with entries. */
        columnTx.cursor().use { cursor ->
            while (cursor.moveNext()) {
                val value = cursor.value()
                if (value is StringValue) {
                    indexTx.store.addDocument(cursor.key(), value)
                }
            }
        }
        return true
    }
}