package org.vitrivr.cottontail.dbms.index.hash

import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.index.basic.DefaultIndex
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [AbstractIndexRebuilder] for the [BTreeIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BTreeIndexRebuilder(index: AbstractBTreeIndex, context: QueryContext): AbstractIndexRebuilder<AbstractBTreeIndex>(index, context) {
    override fun rebuildInternal(indexTx: DefaultIndex.Tx): Boolean {
        require(indexTx is AbstractBTreeIndex.Tx) { "BTreeIndexRebuilder only supports BTreeIndex.Tx implementations." }

        /* Read basic index properties. */
        val column = indexTx.columns[0]

        /* Tx objects required for index rebuilding. */
        val columnTx = this.context.transaction.columnTx(column.name, AccessMode.READ)

        /* Iterate over entity and update index with entries. */
        columnTx.cursor().use { cursor ->
            while (cursor.moveNext()) {
                indexTx.addMapping(cursor.value(), cursor.key())
            }
        }

        /* Close cursor. */
        return true
    }
}