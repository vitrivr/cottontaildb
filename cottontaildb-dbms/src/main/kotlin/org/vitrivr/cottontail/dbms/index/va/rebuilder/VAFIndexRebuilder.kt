package org.vitrivr.cottontail.dbms.index.va.rebuilder

import org.vitrivr.cottontail.core.values.RealVectorValue
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.index.basic.DefaultIndex
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

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
    override fun rebuildInternal(indexTx: DefaultIndex.Tx): Boolean {
        require(indexTx is VAFIndex.Tx) { "VAFIndexRebuilder only supports VAFIndex.Tx implementations." }

        /* Tx objects required for index rebuilding. */
        val columnTx = this.context.transaction.columnTx(indexTx.columns[0].name, AccessMode.READ)

        /* Obtain new marks. */
        val marks = indexTx.readMarks()

        /* Iterate over entity and update index with entries. */
        var counter = 1
        val store = indexTx.store
        columnTx.cursor().use { cursor ->
            while (cursor.hasNext()) {
                val value = cursor.value()
                if (value is RealVectorValue<*>) {
                    if (!store.put(indexTx.xodusTx, cursor.key().toKey(), marks.getSignature(value).toEntry())) {
                        return false
                    }

                    /* Data is flushed every once in a while. */
                    if ((counter ++) % 1_000_000 == 0) {
                        if (!indexTx.xodusTx.flush()) {
                            return false
                        }
                    }
                }
            }
        }

        /* Update stored VAFMarks. */
        return true
    }
}