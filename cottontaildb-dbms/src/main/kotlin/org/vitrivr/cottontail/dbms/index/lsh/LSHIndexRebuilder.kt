package org.vitrivr.cottontail.dbms.index.lsh

import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
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
        val config = indexTx.config as LSHIndexConfig
        val column = indexTx.columns[0]

        /* Tx objects required for index rebuilding. */
        val dataStore = this.tryClearAndOpenStore(indexTx) ?: return false

        /* Generate a new LSHSignature for each entry in the entity and adds it to the index. */
        val generator = config.generator(column.type.logicalSize)
        indexTx.parent.cursor(indexTx.columns).use { cursor ->
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