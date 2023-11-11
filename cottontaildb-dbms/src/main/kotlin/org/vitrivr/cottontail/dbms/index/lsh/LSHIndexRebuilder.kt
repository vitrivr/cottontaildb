package org.vitrivr.cottontail.dbms.index.lsh

import org.vitrivr.cottontail.core.values.VectorValue
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.index.basic.DefaultIndex
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [AbstractIndexRebuilder] for the [LSHIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LSHIndexRebuilder(index: LSHIndex, context: QueryContext): AbstractIndexRebuilder<LSHIndex>(index, context) {

    override fun rebuildInternal(indexTx: DefaultIndex.Tx): Boolean {
        require(indexTx is LSHIndex.Tx) { "LSHIndexRebuilder only supports LSHIndex.Tx implementations." }

        /* Obtain Tx for parent entity. */
        val columnTx = indexTx.transaction.columnTx(indexTx.columns[0].name, AccessMode.READ)

        /* Generate a new LSHSignature for each entry in the entity and adds it to the index. */
        val generator = (indexTx.config as LSHIndexConfig).generator(columnTx.columnDef.type.logicalSize)
        columnTx.cursor().use { cursor ->
            while (cursor.moveNext()) {
                val tupleId = cursor.key()
                val value = cursor.next()
                if (value is VectorValue<*>) {
                    indexTx.addMapping(generator.generate(value), tupleId)
                }
            }
        }

        return true
    }
}