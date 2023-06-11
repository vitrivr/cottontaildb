package org.vitrivr.cottontail.dbms.index.lsh

import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [AbstractIndexRebuilder] for the [LSHIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LSHIndexRebuilder(index: LSHIndex, context: QueryContext): AbstractIndexRebuilder<LSHIndex>(index, context) {

    /**
     * Performs the index rebuilding process for this [LSHIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(): Boolean {
        /* Read basic index properties. */
        val entry = IndexCatalogueEntry.read(this.index.name, this.index.catalogue, this.context.txn.xodusTx)
            ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index  ${this.index.name}: Could not read catalogue entry for index.")
        val config = entry.config as LSHIndexConfig
        val column = entry.columns[0]

        /* Tx objects required for index rebuilding. */
        val entityTx = this.index.parent.newTx(this.context)
        val columnTx = entityTx.columnForName(column).newTx(this.context)
        val dataStore = this.tryClearAndOpenStore() ?: return false

        /* Generate a new LSHSignature for each entry in the entity and adds it to the index. */
        val generator = config.generator(columnTx.columnDef.type.logicalSize)
        val cursor = columnTx.cursor()
        val wrappedStore = LSHDataStore(dataStore)
        while (cursor.moveNext()) {
            val tupleId = cursor.key()
            val value = cursor.next()
            if (value is VectorValue<*>) {
                wrappedStore.addMapping(this.context.txn.xodusTx, generator.generate(value), tupleId)
            }
        }

        /* Close cursor. */
        cursor.close()

        return true
    }
}