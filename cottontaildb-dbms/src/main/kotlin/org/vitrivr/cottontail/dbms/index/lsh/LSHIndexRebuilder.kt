package org.vitrivr.cottontail.dbms.index.lsh

import jetbrains.exodus.env.Store
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder

/**
 * An [AbstractIndexRebuilder] for the [LSHIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LSHIndexRebuilder(index: LSHIndex, context: TransactionContext): AbstractIndexRebuilder<LSHIndex>(index, context) {

    /**
     * Performs the index rebuilding process for this [LSHIndexRebuilder].
     *
     * @param dataStore The [TransactionContext] to execute the rebuild process in.
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(dataStore: Store): Boolean {
        /* Read basic index properties. */
        val entry = IndexCatalogueEntry.read(this.index.name, this.index.catalogue, context.xodusTx)
            ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index  ${this.index.name}: Could not read catalogue entry for index.")
        val config = entry.config as LSHIndexConfig
        val column = entry.columns[0]

        /* Tx objects required for index rebuilding. */
        val entityTx = context.getTx(this.index.parent) as EntityTx
        val columnTx = context.getTx(entityTx.columnForName(column)) as ColumnTx<*>

        /* Generate a new LSHSignature for each entry in the entity and adds it to the index. */
        val generator = config.generator(columnTx.columnDef.type.logicalSize)
        val cursor = columnTx.cursor()
        val wrappedStore = LSHDataStore(dataStore)
        while (cursor.moveNext()) {
            val tupleId = cursor.key()
            val value = cursor.next()
            if (value is VectorValue<*>) {
                wrappedStore.addMapping(context.xodusTx, generator.generate(value), tupleId)
            }
        }

        /* Close cursor. */
        cursor.close()

        return true
    }
}