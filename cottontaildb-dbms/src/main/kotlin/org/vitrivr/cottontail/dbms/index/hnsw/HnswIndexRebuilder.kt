package org.vitrivr.cottontail.dbms.index.hnsw

import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

class HnswIndexRebuilder(index: HnswIndex, context: QueryContext): AbstractIndexRebuilder<HnswIndex>(index, context) {


    override fun rebuildInternal(): Boolean {

        /* Read basic index properties. */
        val entry = IndexCatalogueEntry.read(this.index.name, this.index.catalogue, this.context.txn.xodusTx)
            ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index  ${this.index.name}: Could not read catalogue entry for index.")
        val column = entry.columns[0]


        /* Obtain Tx for parent entity. */
        val entityTx = this.index.parent.newTx(this.context)
        val columnTx = entityTx.columnForName(column).newTx(this.context)

        columnTx.cursor().use { cursor ->
            while (cursor.moveNext()) {
                val value = cursor.value()
                if (value is VectorValue<*>) {
                    index.inMemoryIndex?.add(cursor.key() to value)
                }
            }
        }

        return true
    }


}