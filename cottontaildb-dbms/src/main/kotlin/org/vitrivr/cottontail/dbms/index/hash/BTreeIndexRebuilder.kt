package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer

/**
 * An [AbstractIndexRebuilder] for the [BTreeIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BTreeIndexRebuilder(index: BTreeIndex, context: QueryContext): AbstractIndexRebuilder<BTreeIndex>(index, context) {
    @Suppress("UNCHECKED_CAST")
    override fun rebuildInternal(): Boolean {
        /* Read basic index properties. */
        val indexMetadataStore = IndexMetadata.store(this.index.catalogue, context.txn.xodusTx)
        val indexEntryRaw = indexMetadataStore.get(context.txn.xodusTx, NameBinding.Index.toEntry(this@BTreeIndexRebuilder.index.name)) ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index ${this@BTreeIndexRebuilder.index.name}: Could not read catalogue entry for index.")
        val indexEntry = IndexMetadata.fromEntry(indexEntryRaw)
        val column = this.index.name.entity().column(indexEntry.columns[0])

        /* Tx objects required for index rebuilding. */
        val entityTx = this.index.parent.newTx(this.context)
        val columnTx = entityTx.columnForName(column).newTx(this.context)
        val binding: ValueSerializer<Value> = SerializerFactory.value(columnTx.columnDef.type) as ValueSerializer<Value>
        val dataStore = this.tryClearAndOpenStore() ?: return false

        /* Iterate over entity and update index with entries. */
        columnTx.cursor().use { cursor ->
            while (cursor.moveNext()) {
                val keyRaw = binding.toEntry(cursor.value())
                val tupleIdRaw = LongBinding.longToCompressedEntry(cursor.key())
                if (!dataStore.put(this.context.txn.xodusTx, keyRaw, tupleIdRaw)) {
                    return false
                }
            }
        }

        /* Close cursor. */
        return true
    }
}