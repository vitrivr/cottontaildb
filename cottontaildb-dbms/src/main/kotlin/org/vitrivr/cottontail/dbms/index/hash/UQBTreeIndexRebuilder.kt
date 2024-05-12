package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer

/**
 * An [AbstractIndexRebuilder] for the [UQBTreeIndex].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Suppress("UNCHECKED_CAST")
class UQBTreeIndexRebuilder(index: UQBTreeIndex, context: QueryContext): AbstractIndexRebuilder<UQBTreeIndex>(index, context) {
    override fun rebuildInternal(indexTx: AbstractIndex.Tx): Boolean {
        require(indexTx is UQBTreeIndex.Tx) { "UQBTreeIndexRebuilder can only be accessed with a UQBTreeIndex.Tx!" }

        /* Read basic index properties. */
        val indexMetadataStore = IndexMetadata.store(indexTx.parent.parent.parent.xodusTx)
        val indexEntryRaw = indexMetadataStore.get(indexTx.parent.parent.parent.xodusTx, NameBinding.Index.toEntry(this@UQBTreeIndexRebuilder.index.name)) ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index ${this@UQBTreeIndexRebuilder.index.name}: Could not read catalogue entry for index.")
        val indexEntry = IndexMetadata.fromEntry(indexEntryRaw)
        val column = this.index.name.entity().column(indexEntry.columns[0])

        /* Tx objects required for index rebuilding. */
        val columnTx = indexTx.parent.columnForName(column).newTx(indexTx.parent)
        val binding: ValueSerializer<Value> = SerializerFactory.value(columnTx.columnDef.type) as ValueSerializer<Value>
        val dataStore = this.tryClearAndOpenStore(indexTx) ?: return false

        /* Iterate over entity and update index with entries. */
        columnTx.cursor().use { cursor ->
            while (cursor.moveNext()) {
                val key = cursor.value()
                if (key != null) {
                    val keyRaw = binding.toEntry(key)
                    val tupleIdRaw = LongBinding.longToCompressedEntry(cursor.key())
                    if (!dataStore.add(indexTx.xodusTx, keyRaw, tupleIdRaw)) {
                        return false
                    }
                }
            }
        }

        /* Close cursor. */
        return true
    }
}