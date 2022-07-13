package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.xodus.XodusBinding

/**
 * An [AbstractIndexRebuilder] for the [UQBTreeIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class UQBTreeIndexRebuilder(index: UQBTreeIndex, context: TransactionContext): AbstractIndexRebuilder<UQBTreeIndex>(index, context) {
    @Suppress("UNCHECKED_CAST")
    override fun rebuildInternal(): Boolean {
        /* Read basic index properties. */
        val entry = IndexCatalogueEntry.read(this.index.name, this.index.catalogue, this.context.xodusTx)
            ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index  ${this.index.name} (${this.index.type}). Could not read catalogue entry for index.")
        val column = entry.columns[0]

        /* Tx objects required for index rebuilding. */
        val entityTx = context.getTx(this.index.parent) as EntityTx
        val columnTx = context.getTx(entityTx.columnForName(column)) as ColumnTx<*>
        val binding: XodusBinding<Value> = ValueSerializerFactory.xodus(columnTx.columnDef.type, columnTx.columnDef.nullable) as XodusBinding<Value>
        val dataStore = this.tryClearAndOpenStore() ?: return false

        /* Iterate over entity and update index with entries. */
        columnTx.cursor().use { cursor ->
            while (cursor.moveNext()) {
                val keyRaw = binding.valueToEntry(cursor.value())
                val tupleIdRaw = LongBinding.longToCompressedEntry(cursor.key())
                if (!dataStore.add(this.context.xodusTx, keyRaw, tupleIdRaw)) {
                    return false
                }
            }
        }

        /* Close cursor. */
        return true
    }
}