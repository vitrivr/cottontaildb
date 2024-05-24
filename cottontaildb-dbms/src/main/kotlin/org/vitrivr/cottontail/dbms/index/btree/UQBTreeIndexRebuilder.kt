package org.vitrivr.cottontail.dbms.index.btree

import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.serialization.buffer.ValueSerializer
import org.vitrivr.cottontail.storage.serializers.ValueBinding

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
        val column = indexTx.columns[0]

        /* Tx objects required for index rebuilding. */
        val binding: ValueBinding<Value> = ValueBinding(ValueSerializer.serializer(column.type)) as ValueBinding<Value>
        val dataStore = this.tryClearAndOpenStore(indexTx) ?: return false

        /* Iterate over entity and update index with entries. */
        indexTx.parent.cursor(indexTx.columns).use { cursor ->
            while (cursor.moveNext()) {
                val key = cursor.value()[column]
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