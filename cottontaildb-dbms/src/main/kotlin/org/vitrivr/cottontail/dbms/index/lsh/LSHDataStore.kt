package org.vitrivr.cottontail.dbms.index.lsh

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.index.lsh.signature.LSHSignature

/**
 * This is an abstraction over a Xodus [Store] that provides certain primitives required by the [LSHIndex].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@JvmInline
value class LSHDataStore internal constructor(val store: Store) {
    companion object {
        fun open(indexTx: LSHIndex.Tx): LSHDataStore {
            val store = indexTx.xodusTx.environment.openStore(indexTx.dbo.name.storeName(), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, indexTx.xodusTx)
            return LSHDataStore(store)
        }
    }

    /**
     * Adds a mapping from the bucket [IntArray] to the given [TupleId].
     *
     * @param transaction The Xodus [Transaction] to remove the mapping with.
     * @param signature The [IntArray] signature key to add a mapping for.
     * @param tupleId The [TupleId] to add to the mapping
     *
     * This is an internal function and can be used safely with values o
     */
    fun addMapping(transaction: Transaction, signature: LSHSignature, tupleId: TupleId): Boolean {
        val signatureRaw = LSHSignature.Binding.objectToEntry(signature)
        val tupleIdRaw = tupleId.toKey()
        return if (this.store.exists(transaction, signatureRaw, tupleIdRaw)) {
            this.store.put(transaction, signatureRaw, tupleIdRaw)
        } else {
            false
        }
    }

    /**
     * Removes a mapping from the given [IntArray] signature to the given [TupleId].
     *
     * @param transaction The Xodus [Transaction] to remove the mapping with.
     * @param signature The [IntArray] signature key to remove a mapping for.
     * @param tupleId The [TupleId] to remove.
     *
     * This is an internal function and can be used safely with values o
     */
    fun removeMapping(transaction: Transaction, signature: LSHSignature, tupleId: TupleId): Boolean {
        val signatureRaw = LSHSignature.Binding.objectToEntry(signature)
        val valueRaw = tupleId.toKey()
        val cursor = this.store.openCursor(transaction)
        return cursor.getSearchBoth(signatureRaw, valueRaw) && cursor.deleteCurrent()
    }
}