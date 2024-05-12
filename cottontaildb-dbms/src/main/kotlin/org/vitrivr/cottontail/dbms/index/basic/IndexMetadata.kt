package org.vitrivr.cottontail.dbms.index.basic

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.ByteArraySizedInputStream
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

/**
 * A metadata about [Index]s as stored in the Cottontail DB catalogue
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
data class IndexMetadata(val type: IndexType, val state: IndexState, val columns: List<String>, val config: IndexConfig<*>) {
    companion object {
        /** Name of the [IndexMetadata] store in the Cottontail DB catalogue. */
        private const val CATALOGUE_INDEX_STORE_NAME: String = "org.vitrivr.cottontail.indexes"

        /**
         * Opens the Xodus [Store] used to store [IndexMetadata] entries in Cottontail DB.
         *
         * @param transaction The [Transaction] to use.
         * @return Xodus [Store] for [IndexMetadata] entries.
         */
        fun store(transaction: Transaction) = transaction.environment.openStore(CATALOGUE_INDEX_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction)

        /**
         * De-serializes a [IndexMetadata] from the given [ByteIterable].
         *
         * @param entry The [ByteIterable] entry.
         * @return [IndexMetadata]
         */
        fun fromEntry(entry: ByteIterable): IndexMetadata {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            val type = IndexType.entries[IntegerBinding.readCompressed(stream)]
            val state = IndexState.entries[IntegerBinding.readCompressed(stream)]
            val columns = (0 until IntegerBinding.readCompressed(stream)).map {
                StringBinding.BINDING.readObject(stream)
            }
            val config = type.descriptor.configBinding().readObject(stream) as IndexConfig<*>
            return IndexMetadata(type, state, columns, config)
        }

        /**
         * Serializes a [IndexMetadata] to the given [ByteIterable].
         *
         * @param entry [IndexMetadata] to serialize.
         * @return [ByteIterable]
         */
        fun toEntry(entry: IndexMetadata): ByteIterable {
            val output = LightOutputStream()
            IntegerBinding.writeCompressed(output, entry.type.ordinal)
            IntegerBinding.writeCompressed(output, entry.state.ordinal)

            /* Write all columns. */
            IntegerBinding.writeCompressed(output,entry.columns.size)
            for (columnName in entry.columns) {
                StringBinding.BINDING.writeObject(output, columnName)
            }

            /* Write index configuration. */
            entry.type.descriptor.configBinding().writeObject(output, entry.config)
            return output.asArrayByteIterable()
        }


        /**
         * Internal function used to obtain the name of the Xodus store for the given [Name.IndexName].
         *
         * @return Store name.
         */
        internal fun Name.IndexName.storeName(): String = "${CATALOGUE_INDEX_STORE_NAME}.${this.schema}.${this.entity}.${this.index}"
    }
}