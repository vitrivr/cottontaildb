package org.vitrivr.cottontail.dbms.entity

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.ByteArraySizedInputStream
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import java.io.ByteArrayInputStream

/**
 * A [EntityMetadata] in the Cottontail DB [Catalogue]. Used to store metadata about [Entity]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class EntityMetadata(val created: Long, val columns: List<String>, val indexes: List<String>) {

    companion object {
        /** Name of the Xodus [Store] used to store [EntityMetadata]. */
        private const val CATALOGUE_ENTITY_STORE_NAME: String = "org.vitrivr.cottontail.entity"

        /**
         * Initializes the Xodus [Store] used to store [DefaultEntity] information in Cottontail DB.
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        fun init(catalogue: DefaultCatalogue, transaction: Transaction) {
            catalogue.transactionManager.environment.openStore(CATALOGUE_ENTITY_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create entity catalogue.")
        }

        /**
         * Returns the Xodus [Store] used to store [EntityMetadata].
         *
         * @param catalogue [DefaultCatalogue] to access [Store] for.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [Store]
         */
        fun store(catalogue: DefaultCatalogue, transaction: Transaction): Store =
            catalogue.transactionManager.environment.openStore(CATALOGUE_ENTITY_STORE_NAME, StoreConfig.USE_EXISTING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for entity catalogue.")

        /**
         * De-serializes a [EntityMetadata] from the given [ByteArrayInputStream].
         */
        fun fromEntry(entry: ByteIterable): EntityMetadata {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            val created = LongBinding.readCompressed(stream)
            val columns = (0 until IntegerBinding.readCompressed(stream)).map {
                StringBinding.BINDING.readObject(stream)
            }
            val indexes = (0 until IntegerBinding.readCompressed(stream)).map {
                StringBinding.BINDING.readObject(stream)
            }
            return EntityMetadata(created, columns, indexes)
        }

        /**
         * Serializes a [EntityMetadata] to the given [ByteIterable].
         *
         * @param entry [EntityMetadata] to serialize.
         * @return [ByteIterable]
         */
        fun toEntry(entry: EntityMetadata): ByteIterable {
            val output = LightOutputStream()
            LongBinding.writeCompressed(output,  entry.created)
            IntegerBinding.writeCompressed(output,entry.columns.size)
            for (columnName in entry.columns) {
                StringBinding.BINDING.writeObject(output, columnName)
            }

            /* Write all indexes. */
            IntegerBinding.writeCompressed(output,entry.indexes.size)
            for (indexName in entry.indexes) {
                StringBinding.BINDING.writeObject(output, indexName)
            }
            return output.asArrayByteIterable()
        }
    }
}