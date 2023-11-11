package org.vitrivr.cottontail.dbms.entity

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.ByteArraySizedInputStream
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import java.io.ByteArrayInputStream
import java.util.*

/**
 * A [EntityMetadata] in the Cottontail DB [Catalogue]. Used to store metadata about [Entity]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class EntityMetadata(val handle: UUID = UUID.randomUUID(), val created: Long, val modified: Long) {

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
            catalogue.environment.openStore(CATALOGUE_ENTITY_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
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
            catalogue.environment.openStore(CATALOGUE_ENTITY_STORE_NAME, StoreConfig.USE_EXISTING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for entity catalogue.")

        /**
         * De-serializes a [EntityMetadata] from the given [ByteArrayInputStream].
         */
        fun fromEntry(entry: ByteIterable): EntityMetadata {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            val handle = UUID(LongBinding.readCompressed(stream), LongBinding.readCompressed(stream))
            val created = LongBinding.readCompressed(stream)
            val modified = LongBinding.readCompressed(stream)
            return EntityMetadata(handle, created, modified)
        }

        /**
         * Serializes a [EntityMetadata] to the given [ByteIterable].
         *
         * @param entry [EntityMetadata] to serialize.
         * @return [ByteIterable]
         */
        fun toEntry(entry: EntityMetadata): ByteIterable {
            val output = LightOutputStream()
            LongBinding.writeCompressed(output, entry.handle.mostSignificantBits)
            LongBinding.writeCompressed(output, entry.handle.leastSignificantBits)
            LongBinding.writeCompressed(output, entry.created)
            LongBinding.writeCompressed(output, entry.modified)
            return output.asArrayByteIterable()
        }
    }
}