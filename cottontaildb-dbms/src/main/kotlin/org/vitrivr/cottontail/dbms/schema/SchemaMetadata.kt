package org.vitrivr.cottontail.dbms.schema

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityMetadata
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import java.io.ByteArrayInputStream


/**
 * A [SchemaMetadata] in the Cottontail DB [Catalogue]. Used to store metadata about [Schema]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
data class SchemaMetadata(val created: Long, val updated: Long) {
    companion object {

        /** Name of the Xodus [Store] used to store [SchemaMetadata]. */
        private const val CATALOGUE_SCHEMA_STORE_NAME: String = "org.vitrivr.cottontail.schema"

        /**
         * Returns the Xodus [Store] used to store [SchemaMetadata] information in Cottontail DB.
         *
         * @param transaction The Xodus [Transaction] to use.
         * @return [Store]
         */
        fun store(transaction: Transaction): Store = transaction.environment.openStore(CATALOGUE_SCHEMA_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction)

        /**
         * De-serializes a [SchemaMetadata] from the given [ByteIterable].
         *
         * @param entry [ByteIterable] to deserialize.
         * @return [SchemaMetadata]
         */
        fun fromEntry(entry: ByteIterable): SchemaMetadata {
            val stream = ByteArrayInputStream(entry.bytesUnsafe, 0, entry.length)
            return SchemaMetadata(LongBinding.readCompressed(stream), LongBinding.readCompressed(stream))
        }

        /**
         * Serializes a [SchemaMetadata] to the given [ByteIterable].
         *
         * @param entry [SchemaMetadata] to serialize.
         * @return [ByteIterable]
         */
        fun toEntry(entry: SchemaMetadata): ByteIterable {
            val output = LightOutputStream()
            LongBinding.writeCompressed(output, entry.created)
            LongBinding.writeCompressed(output, entry.updated)
            return output.asArrayByteIterable()
        }
    }
}