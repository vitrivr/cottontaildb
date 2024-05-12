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
import java.io.ByteArrayInputStream
import java.util.*

/**
 * A [EntityMetadata] in the Cottontail DB [Catalogue]. Used to store metadata about [Entity]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
data class EntityMetadata(val uuid: UUID, val created: Long, val columns: List<String>) {

    companion object {
        /** Name of the Xodus [Store] used to store [EntityMetadata]. */
        private const val CATALOGUE_ENTITY_STORE_NAME: String = "org.vitrivr.cottontail.entity"

        /**
         * Returns the Xodus [Store] used to store [EntityMetadata].
         *
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [Store]
         */
        fun store(transaction: Transaction): Store = transaction.environment.openStore(CATALOGUE_ENTITY_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction)

        /**
         * De-serializes a [EntityMetadata] from the given [ByteArrayInputStream].
         */
        fun fromEntry(entry: ByteIterable): EntityMetadata {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            val created = LongBinding.readCompressed(stream)
            val uuid = UUID(LongBinding.BINDING.readObject(stream), LongBinding.BINDING.readObject(stream))
            val columns = (0 until IntegerBinding.readCompressed(stream)).map {
                StringBinding.BINDING.readObject(stream)
            }
            return EntityMetadata(uuid, created, columns)
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
            LongBinding.BINDING.writeObject(output, entry.uuid.mostSignificantBits)
            LongBinding.BINDING.writeObject(output, entry.uuid.leastSignificantBits)
            IntegerBinding.writeCompressed(output,entry.columns.size)
            for (columnName in entry.columns) {
                StringBinding.BINDING.writeObject(output, columnName)
            }
            return output.asArrayByteIterable()
        }

        /**
         * Internal function used to obtain the name of the Xodus store for the given [Name.EntityName].
         *
         * @return Store name.
         */
        fun Name.EntityName.storeName(): String = "$CATALOGUE_ENTITY_STORE_NAME.${this.schema}.${this.entity}"
    }
}