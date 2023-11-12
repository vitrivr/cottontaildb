package org.vitrivr.cottontail.dbms.catalogue

import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.FunctionRegistry
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.events.SchemaEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.functions.initialize
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.index.cache.InMemoryIndexCache
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.dbms.schema.SchemaMetadata
import java.nio.file.Path
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * The default [Catalogue] implementation based on JetBrains Xodus.
 *
 * @see Catalogue
 * @see CatalogueTx
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DefaultCatalogue(private val environment: Environment) : Catalogue {
    /**
     * Companion object to [DefaultCatalogue]
     */
    companion object {
        /** Prefix used for actual column stores. */
        internal const val METADATA_STORE_PREFIX: String = "org.vitrivr.cottontail.metadata"

        /** Prefix used for actual column stores. */
        internal const val SCHEMA_METADATA_STORE_PREFIX: String = "org.vitrivr.cottontail.schemas"

        /** Prefix used for actual column stores. */
        internal const val ENTITY_METADATA_STORE_PREFIX: String = "org.vitrivr.cottontail.entities"

        /** Prefix used for actual column stores. */
        internal const val SEQUENCE_METADATA_STORE_PREFIX: String = "org.vitrivr.cottontail.sequences"

        /** Prefix used for actual column stores. */
        internal const val COLUMN_METADATA_STORE_NAME: String = "org.vitrivr.cottontail.columns"

        /** Prefix used for actual index stores. */
        internal const val INDEX_METADATA_STORE_NAME: String = "org.vitrivr.cottontail.indexes"

        /** Prefix used for actual index stores. */
        internal const val INDEX_STRUCT_STORE_NAME: String = "org.vitrivr.cottontail.indexes.structs"
    }

    /** Constant name of the [DefaultCatalogue] object. */
    override val name: Name.RootName
        get() = Name.RootName

    /** The [DBOVersion] of this [DefaultCatalogue]. */
    override val version: DBOVersion
        get() = DBOVersion.V4_0

    /** Constant parent [DBO], which is the [DefaultCatalogue] itself. */
    override val parent: Catalogue
        get() = this

    /** Constant parent [DBO], which is null in case of the [DefaultCatalogue]. */
    override val catalogue: Catalogue
        get() = this

    /** The [FunctionRegistry] exposed by this [Catalogue]. */
    override val functions: FunctionRegistry = FunctionRegistry()

    /** An internal, in-memory cache for frequently used index structures. This is highly experimental! */
    val cache = InMemoryIndexCache()

    init {
        /* Check if catalogue has been initialized and initialize if needed. */
        val tx = this.environment.beginExclusiveTransaction()
        try {
            val metadataStore = this.environment.openStore(METADATA_STORE_PREFIX, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, tx)
            if (this.environment.getAllStoreNames(tx).size < 7) {
                /* Initialize database metadata. */
                metadataStore.putRight(tx, StringBinding.stringToEntry("version"), StringBinding.stringToEntry(this.name.toString()))

                /* Initialize necessary stores. */
                this.environment.openStore(SCHEMA_METADATA_STORE_PREFIX, StoreConfig.WITHOUT_DUPLICATES, tx, true)
                this.environment.openStore(ENTITY_METADATA_STORE_PREFIX, StoreConfig.WITHOUT_DUPLICATES, tx, true)
                this.environment.openStore(SEQUENCE_METADATA_STORE_PREFIX, StoreConfig.WITHOUT_DUPLICATES, tx, true)
            }

            /* Check database version. */
            val version = metadataStore.get(tx, StringBinding.stringToEntry("version"))?.let { DBOVersion.valueOf(StringBinding.entryToString(it)) } ?: DBOVersion.UNDEFINED
            if (version != this.version) {
                throw DatabaseException.VersionMismatchException(this.version, version)
            }

            /** Commit transaction. */
            tx.commit()
        } catch (e: Throwable) {
            tx.abort()
            throw e
        }

        /* Initialize function registry. */
        this.functions.initialize()
    }

    /**
     * Creates and returns a new [DefaultCatalogue.Tx] for the given [Transaction].
     *
     * @param transaction The [Transaction] to create the [DefaultCatalogue.Tx] for.
     * @return New [DefaultCatalogue.Tx]
     */
    override fun newTx(transaction: Transaction): CatalogueTx = Tx(transaction)

    /**
     * Compares this [DefaultCatalogue] to another object.
     */
    override fun equals(other: Any?): Boolean {
        if (other !is DefaultCatalogue) return false
        if (this.environment != other.environment) return false
        return true
    }

    /**
     * Hash code for this [DefaultCatalogue].
     */
    override fun hashCode(): Int = this.environment.hashCode()

    /**
     * A [Tx] that affects this [DefaultCatalogue].
     */
    inner class Tx(override val transaction: Transaction): CatalogueTx, org.vitrivr.cottontail.dbms.general.Tx.Commitable {

        /** Reference to the [DefaultCatalogue] this [CatalogueTx] belongs to. */
        override val dbo: DefaultCatalogue
            get() = this@DefaultCatalogue

        /** The Xodus [jetbrains.exodus.env.Transaction] used by this [Tx]. */
        internal val xodusTx: jetbrains.exodus.env.Transaction = this@DefaultCatalogue.environment.beginTransaction()

        /** A [ReentrantLock] that synchronises access to this [Tx]'s methods. */
        private val txLatch = ReentrantLock()

        /**
         * Returns a list of [Name.SchemaName] held by this [DefaultCatalogue].
         *
         * @return [List] of all [Name.SchemaName].
         */
        override fun listSchemas(): List<Name.SchemaName> = this.txLatch.withLock {
            val store = this.xodusTx.environment.openStore(SCHEMA_METADATA_STORE_PREFIX, StoreConfig.USE_EXISTING, this.xodusTx)
            val list = mutableListOf<Name.SchemaName>()
            store.openCursor(this.xodusTx).use { cursor ->
                while (cursor.next) {
                    list.add(NameBinding.Schema.fromEntry(cursor.key))
                }
            }
            return list
        }

        /**
         * Returns the [Schema] for the given [Name.SchemaName].
         *
         * @param name [Name.SchemaName] to obtain the [Schema] for.
         */
        override fun schemaForName(name: Name.SchemaName): Schema = this.txLatch.withLock {
            val store = this.xodusTx.environment.openStore(SCHEMA_METADATA_STORE_PREFIX, StoreConfig.USE_EXISTING, this.xodusTx)
            if (store.get(this.xodusTx, NameBinding.Schema.toEntry(name)) == null) {
                throw DatabaseException.SchemaDoesNotExistException(name)
            }
            return DefaultSchema(name, this@DefaultCatalogue, this@DefaultCatalogue.environment)
        }

        /**
         * Creates a new, empty [Schema] with the given [Name.SchemaName] and [Path]
         *
         * @param name The [Name.SchemaName] of the new [Schema].
         */
        override fun createSchema(name: Name.SchemaName): Schema = this.txLatch.withLock {
            val store = this.xodusTx.environment.openStore(SCHEMA_METADATA_STORE_PREFIX, StoreConfig.USE_EXISTING, this.xodusTx)
            val metadata = SchemaMetadata(System.currentTimeMillis(), System.currentTimeMillis())
            if (!store.add(this.xodusTx, NameBinding.Schema.toEntry(name), SchemaMetadata.toEntry(metadata))) {
                throw DatabaseException.SchemaAlreadyExistsException(name) /* Schema already exists. */
            }

            /* Create Event and notify observers */
            val event = SchemaEvent.Create(name)
            this.transaction.signalEvent(event)

            /* Return schema. */
            return DefaultSchema(name, this@DefaultCatalogue, this@DefaultCatalogue.environment)
        }

        /**
         * Commits the local Xodus [jetbrains.exodus.env.Transaction] and persists all changes.
         */
        override fun commit() {
            this.xodusTx.commit()
        }

        /**
         * Aborts the local Xodus [jetbrains.exodus.env.Transaction] and persists all changes.
         */
        override fun rollback() {
            this.xodusTx.abort()
        }
    }
}