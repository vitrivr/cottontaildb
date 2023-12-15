package org.vitrivr.cottontail.dbms.catalogue

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.FunctionRegistry
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.entries.MetadataEntry
import org.vitrivr.cottontail.dbms.catalogue.entries.MetadataEntry.Companion.METADATA_ENTRY_DB_VERSION
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.EntityMetadata
import org.vitrivr.cottontail.dbms.events.SchemaEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.ExecutionManager
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.functions.initialize
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata
import org.vitrivr.cottontail.dbms.index.cache.InMemoryIndexCache
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.dbms.schema.SchemaMetadata
import org.vitrivr.cottontail.dbms.sequence.DefaultSequence
import org.vitrivr.cottontail.dbms.statistics.StatisticsManager
import org.vitrivr.cottontail.dbms.statistics.index.IndexStatisticsManager
import java.nio.file.Files
import java.nio.file.Path
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
class DefaultCatalogue(override val config: Config, executor: ExecutionManager) : Catalogue {
    /**
     * Companion object to [DefaultCatalogue]
     */
    companion object {
        /** [Logger] instance used by [DefaultCatalogue]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(DefaultCatalogue::class.java)

        /** Prefix used for actual column stores. */
        internal const val ENTITY_STORE_PREFIX: String = "org.vitrivr.cottontail.store.entity"

        /** Prefix used for actual column stores. */
        internal const val COLUMN_STORE_PREFIX: String = "org.vitrivr.cottontail.store.column"

        /** Prefix used for actual index stores. */
        internal const val INDEX_STORE_PREFIX: String = "org.vitrivr.cottontail.store.index"
    }

    /** Root to Cottontail DB root folder. */
    val path: Path = this.config.root

    /** Constant name of the [DefaultCatalogue] object. */
    override val name: Name.RootName
        get() = Name.RootName

    /** The [DBOVersion] of this [DefaultCatalogue]. */
    override val version: DBOVersion
        get() = DBOVersion.V3_0

    /** Constant parent [DBO], which is null in case of the [DefaultCatalogue]. */
    override val parent: DBO? = null

    /** Constant parent [DBO], which is null in case of the [DefaultCatalogue]. */
    override val catalogue: Catalogue
        get() = this

    /** The [FunctionRegistry] exposed by this [Catalogue]. */
    override val functions: FunctionRegistry = FunctionRegistry()

    /** The [IndexStatisticsManager] used by this [DefaultCatalogue]. */
    val indexStatistics: IndexStatisticsManager

    /** The [TransactionManager] instanced used and exposed by this [DefaultCatalogue]. */
    override val transactionManager = TransactionManager(executor, this.config)

    /** The [StatisticsManager] instanced used and exposed by this [DefaultCatalogue]. */
    override val statisticsManager = StatisticsManager(this, this.transactionManager)

    /** An internal, in-memory cache for frequently used index structures. This is highly experimental! */
    val cache = InMemoryIndexCache()

    init {
        this.transactionManager.register(this.statisticsManager)
    }

    init {
        /* Check if catalogue has been initialized and initialize if needed. */
        val tx = this.transactionManager.environment.beginExclusiveTransaction()
        try {
            if (this.transactionManager.environment.getAllStoreNames(tx).size < 7) {
                /* Initialize database metadata. */
                MetadataEntry.init(this, tx)
                MetadataEntry.write(MetadataEntry(METADATA_ENTRY_DB_VERSION, this.version.toString()), this, tx)

                /* Initialize necessary stores. */
                SchemaMetadata.init(this, tx)
                EntityMetadata.init(this, tx)
                ColumnMetadata.init(this, tx)
                IndexMetadata.init(this, tx)
                DefaultSequence.init(this, tx)
                IndexStructCatalogueEntry.init(this, tx)
            }

            /** Open the IndexStatisticsManager. */
            this.indexStatistics = IndexStatisticsManager(this.transactionManager.environment, tx)

            /* Check database version. */
            val version = MetadataEntry.read(METADATA_ENTRY_DB_VERSION, this, tx)?.let { it -> DBOVersion.valueOf(it.value) } ?: DBOVersion.UNDEFINED
            if (version != this.version) {
                throw DatabaseException.VersionMismatchException(this.version, version)
            }

            /** Commit transaction. */
            tx.commit()
        } catch (e: Throwable) {
            tx.abort()
            throw e
        }

        /* Tries to clean up the temporary environment. */
        if (!Files.exists(this.config.temporaryDataFolder())) {
            Files.createDirectories(this.config.temporaryDataFolder())
        } else {
            Files.walk(this.config.temporaryDataFolder()).sorted(Comparator.reverseOrder()).forEach {
                try {
                    Files.delete(it)
                } catch (e: Throwable) {
                    LOGGER.warn("Failed to clean-up temporary data at $it.")
                }
            }
        }

        /* Initialize function registry. */
        this.functions.initialize()
    }

    /**
     * Creates and returns a new [DefaultCatalogue.Tx] for the given [QueryContext].
     *
     * @param context The [QueryContext] to create the [DefaultCatalogue.Tx] for.
     * @return New [DefaultCatalogue.Tx]
     */
    override fun newTx(context: QueryContext): CatalogueTx = context.txn.getCachedTxForDBO(this) ?: Tx(context)

    /**
     * Closes the [DefaultCatalogue] and all objects contained within.
     */
    override fun close() {
        try {
            this.indexStatistics.persist() /* Persist all index statistics. */
        } finally {
            this.transactionManager.shutdown()
        }
    }

    override fun equals(other: Any?): Boolean {
        if (other !is DefaultCatalogue) return false
        if (this.path != other.path) return false
        return true
    }

    override fun hashCode(): Int = this.path.hashCode()

    /**
     * A [Tx] that affects this [DefaultCatalogue].
     */
    inner class Tx(context: QueryContext) : AbstractTx(context), CatalogueTx {

        init {
            /* Cache this Tx for future use. */
            context.txn.cacheTx(this)
        }

        /** Reference to the [DefaultCatalogue] this [CatalogueTx] belongs to. */
        override val dbo: DefaultCatalogue
            get() = this@DefaultCatalogue

        /**
         * Returns a list of [Name.SchemaName] held by this [DefaultCatalogue].
         *
         * @return [List] of all [Name.SchemaName].
         */
        override fun listSchemas(): List<Name.SchemaName> = this.txLatch.withLock {
            val store = SchemaMetadata.store(this.dbo, this.context.txn.xodusTx)
            val list = mutableListOf<Name.SchemaName>()
            store.openCursor(this.context.txn.xodusTx).use { cursor ->
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
            val store = SchemaMetadata.store(this.dbo, this.context.txn.xodusTx)
            if (store.get(this.context.txn.xodusTx, NameBinding.Schema.toEntry(name)) == null) {
                throw DatabaseException.SchemaDoesNotExistException(name)
            }
            return DefaultSchema(name, this@DefaultCatalogue)
        }

        /**
         * Creates a new, empty [Schema] with the given [Name.SchemaName] and [Path]
         *
         * @param name The [Name.SchemaName] of the new [Schema].
         */
        override fun createSchema(name: Name.SchemaName): Schema = this.txLatch.withLock {
            val store = SchemaMetadata.store(this@DefaultCatalogue, this.context.txn.xodusTx)
            val metadata = SchemaMetadata(System.currentTimeMillis(), System.currentTimeMillis())
            if (!store.add(this.context.txn.xodusTx, NameBinding.Schema.toEntry(name), SchemaMetadata.toEntry(metadata))) {
                throw DatabaseException.SchemaAlreadyExistsException(name) /* Schema already exists. */
            }

            /* Create Event and notify observers */
            val event = SchemaEvent.Create(name)
            this.context.txn.signalEvent(event)

            /* Return schema. */
            return DefaultSchema(name, this@DefaultCatalogue)
        }

        /**
         * Drops an existing [Schema] with the given [Name.SchemaName].
         *
         * @param name The [Name.SchemaName] of the [Schema] to be dropped.
         */
        override fun dropSchema(name: Name.SchemaName) = this.txLatch.withLock {
            /* Check if schema exists! */
            val store = SchemaMetadata.store(this@DefaultCatalogue, this.context.txn.xodusTx)
            if (!store.delete(this.context.txn.xodusTx, NameBinding.Schema.toEntry(name))) {
                throw DatabaseException.SchemaDoesNotExistException(name)
            }

            /* Obtain schema Tx and drop all entities contained in schema. */
            val schemaTx = DefaultSchema(name, this@DefaultCatalogue).newTx(this.context)
            schemaTx.listEntities().forEach { schemaTx.dropEntity(it) }

            /* Create Event and notify observers */
            val event = SchemaEvent.Drop(name)
            this.context.txn.signalEvent(event)
        }
    }
}