package org.vitrivr.cottontail.dbms.catalogue

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.forEach
import jetbrains.exodus.vfs.ClusteringStrategy
import jetbrains.exodus.vfs.VfsConfig
import jetbrains.exodus.vfs.VirtualFileSystem
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.FunctionRegistry
import org.vitrivr.cottontail.dbms.catalogue.entries.*
import org.vitrivr.cottontail.dbms.catalogue.entries.MetadataEntry.Companion.METADATA_ENTRY_DB_VERSION
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.functions.initialize
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.dbms.schema.Schema
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
class DefaultCatalogue(override val config: Config) : Catalogue {
    /**
     * Companion object to [DefaultCatalogue]
     */
    companion object {
        /** [Logger] instance used by [DefaultCatalogue]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(DefaultCatalogue::class.java)

        /** Prefix used for actual column stores. */
        internal const val ENTITY_STORE_PREFIX: String = "ctt_ent"

        /** Prefix used for actual column stores. */
        internal const val COLUMN_STORE_PREFIX: String = "ctt_col"

        /** Prefix used for actual index stores. */
        internal const val INDEX_STORE_PREFIX: String = "ctt_idx"
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

    /** The main Xodus [Environment] used by Cottontail DB. This is an internal variable and not part of the official interface. */
    internal val environment: Environment = Environments.newInstance(
        this.config.dataFolder().toFile(),
        this.config.xodus.toEnvironmentConfig()
    )

    /** The Xodus [VirtualFileSystem] used by Cottontail DB. This is an internal variable and not part of the official interface. */
    internal val vfs: VirtualFileSystem

    init {
        /* Check if catalogue has been initialized and initialize if needed. */
        val tx = this.environment.beginExclusiveTransaction()
        try {
            if (this.environment.getAllStoreNames(tx).size == 0) {
                /* Initialize database metadata. */
                MetadataEntry.init(this, tx)
                MetadataEntry.write(MetadataEntry(METADATA_ENTRY_DB_VERSION, this.version.toString()), this, tx)

                /* Initialize necessary stores. */
                SchemaCatalogueEntry.init(this, tx)
                EntityCatalogueEntry.init(this, tx)
                SequenceCatalogueEntries.init(this, tx)
                ColumnCatalogueEntry.init(this, tx)
                StatisticsCatalogueEntry.init(this, tx)
                IndexCatalogueEntry.init(this, tx)
                IndexStructCatalogueEntry.init(this, tx)
            }

            /** Initialize virtual file system. */
            val config = VfsConfig()
            config.clusteringStrategy = ClusteringStrategy.QuadraticClusteringStrategy(65536)
            config.clusteringStrategy.maxClusterSize = 65536 * 16
            this.vfs = VirtualFileSystem(this.environment, config, tx)

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

        /* Tries to clean-up the temporary environment. */
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
    override fun newTx(context: QueryContext): CatalogueTx
        = context.txn.getCachedTxForDBO(this) ?: Tx(context)

    /**
     * Closes the [DefaultCatalogue] and all objects contained within.
     */
    override fun close() {
        this.vfs.shutdown()
        this.environment.close()
    }

    override fun equals(other: Any?): Boolean {
        if (other !is DefaultCatalogue) return false
        if (this.path != other.path) return false
        return true
    }

    override fun hashCode(): Int = this.path.hashCode()

    /**
     * A [Tx] that affects this [DefaultCatalogue].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    inner class Tx(context: QueryContext) : AbstractTx(context), CatalogueTx {

        /** Reference to the [DefaultCatalogue] this [CatalogueTx] belongs to. */
        override val dbo: DefaultCatalogue
            get() = this@DefaultCatalogue

        /**
         * Returns a list of [Name.SchemaName] held by this [DefaultCatalogue].
         *
         * @return [List] of all [Name.SchemaName].
         */
        override fun listSchemas(): List<Name.SchemaName> = this.txLatch.withLock {
            val store = SchemaCatalogueEntry.store(this@DefaultCatalogue, this.context.txn.xodusTx)
            val list = mutableListOf<Name.SchemaName>()
            store.openCursor(this.context.txn.xodusTx).forEach {
                val entry = SchemaCatalogueEntry.entryToObject(this.value) as SchemaCatalogueEntry
                list.add(entry.name)
            }
            return list
        }

        /**
         * Returns the [Schema] for the given [Name.SchemaName].
         *
         * @param name [Name.SchemaName] to obtain the [Schema] for.
         */
        override fun schemaForName(name: Name.SchemaName): Schema = this.txLatch.withLock {
            if (!SchemaCatalogueEntry.exists(name, this@DefaultCatalogue, this.context.txn.xodusTx)) {
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
            /* Check if schema exists! */
            if (SchemaCatalogueEntry.exists(name, this@DefaultCatalogue, this.context.txn.xodusTx)) throw DatabaseException.SchemaAlreadyExistsException(name)

            /* Write schema! */
            SchemaCatalogueEntry.write(SchemaCatalogueEntry(name), this@DefaultCatalogue, this.context.txn.xodusTx)
            return DefaultSchema(name, this@DefaultCatalogue)
        }

        /**
         * Drops an existing [Schema] with the given [Name.SchemaName].
         *
         * @param name The [Name.SchemaName] of the [Schema] to be dropped.
         */
        override fun dropSchema(name: Name.SchemaName) = this.txLatch.withLock {
            /* Check if schema exists! */
            if (!SchemaCatalogueEntry.exists(name, this@DefaultCatalogue, this.context.txn.xodusTx)) {
                throw DatabaseException.SchemaDoesNotExistException(name)
            }

            /* Obtain schema Tx and drop all entities contained in schema. */
            val schemaTx = DefaultSchema(name, this@DefaultCatalogue).newTx(this.context)
            schemaTx.listEntities().forEach { schemaTx.dropEntity(it) }

            /* Remove schema from catalogue. */
            SchemaCatalogueEntry.delete(name, this@DefaultCatalogue, this.context.txn.xodusTx)
            Unit
        }
    }
}