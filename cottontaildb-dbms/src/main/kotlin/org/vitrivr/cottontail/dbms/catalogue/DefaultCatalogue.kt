package org.vitrivr.cottontail.dbms.catalogue

import it.unimi.dsi.fastutil.longs.Long2ObjectFunction
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Environments
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.FunctionRegistry
import org.vitrivr.cottontail.dbms.catalogue.entries.MetadataEntry
import org.vitrivr.cottontail.dbms.catalogue.entries.MetadataEntry.Companion.METADATA_ENTRY_DB_VERSION
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.EntityMetadata
import org.vitrivr.cottontail.dbms.events.SchemaEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionStatus
import org.vitrivr.cottontail.dbms.functions.initialize
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata
import org.vitrivr.cottontail.dbms.index.cache.InMemoryIndexCache
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.dbms.schema.SchemaMetadata
import org.vitrivr.cottontail.dbms.sequence.DefaultSequence
import java.nio.file.Files
import java.nio.file.Path
import java.util.LinkedList

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
    }

    /** Root to Cottontail DB root folder. */
    val path: Path = this.config.root

    /** Constant name of the [DefaultCatalogue] object. */
    override val name: Name.RootName
        get() = Name.RootName

    /** Constant parent [DBO], which is null in case of the [DefaultCatalogue]. */
    override val parent: DBO? = null

    /** Constant parent [DBO], which is null in case of the [DefaultCatalogue]. */
    override val catalogue: Catalogue
        get() = this

    /** The [FunctionRegistry] exposed by this [Catalogue]. */
    override val functions: FunctionRegistry = FunctionRegistry()

    /** An internal, in-memory cache for frequently used index structures. This is highly experimental! */
    val cache = InMemoryIndexCache()

    /**
     * A [Object2ObjectOpenHashMap] of [Schema]s held by this [DefaultCatalogue].
     *
     * These are cached to avoid re-creating them for every query.
     */
    private val schemas = Object2ObjectOpenHashMap<Name.SchemaName, Schema>()

    /** An internal cache of all ongoing [DefaultSchema.Tx]s for this [DefaultSchema]. */
    private val transactions = Long2ObjectOpenHashMap<DefaultCatalogue.Tx>()

    /** The [Environment] backing this [DefaultCatalogue]. */
    private val environment = Environments.newInstance(
        this.config.catalogueFolder().toFile(),
        this.config.xodus.toEnvironmentConfig()
    )

    init {
        /* Check if catalogue has been initialized and initialize if needed. */
        val tx = this.environment.beginExclusiveTransaction()
        try {
            if (this.environment.getAllStoreNames(tx).size < 7) {
                /* Initialize database metadata. */
                MetadataEntry.store(tx)
                MetadataEntry.write(MetadataEntry(METADATA_ENTRY_DB_VERSION, this.version.toString()), tx)

                /* Initialize necessary stores. */
                SchemaMetadata.store(tx)
                EntityMetadata.store(tx)
                ColumnMetadata.store(tx)
                IndexMetadata.store(tx)
                DefaultSequence.store(tx)
            }

            /* Check database version. */
            val version = MetadataEntry.read(METADATA_ENTRY_DB_VERSION, tx)?.let {  DBOVersion.valueOf(it.value) } ?: DBOVersion.UNDEFINED
            if (version != this.version) {
                throw DatabaseException.VersionMismatchException(this.version, version)
            }

            /* Load schemas. */
            val store = SchemaMetadata.store(tx)
            store.openCursor(tx).use { cursor ->
                while (cursor.next) {
                    this.schemas[NameBinding.Schema.fromEntry(cursor.key)] = DefaultSchema(NameBinding.Schema.fromEntry(cursor.key), this)
                }
            }

            /* Commit transaction. */
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
    override fun createOrResumeTx(context: QueryContext): Tx = this.transactions.computeIfAbsent(context.txn.transactionId, Long2ObjectFunction {
        val subTransaction = Tx(context)
        context.txn.registerSubtransaction(subTransaction)
        subTransaction
    })

    /**
     * Closes the [DefaultCatalogue] and all objects contained within.
     */
    override fun close() {
        try {
            this.environment.close()
        } catch (e: Throwable) {
            LOGGER.error("Failed to close catalogue environment. Some transaction may be lost now.", e)
        }
    }

    /**
     * A [Tx] that affects this [DefaultCatalogue].
     */
    inner class Tx(override val context: QueryContext): CatalogueTx, SubTransaction.WithCommit,  SubTransaction.WithAbort, SubTransaction.WithFinalization {

        /** The Xodus [Transaction] backings this [DefaultCatalogue]*/
        internal val xodusTx = this@DefaultCatalogue.environment.beginTransaction()

        /** A [List] of [SchemaEvent]s that were executed through this [Tx]. */
        private val events = LinkedList<SchemaEvent>()

        /** Reference to the [DefaultCatalogue] this [CatalogueTx] belongs to. */
        override val dbo: DefaultCatalogue
            get() = this@DefaultCatalogue

        /**
         * A [Object2ObjectOpenHashMap] of [Schema]s held by this [DefaultCatalogue].
         *
         * Every [DefaultCatalogue.Tx] holds a snapshot of the [Schema]s held by the [DefaultCatalogue].
         */
        private val schemas = Object2ObjectLinkedOpenHashMap(this@DefaultCatalogue.schemas)

        /**
         * Returns a list of [Name.SchemaName] held by this [DefaultCatalogue].
         *
         * @return [List] of all [Name.SchemaName].
         */
        @Synchronized
        override fun listSchemas(): List<Name.SchemaName> {
            return this.schemas.keys.toList()
        }

        /**
         * Returns the [Schema] for the given [Name.SchemaName].
         *
         * @param name [Name.SchemaName] to obtain the [Schema] for.
         */
        @Synchronized
        override fun schemaForName(name: Name.SchemaName): Schema {
            return this.schemas[name] ?: throw DatabaseException.SchemaDoesNotExistException(name)
        }

        /**
         * Creates a new, empty [Schema] with the given [Name.SchemaName] and [Path]
         *
         * @param name The [Name.SchemaName] of the new [Schema].
         */
        @Synchronized
        override fun createSchema(name: Name.SchemaName): Schema {
            val store = SchemaMetadata.store(this.xodusTx)
            val metadata = SchemaMetadata(System.currentTimeMillis(), System.currentTimeMillis())
            if (!store.add(this.xodusTx, NameBinding.Schema.toEntry(name), SchemaMetadata.toEntry(metadata))) {
                throw DatabaseException.SchemaAlreadyExistsException(name) /* Schema already exists. */
            }

            /* Create schema. */
            val schema = DefaultSchema(name, this@DefaultCatalogue)
            this.schemas[name] = schema

            /* Create Event and notify observers */
            val event = SchemaEvent.Create(schema)
            this.events.add(event)
            this.context.txn.signalEvent(event)

            /* Return schema. */
            return schema
        }

        /**
         * Drops an existing [Schema] with the given [Name.SchemaName].
         *
         * @param name The [Name.SchemaName] of the [Schema] to be dropped.
         */
        @Synchronized
        override fun dropSchema(name: Name.SchemaName) {
            /* Remove entity. */
            val schema = this.schemas.remove(name) ?: throw DatabaseException.SchemaDoesNotExistException(name)

            /* Check if schema metadata exists! */
            val store = SchemaMetadata.store(this.xodusTx)
            if (!store.delete(this.xodusTx, NameBinding.Schema.toEntry(name))) {
                throw DatabaseException.SchemaDoesNotExistException(name)
            }

            /* Obtain schema Tx and drop all entities contained in schema. */
            val schemaTx = schema.newTx(this)
            schemaTx.listEntities().forEach { schemaTx.dropEntity(it) }

            /* Create Event and notify observers */
            val event = SchemaEvent.Drop(schema)
            this.events.add(event)
            this.context.txn.signalEvent(event)
        }

        /**
         * Checks if this [DefaultCatalogue.Tx] is prepared for commit by comparing the high address of the
         * local Xodus transaction with the high address of the latest snapshot
         */
        @Synchronized
        override fun prepareCommit(): Boolean {
            check(this.context.txn.state == TransactionStatus.PREPARE) { "\"Parent-transaction is not in PREPARE state." }
            if (this.xodusTx.isIdempotent) return true
            return this.xodusTx.environment.computeInReadonlyTransaction {
                it.highAddress == this.xodusTx.highAddress
           }
        }

        /**
         * Commits the [DefaultCatalogue.Tx] and persists all changes.
         */
        @Synchronized
        override fun commit() {
            check(this.context.txn.state == TransactionStatus.COMMIT) { "Parent-transaction is not in COMMIT state." }
            check(!this.xodusTx.isFinished) { "Xodus transaction ${this.xodusTx} is already finished. This is a programmer's error!" }
            if (this.xodusTx.isIdempotent) {
                this.xodusTx.abort()
            } else {
                if (this.xodusTx.commit()) {

                } else {
                    throw DatabaseException.DataCorruptionException("Failed to commit transaction in COMMIT phase.")
                }
            }
        }

        /**
         * Aborts the [DefaultCatalogue.Tx] and reverts all changes.
         */
        @Synchronized
        override fun abort() {
            check(!this.xodusTx.isFinished) { "Xodus transaction ${this.xodusTx} is already finished. This is a programmer's error!" }
            this.xodusTx.abort()
        }

        /**
         * Materializes local changes in owning [DefaultCatalogue].
         */
        @Synchronized
        override fun finalize(commit: Boolean) {
            this@DefaultCatalogue.transactions.remove(this.context.txn.transactionId)
            if (!commit) return
            for (event in this.events) {
                when (event) {
                    is SchemaEvent.Create -> this@DefaultCatalogue.schemas[event.schema.name] = event.schema
                    is SchemaEvent.Drop ->  this@DefaultCatalogue.schemas.remove(event.schema.name)
                }
            }
        }
    }
}