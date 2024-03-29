package org.vitrivr.cottontail.legacy

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps
import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.sources.partitionFor
import org.vitrivr.cottontail.dbms.execution.transactions.*
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.Tx
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.storage.serializers.tablets.Compression
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.io.BufferedWriter
import java.lang.Math.floorDiv
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * An abstract implementation of the [MigrationManager].
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
@ExperimentalTime
abstract class AbstractMigrationManager(private val batchSize: Int, logFile: Path) : MigrationManager {

    /** The internal [TransactionId] counter. */
    private val transactionIdCounter = AtomicLong(0L)

    /** [BufferedWriter] used to write to the log file. */
    private val writer: BufferedWriter = Files.newBufferedWriter(
        logFile.resolve("cottontaildb_migration_${System.currentTimeMillis()}.log"),
        StandardOpenOption.CREATE,
        StandardOpenOption.CREATE
    )

    /**
     * Tries to open the source [Catalogue] for migration.
     *
     * @param config The [Config] to open the [Catalogue] with.
     * @return Source [Catalogue] of null upon failure.
     */
    abstract fun openSourceCatalogue(config: Config): Catalogue?

    /**
     * Tries to open the destination [Catalogue] for migration.
     *
     * @param config The [Config] to open the [Catalogue] with.
     * @return Destination [DefaultCatalogue] of null upon failure.
     */
    abstract fun openDestinationCatalogue(config: Config): DefaultCatalogue?

    /**
     * Executes the data migration.
     */
    override fun migrate(config: Config) {
        /** Opens old catalogue. */
        val duration = measureTime {
            this.log("Starting catalogue migration from ${this.from} for ${config.root}.\n")
            val srcCatalogue: Catalogue? = this.openSourceCatalogue(config)
            if (srcCatalogue == null) {
                this.log("Failed to open source catalogue.\n")
                return
            } else {
                this.log("Source catalogue ${srcCatalogue.config.root} loaded successfully.\n")
            }

            /* Open and create new catalogue. */
            val migratedDatabaseRoot = config.root.parent.resolve("${config.root.fileName}~migrated")
            if (!Files.exists(migratedDatabaseRoot)) {
                Files.createDirectories(migratedDatabaseRoot)
            }
            val dstCatalogue: DefaultCatalogue? = this.openDestinationCatalogue(config.copy(root = migratedDatabaseRoot))
            if (dstCatalogue == null) {
                this.log("Failed to open destination catalogue.\n")
                srcCatalogue.close()
                return
            } else {
                this.log("Destination catalogue ${dstCatalogue.config.root} loaded successfully.\n")
            }

            /* Windows flag in case of cleanup exceptions (file system does not allow move) */
            var isWindows = false

            /* Execute actual data migration. */
            try {
                /* Migrates all DBOs. */
                this.migrateDBOs(srcCatalogue, dstCatalogue)

                /* Migrates all data. */
                this.migrateData(srcCatalogue, dstCatalogue)

                /* Swap folders. */
                Files.move(config.root, config.root.parent.resolve("${config.root.fileName}~old"), StandardCopyOption.ATOMIC_MOVE)
                Files.move(migratedDatabaseRoot, config.root, StandardCopyOption.ATOMIC_MOVE)
            } catch (e: Throwable) {
                this.log("Error during data migration: ${e.message}\n")
                isWindows = System.getProperty("os.name").lowercase().contains("windows")
                /* Delete destination (Cleanup). */
                TxFileUtilities.delete(dstCatalogue.path)
            } finally {
                /* Close catalogues. */
                srcCatalogue.close()
                dstCatalogue.close()
            }
            if(isWindows){
                this.log("""
                            Windows detected. Finish the migration before a restart manually:
                              1. Move ${config.root} -> ${config.root.parent.resolve("${config.root.fileName}~old")}
                              2. Move $migratedDatabaseRoot -> ${config.root}
                              3. Delete ${dstCatalogue.path}
                        """.trimIndent())
            }
        }
        this.log("Data migration completed. Took $duration.\n")
    }

    /**
     * Migrates all [DBO] from the source [Catalogue] to the destination [Catalogue].
     *
     * This is done in a single transaction!
     *
     * @param source The source [Catalogue].
     * @param destination The destination [DefaultCatalogue].
     */
    protected open fun migrateDBOs(source: Catalogue, destination: DefaultCatalogue) {
        /* Execute actual data migration. */
        val sourceContext = DefaultQueryContext("migration", source, LegacyMigrationContext())
        val destinationContext = DefaultQueryContext("migration", destination, MigrationContext(destination.transactionManager.environment.beginExclusiveTransaction()))
        val srcCatalogueTx = source.newTx(sourceContext)
        val dstCatalogueTx = destination.newTx(destinationContext)

        /* Migrate schemas. */
        val schemas = srcCatalogueTx.listSchemas()
        for ((s, srcSchemaName) in schemas.withIndex()) {
            this.log("+ Migrating schema $srcSchemaName (${s + 1} / ${schemas.size}):\n")

            val schema = dstCatalogueTx.createSchema(srcSchemaName)
            val srcSchemaTx = srcCatalogueTx.schemaForName(srcSchemaName).newTx(sourceContext)
            val dstSchemaTx = schema.newTx(destinationContext)
            val entities = srcSchemaTx.listEntities()

            /* Migrate entities. */
            for ((i, srcEntityName) in entities.withIndex()) {
                this.log("-- Migrating entity $srcEntityName (${i + 1} / ${entities.size}):\n")
                val srcEntityTx = srcSchemaTx.entityForName(srcEntityName).newTx(sourceContext)
                val entity = dstSchemaTx.createEntity(srcEntityName, srcEntityTx.listColumns().map { it.name to ColumnMetadata(it.type, Compression.LZ4, it.nullable, it.primary, it.autoIncrement) })

                /* Migrate indexes. */
                for (indexName in srcEntityTx.listIndexes()) {
                    this.log("---- Migrating index $indexName...\n")
                    val index = srcEntityTx.indexForName(indexName)
                    val indexTx = index.newTx(sourceContext)
                    val destEntityTx = entity.newTx(destinationContext)
                    destEntityTx.createIndex(index.name, index.type, indexTx.columns.map { it.name }, index.type.descriptor.buildConfig())
                }
            }
        }

        /* Commit after all DBOs have been rebuilt. */
        sourceContext.txn.commit()
        destinationContext.txn.commit()
    }

    /**
     * Migrates a single [Entity], including all its [Column]s.
     *
     * This is just a default implementation that covers the standard case and can be overwritten.
     *
     * @param source The [Catalogue] pointing to the source catalogue.
     * @param destination The [DefaultCatalogue] pointing to the destination catalogue.
     */
    protected open fun migrateData(source: Catalogue, destination: DefaultCatalogue) {

        val sourceContext = DefaultQueryContext("migration", source, LegacyMigrationContext())
        val srcCatalogueTx = source.newTx(sourceContext)
        val schemas = srcCatalogueTx.listSchemas()

        for ((s, srcSchemaName) in schemas.withIndex()) {
            val srcSchemaTx = srcCatalogueTx.schemaForName(srcSchemaName).newTx(sourceContext)
            val entities = srcSchemaTx.listEntities()
            this.logStdout("+ Migrating data for schema $srcSchemaName (${s + 1} / ${schemas.size})...\n")

            for (srcEntityName in entities) {
                val srcEntityTx = srcSchemaTx.entityForName(srcEntityName).newTx(sourceContext)
                val count = srcEntityTx.count()

                /* Start migrating column data. */
                if (count > 0) {
                    val size = srcEntityTx.largestTupleId() - srcEntityTx.smallestTupleId()
                    val partitions = floorDiv(size, this.batchSize).toInt() + 1
                    val columns = srcEntityTx.listColumns().toTypedArray()
                    var i = 0L
                    for (p in 0 until partitions) {
                        val destinationContext = DefaultQueryContext("migration", destination, MigrationContext(destination.transactionManager.environment.beginExclusiveTransaction()))
                        val destCatalogueTx = destination.newTx(destinationContext)
                        val destSchemaTx = destCatalogueTx.schemaForName(srcSchemaName).newTx(destinationContext)
                        val destEntityTx = destSchemaTx.entityForName(srcEntityName).newTx(destinationContext)
                        val cursor = srcEntityTx.cursor(columns, srcEntityTx.partitionFor(p, partitions))
                        cursor.forEach { r ->
                            this.logStdout("-- Migrating data for ${srcEntityName}... (${++i} / $count)\r")
                            destEntityTx.insert(r)
                        }
                        cursor.close()
                        this.log("-- Migrating data for $srcEntityName; committing... (${i} / $count)\r")
                        destinationContext.txn.commit()
                    }
                    this.log("-- Data migration for $srcEntityName completed (${i} / $count).\n")
                } else {
                    this.log("-- Data migration for $srcEntityName skipped (no data).\n")
                }
            }
        }

        /* Commit after all DBOs have been rebuilt. */
        sourceContext.txn.rollback()
    }

    /**
     * Logs a message to stdout and the log file.
     *
     * @param message The message to log.
     */
    private fun logStdout(message: String) {
        print(message)
    }

    /**
     * Logs a message to stdout and the log file.
     *
     * @param message The message to log.
     */
    protected fun log(message: String) {
        print(message)
        this.writer.append(message)
        this.writer.flush()
    }

    /**
     * Closes this [MigrationManager].
     */
    override fun close() {
        this.writer.close()
    }

    /**
     * A [MigrationContext] is a special type of [Transaction] used during data migration.
     *
     * @author Ralph Gasser
     * @version 2.0.1
     */
    inner class LegacyMigrationContext : Transaction {
        /** The [TransactionId] of the [MigrationContext]. */
        override val transactionId: TransactionId = transactionIdCounter.getAndIncrement()

        /** The [LegacyMigrationContext] does not hold a [jetbrains.exodus.env.Transaction] reference. */
        override val xodusTx: jetbrains.exodus.env.Transaction
            get() = throw UnsupportedOperationException("Xodus transaction not available for LegacyMigrationContext.")

        /** A [LegacyMigrationContext] does not have a reference to a [TransactionManager]. */
        override val manager: TransactionManager
            get() = throw UnsupportedOperationException("Xodus transaction not available for LegacyMigrationContext.")

        /** The [TransactionType] of a [MigrationManager] is always [TransactionType.SYSTEM_EXCLUSIVE]. */
        override val type: TransactionType = TransactionType.SYSTEM_EXCLUSIVE

        /** [LegacyMigrationContext] do not provide any query workers. */
        override val availableQueryWorkers = 0

        /** [LegacyMigrationContext] do not provide any intra query workers. */
        override val availableIntraQueryWorkers = 0

        /** The [TransactionStatus] of this [MigrationContext]. */
        @Volatile
        override var state: TransactionStatus = TransactionStatus.IDLE
            private set

        /** The timestamp at which this [TransactionMetadata] was created. */
        override val created: Long = System.currentTimeMillis()

        /** The timestamp at which this [TransactionMetadata] has ended created. */
        override var ended: Long? = null
            private set

        /** [LegacyMigrationContext] does not track transaction statistics. */
        override val numberOfSuccess: Int = 0

        /** [LegacyMigrationContext] does not track transaction statistics. */
        override val numberOfError: Int = 0

        /** [LegacyMigrationContext] does not track transaction statistics. */
        override val numberOfOngoing: Int = 0

        /** Map of all [Tx] that have been created as part of this [MigrationManager]. Used for final COMMIT or ROLLBACK. */
        private val txns: MutableMap<DBO, Tx> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

        /**
         * Caches a [Tx] for later re-use.
         *
         * @param tx The [DBO] to create the [Tx] for.
         * @return True on success, false otherwise.
         */
        override fun cacheTx(tx: Tx): Boolean {
            return this.txns.putIfAbsent(tx.dbo, tx) != null
        }

        /**
         * Returns the [Tx] for the provided [DBO]. Creating [Tx] through this method makes sure,
         * that only on [Tx] per [DBO] and [TransactionMetadata] is created.
         *
         * @param dbo [DBO] to return the [Tx] for.
         * @return entity [Tx]
         */
        @Suppress("UNCHECKED_CAST")
        override fun <T: Tx>getCachedTxForDBO(dbo: DBO): T = this.txns[dbo] as T

        override fun signalEvent(event: Event) {
            throw UnsupportedOperationException("Operation signalEvent() not supported for LegacyMigrationContext.")
        }

        override fun execute(operator: Operator): Flow<Tuple> {
            throw UnsupportedOperationException("Operation execute() not supported for LegacyMigrationContext.")
        }

        /**
         * Commits this [TransactionMetadata] thus finalizing and persisting all operations executed so far.
         */
        override fun commit() {
            check(this.state === TransactionStatus.IDLE) { "Cannot commit transaction ${this.transactionId} because it is in wrong state (s = ${this.state})." }
            this.state = TransactionStatus.FINALIZING
            this.txns.clear()
            this.ended = System.currentTimeMillis()
            this.state = TransactionStatus.COMMIT
        }

        /**
         * Rolls back this [TransactionMetadata] thus reverting all operations executed so far.
         */
        override fun rollback() {
            check(this.state === TransactionStatus.IDLE || this.state === TransactionStatus.ERROR) { "Cannot rollback transaction ${this.transactionId} because it is in wrong state (s = ${this.state})." }
            this.state = TransactionStatus.FINALIZING
            this.txns.clear()
            this.ended = System.currentTimeMillis()
            this.state = TransactionStatus.ROLLBACK
        }

        /**
         * Kills this [MigrationContext] thus reverting all operations executed so far.
         */
        override fun kill() {
            throw UnsupportedOperationException("Operation kill() not supported for LegacyMigrationContext.")
        }
    }

    /**
     * A [MigrationContext] is a special type of [Transaction] used during data migration.
     *
     * @author Ralph Gasser
     * @version 2.1.0
     */
    inner class MigrationContext(override val xodusTx: jetbrains.exodus.env.Transaction) : Transaction, TransactionMetadata {
        /** The [TransactionId] of the [MigrationContext]. */
        override val transactionId: TransactionId = transactionIdCounter.getAndIncrement()

        /** The [TransactionType] of a [MigrationManager] is always [TransactionType.SYSTEM_EXCLUSIVE]. */
        override val type: TransactionType = TransactionType.SYSTEM_EXCLUSIVE

        /** The [TransactionStatus] of this [MigrationContext]. */
        @Volatile
        override var state: TransactionStatus = TransactionStatus.IDLE
            private set

        /** The timestamp at which this [MigrationContext] was created. */
        override val created: Long = System.currentTimeMillis()

        /** The timestamp at which this [MigrationContext] has ended created. */
        override var ended: Long? = null
            private set

        /** [MigrationContext] does not track transaction statistics. */
        override val numberOfSuccess: Int = 0

        /** [MigrationContext] does not track transaction statistics. */
        override val numberOfError: Int = 0

        /** [MigrationContext] does not track transaction statistics. */
        override val numberOfOngoing: Int = 0

        /** A [MigrationContext] does not have a reference to a [TransactionManager]. */
        override val manager: TransactionManager
            get() = throw UnsupportedOperationException("Xodus transaction not available for LegacyMigrationContext.")

        /** [MigrationContext] do not provide any query workers. */
        override val availableQueryWorkers = 0

        /** [MigrationContext] do not provide any intra query workers. */
        override val availableIntraQueryWorkers = 0

        /** Map of all [Tx] that have been created as part of this [MigrationManager]. Used for final COMMIT or ROLLBACK. */
        private val txns: MutableMap<DBO, Tx> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

        /** List of all [Tx.WithCommitFinalization] that must be notified before commit. */
        private val notifyOnCommit: MutableList<Tx.WithCommitFinalization> = Collections.synchronizedList(LinkedList())

        /** List of all [Tx.WithRollbackFinalization] that must be notified before rollback. */
        private val notifyOnRollback: MutableList<Tx.WithRollbackFinalization> = Collections.synchronizedList(LinkedList())

        override fun cacheTx(tx: Tx): Boolean {
            return this.txns.putIfAbsent(tx.dbo, tx) != null
        }

        /**
         * Returns the [Tx] for the provided [DBO]. Creating [Tx] through this method makes sure,
         * that only on [Tx] per [DBO] and [TransactionMetadata] is created.
         *
         * @param dbo [DBO] to return the [Tx] for.
         * @return entity [Tx]
         */
        @Suppress("UNCHECKED_CAST")
        override fun <T: Tx>getCachedTxForDBO(dbo: DBO): T? {
            return this.txns[dbo] as T?
        }

        override fun signalEvent(event: Event) {
            /* No op. */
        }

        override fun execute(operator: Operator): Flow<Tuple> {
            throw UnsupportedOperationException("Operation execute() not supported for MigrationContext.")
        }

        /**
         * Commits this [MigrationContext] thus finalizing and persisting all operations executed so far.
         */
        override fun commit() {
            check(this.state === TransactionStatus.IDLE) { "Cannot commit transaction ${this.transactionId} because it is in wrong state (s = ${this.state})." }
            this.state = TransactionStatus.FINALIZING
            try {
                for (txn in this.notifyOnCommit) {
                    txn.beforeCommit()
                }
                this.xodusTx.commit()
            } catch (e: Throwable) {
                this.xodusTx.abort()
                throw e
            } finally {
                this.txns.clear()
                this.notifyOnCommit.clear()
                this.notifyOnRollback.clear()
                this.ended = System.currentTimeMillis()
                this.state = TransactionStatus.COMMIT
            }
        }

        /**
         * Rolls back this [MigrationContext] thus reverting all operations executed so far.
         */
        override fun rollback() {
            check(this.state === TransactionStatus.IDLE || this.state === TransactionStatus.ERROR) { "Cannot rollback transaction ${this.transactionId} because it is in wrong state (s = ${this.state})." }
            this.state = TransactionStatus.FINALIZING
            try {
                try {
                    for (txn in this.notifyOnRollback) {
                        txn.beforeRollback()
                    }
                } finally {
                    this.xodusTx.abort()
                }
            } finally {
                this.txns.clear()
                this.notifyOnCommit.clear()
                this.notifyOnRollback.clear()
                this.ended = System.currentTimeMillis()
                this.state = TransactionStatus.ROLLBACK
            }
        }

        /**
         * Kills this [MigrationContext] thus reverting all operations executed so far.
         */
        override fun kill() {
            throw UnsupportedOperationException("Operation kill() not supported for MigrationContext.")
        }
    }
}
