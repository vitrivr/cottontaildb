package org.vitrivr.cottontail.dbms.execution.transactions

import it.unimi.dsi.fastutil.Hash.VERY_FAST_LOAD_FACTOR
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Environments
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.ExecutionManager
import org.vitrivr.cottontail.dbms.execution.locking.Lock
import org.vitrivr.cottontail.dbms.execution.locking.LockHolder
import org.vitrivr.cottontail.dbms.execution.locking.LockManager
import org.vitrivr.cottontail.dbms.execution.locking.LockMode
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode.READ
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode.WRITE
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.Tx
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.dbms.statistics.StatisticsManager
import java.io.Closeable
import java.lang.ref.SoftReference
import java.nio.file.Files
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.deleteRecursively

/**
 * The default [TransactionManager] for Cottontail DB. It hosts all the necessary facilities to
 * create and execute queries within different [Transaction]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@OptIn(ExperimentalPathApi::class)
class TransactionManager(val executionManager: ExecutionManager, val config: Config): Closeable {

    companion object {
        /** The [Logger] instance used for logging. */
        private val LOGGER: Logger = org.slf4j.LoggerFactory.getLogger(TransactionManager::class.java)
    }

    /** Map of [AbstractTransaction]s that are currently PENDING or RUNNING. */
    private val transactions = Collections.synchronizedMap(Long2ObjectOpenHashMap<AbstractTransaction>(this.config.execution.transactionTableSize, VERY_FAST_LOAD_FACTOR))

    /** List of past [TransactionMetadata] objects. */
    private val transactionHistory: MutableList<TransactionMetadata> = Collections.synchronizedList(ArrayList(this.config.execution.transactionHistorySize))

    /** Set of [TransactionObserver]s registered with this [TransactionManager]. */
    private val observers = Collections.synchronizedSet(ObjectOpenHashSet<TransactionObserver>())

    /** Internal counter to generate [TransactionId]s. Starts with 1 */
    private val tidCounter = AtomicLong(1L)

    /** Map of [Environment]s for the different entites managed by this [TransactionManager]. */
    private val environments = ConcurrentHashMap<UUID, Environment>()

    /** The [Environment] for the main [Catalogue]. */
    private val catalogueEnvironment = Environments.newInstance(
        this.config.catalogueFolder().toFile(),
        this.config.xodus.toEnvironmentConfig()
    )

    /** A map of all [Lock]s held by this [TransactionManager]. */
    internal val locks = LockManager<Name>()

    /** The [StatisticsManager] instance used by this [TransactionManager]. */
    val statistics = StatisticsManager(this.config.statistics, this)

    /** The [DefaultCatalogue] held by this [TransactionMetadata]*/
    val catalogue = DefaultCatalogue(this.catalogueEnvironment)

    /**
     * Returns the [AbstractTransaction] for the provided [TransactionId].
     *
     * @param txId [TransactionId] to return the [AbstractTransaction] for.
     * @return [AbstractTransaction] or null
     */
    operator fun get(txId: TransactionId): Transaction? = this.transactions[txId]

    /**
     * Returns a list of [TransactionMetadata] of all ongoing and past transactions.
     *
     * @return [List] of [TransactionMetadata]
     */
    fun history(): List<TransactionMetadata> {
        return this.transactionHistory + this.transactions.values.toList()
    }

    /**
     * Registers a [TransactionObserver] with this [TransactionManager].
     *
     * The mechanism makes sure, that all ongoing transactions finish before registration.
     *
     * @param observer [TransactionObserver] to register.
     * @return True on success, false otherwise.
     */
    fun register(observer: TransactionObserver) {
        this.observers.add(observer)
    }

    /**
     * De-registers the given [TransactionObserver] with this [TransactionManager].
     *
     * The mechanism makes sure, that all ongoing transactions finish before de-registration.
     *
     * @param observer [TransactionObserver] to de-register.
     * @return True on success, false otherwise.
     */
    fun deregister(observer: TransactionObserver) {
        this.observers.remove(observer)
    }

    /**
     * Attempts to shut down this [TransactionManager].
     */
    override fun close() {
        /** Kill ongoing transactions. */
        for (txn in this.transactions.values) {
            txn.kill()
        }

        /* Close all environments. */
        this.environments.values.forEach { it.close() }
        this.environments.clear()
        this.catalogueEnvironment.close()

        /* Closes the statistics manager. */
        this.statistics.close()

        /* Close temporary environment and cleanup. */
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
    }

    /**
     * Returns the [Environment] for the given [UUID] [handle].
     *
     * @param handle The [UUID] of the [Environment] to return.
     * @return [Environment]
     */
    fun environment(handle: UUID): Environment = this.environments[handle] ?: throw IllegalStateException("No environment with handle $handle found. This is a programmer's error!")

    /**
     * Creates and registers a new [Environment] for the given [UUID] [handle].
     *
     * @param handle The [UUID] of the [Environment] to return.
     * @return New  [Environment]
     */
    fun createEnvironment(handle: UUID): Environment {
        require(!this.environments.containsKey(handle)) { "Environment with handle $handle already exists. This is a programmer's error!" }
        val handlePath = this.config.dataFolder().resolve(handle.toString())
        if (Files.notExists(handlePath)) {
            Files.createDirectories(handlePath)
        }
        val environment: Environment = Environments.newInstance(handlePath.toFile(), this.config.xodus.toEnvironmentConfig())
        this.environments[handle] = environment
        return environment
    }

    /**
     * De-registers and deletes an existing [Environment] for the given [UUID] [handle].
     * If no [Environment] for the [UUID] exists, this method does nothing.
     *
     * @param handle The [UUID] of the [Environment] to return.
     * @return New  [Environment]
     */
    fun deleteEnvironment(handle: UUID) {
        val environment = this.environments.remove(handle)
        if (environment != null) {
            environment.close()

            /* Delete folder. */
            val handlePath = this.config.dataFolder().resolve(handle.toString())
            handlePath.deleteRecursively()
        }
    }

    /**
     * An [AbstractTransaction] used for executing read-only queries.
     *
     * [SnapshotReadonly] transactions operate on a moment-in-time snapshots of the database, that is
     * acquired upon access to a [DBO]. No serialization with concurrent transactions takes place.
     */
    inner class SnapshotReadonly: AbstractTransaction(TransactionType.SNAPSHOT_READONLY) {
        override fun catalogueTx(mode: AccessMode): CatalogueTx {
            require(mode == READ) { "Read-only transaction can only be used for read-only access." }
            return this.txns.computeIfAbsent(Name.RootName) {
                this@TransactionManager.catalogue.newTx(this)
            } as CatalogueTx
        }

        override fun schemaTx(name: Name.SchemaName, mode: AccessMode): SchemaTx {
            require(mode == READ) { "Read-only transaction can only be used for read-only access." }
            return this.txns.computeIfAbsent(name) {
                val catalogue = this@TransactionManager.catalogue.newTx(this)
                catalogue.schemaForName(name).newTx(catalogue)
            } as SchemaTx
        }

        override fun entityTx(name: Name.EntityName, mode: AccessMode): EntityTx {
            require(mode == READ) { "Read-only transaction can only be used for read-only access." }
            return this.txns.computeIfAbsent(name) {
                val schemaTx = this.schemaTx(name.schema(), READ)
                val entity = schemaTx.entityForName(name)
                entity.newTx(schemaTx)
            } as EntityTx
        }

        override fun columnTx(name: Name.ColumnName, mode: AccessMode): ColumnTx<*> {
            require(mode == READ) { "Read-only transaction can only be used for read-only access." }
            return this.txns.computeIfAbsent(name) {
                val entityTx = this.entityTx(name.entity()!!, READ)
                val column = entityTx.columnForName(name)
                column.newTx(entityTx)
            } as ColumnTx<*>
        }

        override fun indexTx(name: Name.IndexName, mode: AccessMode): IndexTx {
            require(mode == READ) { "Read-only transaction can only be used for read-only access." }
            return this.txns.computeIfAbsent(name) {
                val entityTx = this.entityTx(name.entity(), READ)
                val index = entityTx.indexForName(name)
                index.newTx(entityTx)
            } as IndexTx
        }
    }

    /**
     * A [Transaction] used for executing read-only queries.
     *
     * [SerializableReadonly] transaction require a shared lock on the [DBO] they operate on, and thus
     * synchronize it with other writeable [Transaction]s.
     */
    inner class SerializableReadonly(): AbstractTransaction(TransactionType.SERIALIZABLE_READONLY) {
        override fun catalogueTx(mode: AccessMode): CatalogueTx {
            require(mode == READ) { "Read-only transaction can only be used for read-only access." }
            return this.txns.computeIfAbsent(Name.RootName) {
                this@TransactionManager.locks.lock(this, Name.RootName, LockMode.SHARED)
                this@TransactionManager.catalogue.newTx(this)
            } as CatalogueTx
        }

        override fun schemaTx(name: Name.SchemaName, mode: AccessMode): SchemaTx {
            require(mode == READ) { "Read-only transaction can only be used for read-only access." }
            return this.txns.computeIfAbsent(name) {
                this@TransactionManager.locks.lock(this, name, LockMode.SHARED)
                val catalogue = this@TransactionManager.catalogue.newTx(this)
                catalogue.schemaForName(name).newTx(catalogue)
            } as SchemaTx
        }

        override fun entityTx(name: Name.EntityName, mode: AccessMode): EntityTx {
            require(mode == READ) { "Read-only transaction can only be used for read-only access." }
            return this.txns.computeIfAbsent(name) {
                this@TransactionManager.locks.lock(this, name, LockMode.SHARED)
                val schemaTx = this.schemaTx(name.schema(), READ)
                val entity = schemaTx.entityForName(name)
                entity.newTx(schemaTx)
            } as EntityTx
        }

        override fun columnTx(name: Name.ColumnName, mode: AccessMode): ColumnTx<*> {
            require(mode == READ) { "Read-only transaction can only be used for read-only access." }
            return this.txns.computeIfAbsent(name) {
                this@TransactionManager.locks.lock(this, name, LockMode.SHARED)
                val entityTx = this.entityTx(name.entity()!!, READ)
                val column = entityTx.columnForName(name)
                column.newTx(entityTx)
            } as ColumnTx<*>
        }

        override fun indexTx(name: Name.IndexName, mode: AccessMode): IndexTx {
            require(mode == READ) { "Read-only transaction can only be used for read-only access." }
            return this.txns.computeIfAbsent(name) {
                this@TransactionManager.locks.lock(this, name, LockMode.SHARED)
                val entityTx = this.entityTx(name.entity(), READ)
                val index = entityTx.indexForName(name)
                index.newTx(entityTx)
            } as IndexTx
        }
    }

    /**
     * A [Transaction] used for executing read and write queries.
     *
     * [Snapshot] transactions require an exclusive lock on the [DBO] they write. However, read-access
     * is lock-free and does not synchronize with other [Transaction]s.
     */
    inner class Snapshot(): AbstractTransaction(TransactionType.SNAPSHOT) {
        override fun catalogueTx(mode: AccessMode): CatalogueTx {
            return this.txns.computeIfAbsent(Name.RootName) {
                if (mode == WRITE) this@TransactionManager.locks.lock(this, Name.RootName, LockMode.EXCLUSIVE)
                this@TransactionManager.catalogue.newTx(this)
            } as CatalogueTx
        }

        override fun schemaTx(name: Name.SchemaName, mode: AccessMode): SchemaTx {
            return this.txns.computeIfAbsent(name) {
                if (mode == WRITE) this@TransactionManager.locks.lock(this, name, LockMode.EXCLUSIVE)
                val catalogueTx = this.catalogueTx(mode)
                catalogueTx.schemaForName(name).newTx(catalogueTx)
            } as SchemaTx
        }

        override fun entityTx(name: Name.EntityName, mode: AccessMode): EntityTx {
            return this.txns.computeIfAbsent(name) {
                if (mode == WRITE) this@TransactionManager.locks.lock(this, name, LockMode.EXCLUSIVE)
                val schemaTx = this.schemaTx(name.schema(), READ)
                val entity = schemaTx.entityForName(name)
                entity.newTx(schemaTx)
            } as EntityTx
        }

        override fun columnTx(name: Name.ColumnName, mode: AccessMode): ColumnTx<*> {
            return this.txns.computeIfAbsent(name) {
                if (mode == WRITE) this@TransactionManager.locks.lock(this, name, LockMode.EXCLUSIVE)
                val entityTx = this.entityTx(name.entity()!!, mode) /* Changing a column requires a write-access to the entity. */
                val column = entityTx.columnForName(name)
                column.newTx(entityTx)
            } as ColumnTx<*>
        }

        override fun indexTx(name: Name.IndexName, mode: AccessMode): IndexTx {
            return this.txns.computeIfAbsent(name) {
                if (mode == WRITE) this@TransactionManager.locks.lock(this, name, LockMode.EXCLUSIVE)
                val entityTx = this.entityTx(name.entity(), mode) /* Changing an index requires a write-access to the entity. */
                val index = entityTx.indexForName(name)
                index.newTx(entityTx)
            } as IndexTx
        }
    }

    /**
     * A [Transaction] used for executing read and write queries.
     *
     * [SerializableReadonly] transactions require locks on the [DBO] before they can access it
     * and therefore synchronize with other [Transaction]s.
     */
    inner class Serializable(): AbstractTransaction(TransactionType.SERIALIZABLE) {
        override fun catalogueTx(mode: AccessMode): CatalogueTx {
            return this.txns.computeIfAbsent(Name.RootName) {
                this@TransactionManager.locks.lock(this, Name.RootName, mode.lock)
                this@TransactionManager.catalogue.newTx(this)
            } as CatalogueTx
        }

        override fun schemaTx(name: Name.SchemaName, mode: AccessMode): SchemaTx {
            return this.txns.computeIfAbsent(name) {
               this@TransactionManager.locks.lock(this, name, mode.lock)
                val catalogueTx = this.catalogueTx(mode)
                catalogueTx.schemaForName(name).newTx(catalogueTx)
            } as SchemaTx
        }

        override fun entityTx(name: Name.EntityName, mode: AccessMode): EntityTx {
            return this.txns.computeIfAbsent(name) {
                this@TransactionManager.locks.lock(this, name, mode.lock)
                val schemaTx = this.schemaTx(name.schema(), READ)
                val entity = schemaTx.entityForName(name)
                entity.newTx(schemaTx)
            } as EntityTx
        }

        override fun columnTx(name: Name.ColumnName, mode: AccessMode): ColumnTx<*> {
            return this.txns.computeIfAbsent(name) {
                this@TransactionManager.locks.lock(this, name, mode.lock)
                val entityTx = this.entityTx(name.entity()!!, mode) /* Changing a column requires a write-access to the entity. */
                val column = entityTx.columnForName(name)
                column.newTx(entityTx)
            } as ColumnTx<*>
        }

        override fun indexTx(name: Name.IndexName, mode: AccessMode): IndexTx {
            return this.txns.computeIfAbsent(name) {
                this@TransactionManager.locks.lock(this, name, mode.lock)
                val entityTx = this.entityTx(name.entity(), mode) /* Changing an index requires a write-access to the entity. */
                val index = entityTx.indexForName(name)
                index.newTx(entityTx)
            } as IndexTx
        }
    }


    /**
     * A concrete [AbstractTransaction] used for executing a query.
     *
     * A [AbstractTransaction] can be of different [TransactionType]s. Their execution semantics may differ slightly.
     *
     * @author Ralph Gasser
     * @version 2.0.0
     */
    abstract inner class AbstractTransaction(override val type: TransactionType): Transaction, LockHolder<Name>(this@TransactionManager.tidCounter.getAndIncrement()) {

        /** The [TransactionStatus] of this [AbstractTransaction]. */
        @Volatile
        final override var state: TransactionStatus = TransactionStatus.IDLE
            private set

        /** Reference to the enclosing [TransactionManager]. */
        override val manager: TransactionManager
            get() = this@TransactionManager

        /**
         * A [MutableMap] of all [TransactionObserver] and the [Event]s that were collected for them.
         *
         * Since a lot of [Event]s can build-up during a [TransactionMetadata], the [MutableList] is wrapped in a [SoftReference],
         * which can be claimed by the garbage collector. Such a situation will result in failure to notify observers. Consequently,
         * the channel provided by this facility must be considered unreliable!
         */
        private val localObservers: MutableMap<TransactionObserver, SoftReference<MutableList<Event>>> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

        /** Map of all [Tx] that have been created as part of this [AbstractTransaction]. */
        protected val txns: MutableMap<Name, Tx> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

        /** A [Mutex] data structure used for synchronisation on the [AbstractTransaction]. */
        private val mutex = Mutex()

        /** A [HashSet] containing ALL [CoroutineContext] instances associated with execution in this [AbstractTransaction], */
        private val activeContexts = HashSet<CoroutineContext>()

        /** Timestamp of when this [AbstractTransaction] was created. */
        final override val created: Long = System.currentTimeMillis()

        /** Timestamp of when this [AbstractTransaction] was either COMMITTED or ABORTED. */
        final override var ended: Long? = null
            private set

        /** Number of queries executed successfully in this [AbstractTransaction]. */
        @Volatile
        final override var numberOfSuccess: Int = 0
            private set

        /** Number of queries executed with an error in this [AbstractTransaction]. */
        @Volatile
        final override var numberOfError: Int = 0
            private set

        /** Number of ongoing queries in this [AbstractTransaction]. */
        final override val numberOfOngoing: Int
            get() = this.activeContexts.size

        /** The number of available workers for query execution. */
        override val availableQueryWorkers: Int
            get() = this@TransactionManager.executionManager.availableQueryWorkers()

        /** The number of available workers for intra-query execution. */
        override val availableIntraQueryWorkers: Int
            get() = this@TransactionManager.executionManager.availableIntraQueryWorkers()

        init {
            /** Add this transaction to the manager's transaction table. */
            this@TransactionManager.transactions[this.transactionId] = this

            /**
             * Create transaction, local snapshot of the registered transaction observers.
             *
             * Observers registered after the transaction has started, are considered! This is a design choice!
             */
            for (observer in this@TransactionManager.observers) {
                this.localObservers[observer] = SoftReference(LinkedList<Event>())
            }
        }

        /**
         * Signals an [Event] to this [AbstractTransaction].
         *
         * Those [Event]s are stored, if any of the registered [localObservers]s signal interest in the [Event].
         *
         * @param event The [Event] that has been reported.
         */
        override fun signalEvent(event: Event) {
            for ((observer, listRef) in this.localObservers) {
                val list = listRef.get()
                if (list != null && observer.isRelevant(event)) {
                    list.add(event)
                }
            }
        }

        /**
         * Schedules an [Operator] for execution in this [AbstractTransaction] and blocks, until execution has completed.
         *
         * @param operator The [Operator.SinkOperator] that should be executed.
         */
        override fun execute(operator: Operator): Flow<Tuple> = operator.toFlow().flowOn(this@TransactionManager.executionManager.queryDispatcher).onStart {
            this@AbstractTransaction.mutex.withLock {  /* Update transaction state; synchronise with ongoing COMMITS or ROLLBACKS. */
                check(this@AbstractTransaction.state.canExecute) {
                    "Cannot start execution of transaction ${this@AbstractTransaction.transactionId} because it is in the wrong state (s = ${this@AbstractTransaction.state})."
                }
                this@AbstractTransaction.state = TransactionStatus.RUNNING
                this@AbstractTransaction.activeContexts.add(currentCoroutineContext())
            }
        }.onCompletion {
            this@AbstractTransaction.mutex.withLock { /* Update transaction state; synchronise with ongoing COMMITS or ROLLBACKS. */
                /* Remove context. */
                this@AbstractTransaction.activeContexts.remove(currentCoroutineContext())

                /* Update statistics. */
                if (it == null) {
                    this@AbstractTransaction.numberOfSuccess += 1
                } else {
                    this@AbstractTransaction.numberOfError += 1
                }

                /* Update state of transaction if necessary. */
                if (this@AbstractTransaction.state == TransactionStatus.RUNNING && this@AbstractTransaction.activeContexts.isEmpty()) {
                    this@AbstractTransaction.state = TransactionStatus.IDLE
                }
            }
        }.cancellable()

        /**
         * Commits this [AbstractTransaction] thus finalizing and persisting all operations executed so far.
         */
        override fun commit() = runBlocking {
            this@AbstractTransaction.mutex.withLock { /* Synchronise with ongoing COMMITS, ROLLBACKS or queries that are being scheduled. */
                if (!this@AbstractTransaction.state.canCommit)
                    throw TransactionException.Commit(this@AbstractTransaction.transactionId, "Unable to COMMIT because transaction is in wrong state (s = ${this@AbstractTransaction.state}).")
                this@AbstractTransaction.state = TransactionStatus.FINALIZING

                /* Phase 1: Perform pre-commit finalization. */
                for (txn in this@AbstractTransaction.txns.values.filterIsInstance<Tx.BeforeCommit>()) {
                    try {
                        txn.beforeCommit()
                    } catch (e: Throwable) {
                        this@AbstractTransaction.state = TransactionStatus.ERROR
                        LOGGER.warn("Failed to execute beforeCommit() on ${txn.dbo.name}. This is not supposed to happen. Transaction will be aborted!", e)
                        break
                    }
                }


                /* Phase 2: Execute commit (if no error occurred). */
                for (txn in this@AbstractTransaction.txns.values.filterIsInstance<Tx.Commitable>()) {
                    if (this@AbstractTransaction.state ==  TransactionStatus.ERROR) {
                        txn.rollback() /* Execute individual commits. */
                    } else {
                        txn.commit() /* Execute individual commits. */
                    }
                }

                /* Phase 3: Finalize transaction. */
                this@AbstractTransaction.finalizeTransaction()
                if (this@AbstractTransaction.state === TransactionStatus.COMMIT) {
                    this@AbstractTransaction.notifyObservers()
                }
            }
        }

        /**
         * Rolls back this [AbstractTransaction] thus reverting all operations executed so far.
         */
        override fun rollback() = runBlocking {
            this@AbstractTransaction.mutex.withLock {
                if (!this@AbstractTransaction.state.canRollback)
                    throw TransactionException.Rollback(this@AbstractTransaction.transactionId, "Unable to ROLLBACK because transaction is in wrong state (s = ${this@AbstractTransaction.state}).")
                this@AbstractTransaction.performRollback()
            }
        }

        /**
         * Kills this [AbstractTransaction] interrupting all running queries and rolling it back.
         */
        override fun kill() = runBlocking {
            this@AbstractTransaction.mutex.withLock {
                /* Cancel all running queries. */
                if (this@AbstractTransaction.state === TransactionStatus.RUNNING) {
                    this@AbstractTransaction.activeContexts.forEach {
                        it.cancel(CancellationException("Transaction ${this@AbstractTransaction.transactionId} was killed by user."))
                    }
                }
                this@AbstractTransaction.state = TransactionStatus.FINALIZING
            }

            /* Wait for all queries to be cancelled. */
            while (this@AbstractTransaction.activeContexts.isNotEmpty()) {
                delay(250)
            }

            /* Perform actual rollback. */
            this@AbstractTransaction.performRollback()
        }

        /**
         * Actually performs transaction rollback.
         */
        private fun performRollback() {
            this@AbstractTransaction.state = TransactionStatus.FINALIZING

            /* Phase 1: Perform pre-rollback finalization. */
            for (txn in this@AbstractTransaction.txns.values.filterIsInstance<Tx.BeforeRollback>()) {
                try {
                    txn.beforeRollback()
                } catch (e: Throwable) {
                    LOGGER.warn("Failed to execute beforeRollback() on ${txn.dbo.name}. This is not supposed to happen!", e)
                }
            }

            /* Phase 2: Execute commit (if no error occurred). */
            for (txn in this@AbstractTransaction.txns.values.filterIsInstance<Tx.Commitable>()) {
                try {
                    txn.rollback() /* Execute individual rollback. */
                } catch (e: Throwable) {
                    LOGGER.error("Failed to execute rollback() on ${txn.dbo.name}. This is a severe error!", e)
                }
            }
            this.state = TransactionStatus.ROLLBACK

            /* Phase 3: Finalize transaction. */
            this@AbstractTransaction.finalizeTransaction()
        }

        /**
         * Finalizes the state of this [AbstractTransaction].
         */
        protected open fun finalizeTransaction() {

            /* Release all locks. */
            for (txn in this.txns.values) {
                this@TransactionManager.locks.unlock(this, txn.dbo.name)
            }

            /* Clear Txn objects. */
            this.txns.clear()

            /* Update timestamp. */
            this@AbstractTransaction.ended = System.currentTimeMillis()

            /* Remove this transaction from the list of ongoing transactions. */
            this@TransactionManager.transactions.remove(this@AbstractTransaction.transactionId)

            /* Add this transaction to the transaction history. */
            this@TransactionManager.transactionHistory.add(FinishedTransaction(this))
            if (this@TransactionManager.transactionHistory.size >= this@TransactionManager.config.execution.transactionHistorySize) {
                this@TransactionManager.transactionHistory.removeAt(0)
            }
        }

        /**
         * Notifies all [TransactionObserver]s about a successful commit.
         */
        private fun notifyObservers(){
            for ((observer, listRef) in this.localObservers) {
                val list = listRef.get()
                if (list != null) {
                    observer.onCommit(this.transactionId, list)    /* Signal COMMIT to local observers. */
                } else {
                    observer.onDeliveryFailure(this.transactionId) /* Signal DELIVERY FAILURE to local observers. */
                }
            }

            /* Clear local observers to release memory. */
            this.localObservers.clear()
        }
    }
}