package org.vitrivr.cottontail.dbms.index.basic.rebuilder

import jetbrains.exodus.env.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexState
import java.nio.file.Files
import java.util.*
import java.util.concurrent.locks.ReentrantLock

/**
 * A [AbstractAsyncIndexRebuilder] de-couples the step uf building-up and merging the changes with the actual [Index] structure.
 *
 * This can be advantageous for [Index] structures, that require a long time to rebuild. The first (long) step can be
 * executed in a read-only [TransactionContext], using non-blocking reads while the second (shorter) step is executed
 * in a separate [TransactionContext] thereafter.
 *
 * In order to be informed about changes that happen in the meanwhile, the [AbstractAsyncIndexRebuilder] implements the
 * [TransactionObserver], which it uses to be informed about changes to the data.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractAsyncIndexRebuilder<T: Index>(final override val index: T): AsyncIndexRebuilder<T> {

    companion object {
        /** [Logger] instance used by [AbstractAsyncIndexRebuilder]. */
        internal val LOGGER: Logger = LoggerFactory.getLogger(AbstractAsyncIndexRebuilder::class.java)
    }

    /** The [IndexRebuilderState] of this [AbstractAsyncIndexRebuilder] */
    @Volatile
    final override var state: IndexRebuilderState = IndexRebuilderState.INITIALIZED
        private set

    /** Path to the temporary [Environment] */
    private val tmpPath = this.index.catalogue.config.temporaryDataFolder().resolve("${index.type.toString().lowercase()}-rebuild-${UUID.randomUUID()}")

    /** The temporary [Environment] used by this [AbstractAsyncIndexRebuilder]. */
    protected val tmpEnvironment: Environment = Environments.newInstance(this.tmpPath.toFile(), this.index.catalogue.config.xodus.toEnvironmentConfig())

    /** The Xodus [Transaction] object of the temporary environment. */
    protected val tmpTx: Transaction = this.tmpEnvironment.beginExclusiveTransaction()

    /**
     * The [Name.EntityName] the [Index] rebuilt by this [AbstractAsyncIndexRebuilder] works with.
     *
     * Used for [Event] filtering.
     */
    protected val entityName: Name.EntityName = this.index.name.entity()

    /** An [AbstractAsyncIndexRebuilder] is only interested in [DataEvent]s that concern the [Entity]. */
    override fun isRelevant(event: Event): Boolean
        = event is DataEvent && event.entity == this.entityName

    /** A [ReentrantLock] that makes sure, that the two rebuild-steps are not invoked concurrently. */
    private val rebuildLock = ReentrantLock()

    /** A [ReentrantLock] that makes sure that messaging from other transactions are processed sequentially. */
    private val asyncLock = ReentrantLock()

    /**
     * Scans the data necessary for this [AbstractAsyncIndexRebuilder]. Usually, this takes place within an existing [TransactionContext].
     *
     * @param context1 The [TransactionContext] to perform the SCAN in.
     */
    override fun scan(context1: TransactionContext) {
        this.rebuildLock.lock()
        try {
            require(this.state == IndexRebuilderState.INITIALIZED) { "Cannot perform SCAN with index builder because it is in the wrong state."}

            LOGGER.debug("Scanning index ${this.index.name} (${this.index.type}).")

            this.state = IndexRebuilderState.SCANNING
            if (!this.internalScan(context1)) {
                this.state = IndexRebuilderState.ABORTED
                LOGGER.debug("Scanning index ${this.index.name} (${this.index.type}) failed.")
                return
            }
            this.state = IndexRebuilderState.SCANNED

            LOGGER.debug("Scanning index ${this.index.name} (${this.index.type}) completed!")
        } finally {
            this.rebuildLock.unlock()
        }
    }

    /**
     * Merges this [AbstractAsyncIndexRebuilder] with its [Index] using the given [TransactionContext].
     *
     * @param context2 The [TransactionContext] to perform the MERGE in.
     */
    override fun merge(context2: TransactionContext) {
        this.rebuildLock.lock()
        try {
            /* Sanity check. */
            require(this.state  == IndexRebuilderState.SCANNED) { "Cannot perform MERGE with index builder because it is in the wrong state."}
            require(context2.xodusTx.isExclusive) { "Failed to rebuild index ${this.index.name} (${this.index.type}); merge operation requires exclusive transaction."}

            LOGGER.debug("Merging index ${this.index.name} (${this.index.type}).")

            /* Clear store and update state of index (* ---> DIRTY). */
            val dataStore: Store = this.clearAndOpenStore(context2)
            if (!IndexCatalogueEntry.updateState(this.index.name, this.index.catalogue as DefaultCatalogue, IndexState.DIRTY, context2.xodusTx)) {
                this.state = IndexRebuilderState.ABORTED
                LOGGER.error("Merging index ${this.index.name} (${this.index.type}) failed because index state could not be changed to DIRTY!")
                return
            }

            /* Execute actual merging. */
            this.state = IndexRebuilderState.MERGING
            if (!this.internalMerge(context2, dataStore)) {
                this.state = IndexRebuilderState.ABORTED
                LOGGER.debug("Merging index ${this.index.name} (${this.index.type}) failed.")
                return
            }

            /* Update state of index (DIRTY ---> CLEAN). */
            if (!IndexCatalogueEntry.updateState(this.index.name, this.index.catalogue as DefaultCatalogue, IndexState.CLEAN, context2.xodusTx)) {
                this.state = IndexRebuilderState.ABORTED
                LOGGER.error("Merging index ${this.index.name} (${this.index.type}) failed because index state could not be changed to CLEAN!")
                return
            }

            this.state = IndexRebuilderState.MERGED
        } finally {
            this.rebuildLock.unlock()
        }
    }

    /**
     * Internal scan method that is being executed when executing the SCAN stage of this [AbstractAsyncIndexRebuilder].
     *
     * @param context1 The [TransactionContext] to execute the SCAN stage in.
     * @return True on success, false otherwise.
     */
    abstract fun internalScan(context1: TransactionContext): Boolean

    /**
     * Internal merge method that is being executed when executing the MERGE stage of this [AbstractAsyncIndexRebuilder].
     *
     * @param context2 The [TransactionContext] to execute the MERGE stage in.
     * @param store The [Store] to merge data into.
     * @return True on success, false otherwise.
     */
    abstract fun internalMerge(context2: TransactionContext, store: Store): Boolean

    /**
     * Internal method that applies a [DataEvent.Insert] from an external transaction to this [AbstractAsyncIndexRebuilder].
     *
     * @param event The [DataEvent.Insert] to process.
     * @return True on success, false otherwise.
     */
    protected abstract fun applyAsyncInsert(event: DataEvent.Insert): Boolean

    /**
     * Internal method that applies a [DataEvent.Update] from an external transaction to this [AbstractAsyncIndexRebuilder].
     *
     * @param event The [DataEvent.Update] to process.
     * @return True on success, false otherwise.
     */
    protected abstract fun applyAsyncUpdate(event: DataEvent.Update): Boolean

    /**
     * Internal method that apples a [DataEvent.Delete] from an external transaction to this [AbstractAsyncIndexRebuilder].
     *
     * @param event The [DataEvent.Delete] to process.
     * @return True on success, false otherwise.
     */
    protected abstract fun applyAsyncDelete(event: DataEvent.Delete): Boolean

    /**
     * If an external transaction reports a successful COMMIT, the committed information must be considered by this [AbstractAsyncIndexRebuilder].
     *
     * @param txId The [TransactionId] that reports the commit.
     * @param events The [List] of [Event]s that should be processed.
     * @see TransactionObserver
     */
    final override fun onCommit(txId: TransactionId, events: List<Event>) {
        /* Once IndexRebuilder has been merged, closed or aborted, we're no longer interested in updates. */
        this.asyncLock.lock()
        try {
            if (this.state !in setOf(IndexRebuilderState.INITIALIZED, IndexRebuilderState.ABORTED, IndexRebuilderState.MERGING, IndexRebuilderState.MERGED, IndexRebuilderState.FINISHED)) {
                for (event in events) {
                    val success = when(event) {
                        is DataEvent.Insert -> {
                            require(event.entity == this.index.name.entity()) { "DataEvent $event received that does not concern this index. This is a programmer's error!" }
                            this.applyAsyncInsert(event)
                        }
                        is DataEvent.Update -> {
                            require(event.entity == this.index.name.entity()) { "DataEvent $event received that does not concern this index. This is a programmer's error!" }
                            this.applyAsyncUpdate(event)
                        }
                        is DataEvent.Delete -> {
                            require(event.entity == this.index.name.entity()) { "DataEvent $event received that does not concern this index. This is a programmer's error!" }
                            this.applyAsyncDelete(event)
                        }
                        else -> continue
                    }
                    /* Check status of event processing. */
                    if (!success) {
                        this.state = IndexRebuilderState.ABORTED
                        break
                    }
                }
            }
        } finally {
            this.asyncLock.unlock()
        }
    }

    /**
     * If delivery of transaction information fails, then this [AbstractAsyncIndexRebuilder] must abort because otherwise, it will create an inconsistent index.
     *
     * @param txId The [TransactionId] that is reporting.
     */
    final override fun onDeliveryFailure(txId: TransactionId) {
        this.asyncLock.lock()
        if (this.state in setOf(IndexRebuilderState.SCANNING, IndexRebuilderState.SCANNED, IndexRebuilderState.MERGING)) {
            this.state = IndexRebuilderState.ABORTED
        }
        this.asyncLock.unlock()
    }

    /**
     * Closes this [AbstractAsyncIndexRebuilder].
     */
    override fun close() {
        this.rebuildLock.lock()
        this.asyncLock.lock()
        try {
            if (this.state != IndexRebuilderState.FINISHED) {
                this.state = IndexRebuilderState.FINISHED

                /* Abort transaction and close environment. */
                this.tmpTx.abort()
                this.tmpEnvironment.close()

                /* Tries to cleanup the temporary environment. */
                Files.walk(this.tmpPath).sorted(Comparator.reverseOrder()).forEach {
                    try {
                        Files.delete(it)
                    } catch (e: Throwable) {
                        /* No op. */
                    }
                }
            }
        } finally {
            this.rebuildLock.unlock()
            this.asyncLock.unlock()
        }
    }

    /**
     * Clears and opens the data store associated with this [AbstractIndexRebuilder].
     *
     * @param context The [TransactionContext] to execute operation in.
     * @return [Store]
     */
    private fun clearAndOpenStore(context: TransactionContext): Store {
        val storeName = this.index.name.storeName()
        (this.index.catalogue as DefaultCatalogue).environment.truncateStore(storeName, context.xodusTx)
        return (this.index.catalogue as DefaultCatalogue).environment.openStore(storeName, StoreConfig.USE_EXISTING, context.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this.index.name} is missing.")
    }
}