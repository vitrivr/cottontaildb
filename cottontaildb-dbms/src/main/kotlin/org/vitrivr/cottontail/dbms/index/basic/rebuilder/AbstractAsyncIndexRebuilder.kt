package org.vitrivr.cottontail.dbms.index.basic.rebuilder

import jetbrains.exodus.env.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexState
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import java.nio.file.Files
import java.util.*
import java.util.concurrent.atomic.AtomicLong

/**
 * A [AbstractAsyncIndexRebuilder] de-couples the step uf building-up and merging the changes with the actual [Index] structure.
 *
 * This can be advantageous for [Index] structures, that require a long time to rebuild. The first (long) step can be
 * executed in a read-only [Transaction], using non-blocking reads while the second (shorter) step is executed
 * in a separate [Transaction] thereafter.
 *
 * In order to be informed about changes that happen in the meanwhile, the [AbstractAsyncIndexRebuilder] implements the
 * [TransactionObserver], which it uses to be informed about changes to the data.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
abstract class AbstractAsyncIndexRebuilder<T: Index>(final override val index: T, private val catalogue: Catalogue, private val manager: TransactionManager): AsyncIndexRebuilder<T> {

    companion object {
        /** [Logger] instance used by [AbstractAsyncIndexRebuilder]. */
        internal val LOGGER: Logger = LoggerFactory.getLogger(AbstractAsyncIndexRebuilder::class.java)

        /** Internal counter to keep track of then umber of spawned [AbstractAsyncIndexRebuilder]. */
        private val COUNTER = AtomicLong(0L)
    }

    /** The [IndexRebuilderState] of this [AbstractAsyncIndexRebuilder] */
    @Volatile
    final override var state: IndexRebuilderState = IndexRebuilderState.INITIALIZED
        private set

    /** Path to the temporary [Environment] */
    private val tmpPath = this.index.catalogue.config.temporaryDataFolder().resolve("${index.type.toString().lowercase()}-rebuild-${UUID.randomUUID()}")

    /** The temporary [Environment] used by this [AbstractAsyncIndexRebuilder]. */
    protected val tmpEnvironment: Environment = Environments.newInstance(this.tmpPath.toFile(), this.index.catalogue.config.xodus.toEnvironmentConfig().setGcUtilizationFromScratch(false).setGcEnabled(false))

    /** The Xodus [Transaction] object of the temporary environment. */
    protected val tmpTx: jetbrains.exodus.env.Transaction = this.tmpEnvironment.beginExclusiveTransaction()

    /**
     * The [Name.EntityName] the [Index] rebuilt by this [AbstractAsyncIndexRebuilder] works with.
     *
     * Used for [Event] filtering.
     */
    protected val entityName: Name.EntityName = this.index.name.entity()

    /** Internal sequence number for this [AbstractAsyncIndexRebuilder]. */
    private val sequenceNumber = COUNTER.incrementAndGet()

    /** An [AbstractAsyncIndexRebuilder] is only interested in [DataEvent]s that concern the [Entity]. */
    override fun isRelevant(event: Event): Boolean
        = event is DataEvent && event.entity == this.entityName

    /**
     * Scans the data necessary for this [AbstractAsyncIndexRebuilder]. Usually, this takes place within an existing [QueryContext].
     */
    @Synchronized
    override fun build() {
        require(this.state == IndexRebuilderState.INITIALIZED) { "Cannot perform SCAN with index builder because it is in the wrong state."}
        LOGGER.debug("Scanning index ${this.index.name} (${this.index.type}).")

        /* Acquire query context; requires write-latch to prevent concurrent data events from "seeping" through. */
        val context = this.manager.computeExclusively {
            val transaction = this.manager.startTransaction(TransactionType.SYSTEM_READONLY)
            val context = DefaultQueryContext("auto-rebuild-scan-$sequenceNumber", this.catalogue, transaction)
            this.manager.register(this)
            this.state = IndexRebuilderState.REBUILDING
            context
        }

        try {
            /* Start BUILD phase of process. */
            if (!this.internalBuild(context)) {
                this.state = IndexRebuilderState.ABORTED
                LOGGER.error("Scanning index ${this.index.name} (${this.index.type}) failed.")
                context.txn.rollback()
                return
            }

            /* Commit transaction. */
            context.txn.commit()
            this.state = IndexRebuilderState.REBUILT
            LOGGER.debug("Scanning index ${this.index.name} (${this.index.type}) completed!")
        } catch (e: Throwable) {
            context.txn.rollback()
            LOGGER.error("Scanning index ${this.index.name} (${this.index.type}) failed due to exception: ${e.message}")
        }
    }

    /**
     * Merges this [AbstractAsyncIndexRebuilder] with its [Index] using the given [QueryContext].
     */
    @Synchronized
    override fun replace() {
        require(this.state == IndexRebuilderState.REBUILT) { "Cannot perform MERGE with index builder because it is in the wrong state."}
        LOGGER.debug("Merging index ${this.index.name} (${this.index.type}).")

        /* Acquire query context; requires write-latch to prevent concurrent data events from "seeping" through. */
        val transaction = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val context = DefaultQueryContext("auto-rebuild-replace-$sequenceNumber", this.catalogue, transaction)
        this.manager.deregister(this)

        /* Clear store and update state of index (* ---> DIRTY). */
        try {
            if (!IndexCatalogueEntry.updateState(this.index.name, this.index.catalogue as DefaultCatalogue, IndexState.DIRTY, context.txn.xodusTx)) {
                this.state = IndexRebuilderState.ABORTED
                LOGGER.error("Merging index ${this.index.name} (${this.index.type}) failed because index state could not be changed to DIRTY!")
                context.txn.rollback()
                return
            }

            /* Drain side-channel. */
            if (!this.drainAndMergeLog()) {
                this.state = IndexRebuilderState.ABORTED
                LOGGER.error("Merging index ${this.index.name} (${this.index.type}) failed because not all side-channel could be processed.")
                context.txn.rollback()
                return
            }
            this.tmpTx.flush()

            /* Execute actual REPLACEMENT phase. */
            this.state = IndexRebuilderState.REPLACING
            val dataStore: Store = this.clearAndOpenStore(context.txn)
            if (!this.internalReplace(context, dataStore)) {
                this.state = IndexRebuilderState.ABORTED
                LOGGER.error("Merging index ${this.index.name} (${this.index.type}) failed.")
                context.txn.rollback()
                return
            }

            /* Update state of index (DIRTY ---> CLEAN). */
            if (!IndexCatalogueEntry.updateState(this.index.name, this.index.catalogue as DefaultCatalogue, IndexState.CLEAN, context.txn.xodusTx)) {
                this.state = IndexRebuilderState.ABORTED
                LOGGER.error("Merging index ${this.index.name} (${this.index.type}) failed because index state could not be changed to CLEAN!")
                context.txn.rollback()
                return
            }

            /* Commit transaction. */
            context.txn.commit()

            this.state = IndexRebuilderState.FINISHED
            LOGGER.debug("Merging index ${this.index.name} (${this.index.type}) completed!")
        } catch (e: Throwable) {
            LOGGER.error("Merging index ${this.index.name} (${this.index.type}) failed because of exception: ${e.message}")
            context.txn.rollback()
            this.state = IndexRebuilderState.ABORTED
        }
    }

    /**
     * If an external transaction reports a successful COMMIT, the committed information must be considered by this [AbstractAsyncIndexRebuilder].
     *
     * @param txId The [TransactionId] that reports the commit.
     * @param events The [List] of [Event]s that should be processed.
     * @see TransactionObserver
     */
    final override fun onCommit(txId: TransactionId, events: List<Event>) {
        for (event in events) {
            require(event is DataEvent) { "Event $event is not a DataEvent." }
            require(event.entity == this.index.name.entity()) { "DataEvent $event received that does not concern this index. This is a programmer's error!" }
            if (!this.processSideChannelEvent(event)) {
                this.state = IndexRebuilderState.ABORTED
                LOGGER.error("Index rebuild failed due to side-channel message processing failure: $event")
                break
            }
        }
    }

    /**
     * If delivery of transaction information fails, then this [AbstractAsyncIndexRebuilder] must abort because otherwise, it will create an inconsistent index.
     *
     * @param txId The [TransactionId] that is reporting.
     */
    final override fun onDeliveryFailure(txId: TransactionId) {
        this.state = IndexRebuilderState.ABORTED
        LOGGER.error("Index rebuild failed due to side-channel message delivery failure.")
    }

    /**
     * Closes this [AbstractAsyncIndexRebuilder].
     */
    @Synchronized
    override fun close() {
        try {
            if (this.state != IndexRebuilderState.FINISHED) {
                this.state = IndexRebuilderState.FINISHED

                /* Just to make sure! */
                this.manager.deregister(this)

                /* Abort transaction and close environment. */
                this.tmpTx.abort()
                this.tmpEnvironment.clear()

                /* Tries to clean-up the temporary environment. */
                Files.walk(this.tmpPath).sorted(Comparator.reverseOrder()).forEach {
                    try {
                        Files.delete(it)
                    } catch (e: Throwable) {
                        /* No op. */
                    }
                }

                LOGGER.debug("Asynchronous index re-builder index ${this.index.name} (${this.index.type}) discarded!")
            }
        } catch (e: Throwable) {
            LOGGER.warn("Asynchronous index re-builder for index ${this.index.name} (${this.index.type}) could not be discarded: ${e.message}")
        }
    }

    /**
     * Internal scan method that is being executed when executing the BUILD stage of this [AbstractAsyncIndexRebuilder].
     *
     * @param context The [QueryContext] to execute the BUILD stage in.
     * @return True on success, false otherwise.
     */
    abstract fun internalBuild(context: QueryContext): Boolean

    /**
     * Internal merge method that is being executed when executing the REPLACE stage of this [AbstractAsyncIndexRebuilder].
     *
     * @param context The [QueryContext] to execute the REPLACE stage in.
     * @param store The [Store] to merge data into.
     * @return True on success, false otherwise.
     */
    abstract fun internalReplace(context: QueryContext, store: Store): Boolean

    /**
     * Processes and usually enqueues a [DataEvent] for later persisting it in the index rebuilt by this [AbstractAsyncIndexRebuilder].
     *
     * @param event [DataEvent] that should be processed.
     * @return True, if data event could be processed and enqueue, false otherwise.
     */
    abstract fun processSideChannelEvent(event: DataEvent): Boolean

    /**
     * Drains and processes all [DataEvent]s that are currently waiting on the side-channel.
     *
     * @return True on success, false otherwise.
     */
    abstract fun drainAndMergeLog(): Boolean

    /**
     * Clears and opens the data store associated with this [AbstractIndexRebuilder].
     *
     * @param context The [Transaction] to execute operation in.
     * @return [Store]
     */
    private fun clearAndOpenStore(context: Transaction): Store {
        val storeName = this.index.name.storeName()
        this.index.catalogue.transactionManager.environment.truncateStore(storeName, context.xodusTx)
        return this.index.catalogue.transactionManager.environment.openStore(storeName, StoreConfig.USE_EXISTING, context.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this.index.name} is missing.")
    }
}