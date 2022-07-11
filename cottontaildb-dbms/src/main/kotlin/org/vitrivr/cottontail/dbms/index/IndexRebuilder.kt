package org.vitrivr.cottontail.dbms.index

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import java.io.Closeable
import java.nio.file.Files
import java.util.*
import java.util.concurrent.atomic.AtomicReference

/**
 * A [IndexRebuilder] de-couples the step uf building-up and merging the changes with the actual [Index] structure.
 *
 * This can be advantageous for [Index] structures, that require a long time to rebuild. The first (long) step can be
 * executed in a read-only [TransactionContext], using non-blocking reads while the second (shorter) step is executed
 * in a separate [TransactionContext] thereafter.
 *
 * In order to be informed about changes that happen in the meanwhile, the [IndexRebuilder] implements the
 * [TransactionObserver], which it uses to be informed about changes to the data.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class IndexRebuilder<T: Index>(val index: T): TransactionObserver, Closeable {

    /** The current [IndexRebuilderState] of this [IndexRebuilder]. */
    private val _state = AtomicReference(IndexRebuilderState.INITIALIZED)
    val state: IndexRebuilderState
        get() = this._state.get()

    /**
     * The [Name.EntityName] the [Index] rebuilt by this [IndexRebuilder] works with.
     *
     * Used for [Event] filtering.
     */
    protected val entityName: Name.EntityName = this.index.name.entity()

    /** Path to the temporary [Environment] */
    protected val tmpPath = this.index.catalogue.config.temporaryDataFolder().resolve("${index.type.toString().lowercase()}-rebuild-${UUID.randomUUID()}")

    /** The temporary [Environment] used by this [IndexRebuilder]. */
    protected val tmpEnvironment: Environment = Environments.newInstance(this.tmpPath.toFile(), this.index.catalogue.config.xodus.toEnvironmentConfig())

    /** The Xodus [Transaction] object of the temporary environment. */
    protected val tmpTx: Transaction = this.tmpEnvironment.beginExclusiveTransaction()

    /** Internal [LinkedList] of [DataEvent]s that should be processed. */
    protected val events = LinkedList<DataEvent>()

    /** An [IndexRebuilder] is only interested in [DataEvent]s that concern the [Entity]. */
    override fun isRelevant(event: Event): Boolean
        = event is DataEvent && event.entity == this.entityName

    /**
     * Scans the data necessary for this [IndexRebuilder]. Usually, this takes place within an existing [TransactionContext].
     */
    fun scan(context: TransactionContext, async: Boolean) {
        require(this._state.get() == IndexRebuilderState.INITIALIZED) { "Cannot perform SCAN with index builder because it is in the wrong state."}
        this.internalScan(context, async)
        this._state.compareAndSet(IndexRebuilderState.INITIALIZED, IndexRebuilderState.SCANNED)
    }

    /**
     * Merges this [IndexRebuilder] with its [IndexTx] using the given [TransactionContext].
     *
     * @param context The [TransactionContext] to perform the MERGE in.
     */
    fun merge(context: TransactionContext) {
        require(this._state.get() == IndexRebuilderState.SCANNED) { "Cannot perform MERGE with index builder because it is in the wrong state."}
        this.internalMerge(context)
        this._state.compareAndSet(IndexRebuilderState.SCANNED, IndexRebuilderState.MERGED)
    }

    /**
     * Internal scan method that is being executed when executing the SCAN stage of this [IndexRebuilder].
     *
     * @param context The [TransactionContext] to execute the SCAN stage in.
     */
    abstract fun internalScan(context: TransactionContext, async: Boolean)

    /**
     * Internal merge method that is being executed when executing the MERGE stage of this [IndexRebuilder].
     *
     * @param context The [TransactionContext] to execute the MERGE stage in.
     */
    abstract fun internalMerge(context: TransactionContext)

    /**
     * Internal method that apples a [DataEvent.Insert] from an external transaction to this [IndexRebuilder].
     *
     * @param event The [DataEvent.Insert] to process.
     * @return True on success, false otherwise.
     */
    protected abstract fun applyInsert(event: DataEvent.Insert): Boolean

    /**
     * Internal method that apples a [DataEvent.Update] from an external transaction to this [IndexRebuilder].
     *
     * @param event The [DataEvent.Update] to process.
     * @return True on success, false otherwise.
     */
    protected abstract fun applyUpdate(event: DataEvent.Update): Boolean

    /**
     * Internal method that apples a [DataEvent.Delete] from an external transaction to this [IndexRebuilder].
     *
     * @param event The [DataEvent.Delete] to process.
     * @return True on success, false otherwise.
     */
    protected abstract fun applyDelete(event: DataEvent.Delete): Boolean

    /**
     * If an external transaction reports a successful COMMIT, the committed information must be considered by this [IndexRebuilder].
     *
     * @param txId The [TransactionId] that reports the commit.
     * @param events The [List] of [Event]s that should be processed.
     * @see TransactionObserver
     */
    @Synchronized
    final override fun onCommit(txId: TransactionId, events: List<Event>) {
        /* Once IndexRebuilder has been merged, closed or aborted, we're no longer interested in updates. */
        if (this.state !in setOf(IndexRebuilderState.ABORTED, IndexRebuilderState.MERGED, IndexRebuilderState.CLOSED)) {
            for (event in events) {
                val success = when(event) {
                    is DataEvent.Insert -> {
                        require(event.entity == this.index.name.entity()) { "DataEvent $event received that does not concern this index. This is a programmer's error!" }
                        this.applyInsert(event)
                    }
                    is DataEvent.Update -> {
                        require(event.entity == this.index.name.entity()) { "DataEvent $event received that does not concern this index. This is a programmer's error!" }
                        this.applyUpdate(event)
                    }
                    is DataEvent.Delete -> {
                        require(event.entity == this.index.name.entity()) { "DataEvent $event received that does not concern this index. This is a programmer's error!" }
                        this.applyDelete(event)
                    }
                    else -> continue
                }
                /* Check status of event processing. */
                if (success) {
                    this._state.set(IndexRebuilderState.ABORTED)
                    break
                }
            }
        }
    }

    /**
     * If delivery of transaction information fails, then this [IndexRebuilder] must abort because otherwise, it will create an inconsistent index.
     *
     * @param txId The [TransactionId] that is reporting.
     */
    @Synchronized
    final override fun onDeliveryFailure(txId: TransactionId) {
        this._state.getAndUpdate {
            if (it in setOf(IndexRebuilderState.ABORTED, IndexRebuilderState.MERGED, IndexRebuilderState.CLOSED)) {
                it
            } else {
                IndexRebuilderState.ABORTED
            }
        }
    }

    /**
     * Closes this [IndexRebuilder].
     */
    override fun close() {
        if (this._state.getAndSet(IndexRebuilderState.CLOSED) != IndexRebuilderState.CLOSED) {
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
    }
}