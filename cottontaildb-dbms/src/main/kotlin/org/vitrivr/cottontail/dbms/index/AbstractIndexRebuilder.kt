package org.vitrivr.cottontail.dbms.index

import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import java.util.*

/**
 * A [AbstractIndexRebuilder] de-couples the step uf building-up and merging the changes with the actual [Index] structure.
 *
 * This can be advantageous for [Index] structures, that require a long time to rebuild. The first (long) step can be
 * executed in a read-only [TransactionContext], using non-blocking reads while the second (shorter) step is executed
 * in a separate [TransactionContext] thereafter.
 *
 * In order to be informed about changes that happen in the meanwhile, the [AbstractIndexRebuilder] implements the
 * [TransactionObserver], which it uses to be informed about changes to the data.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractIndexRebuilder(context: TransactionContext): TransactionObserver {
    /** The [TransactionId] that created and owns this [TransactionId]. */
    val ownerTx: TransactionId = context.txId

    /** The [TransactionContext] that created and owns this [TransactionId]. */
    abstract val index: Index

    /** Internal [LinkedList] of [DataEvent]s that should be processed. */
    protected val events = LinkedList<DataEvent>()

    /**
     * Merges this [AbstractIndexRebuilder] with its [IndexTx] using the given [TransactionContext].
     *
     * @param context The [TransactionContext] to perform the merge in.
     */
    abstract fun merge(context: TransactionContext)

    /**
     * Processes incoming [DataEvent]s and stores them for later reference.
     *
     * The implementation of this method strictly assumes that it only receives [DataEvent]s that
     * affect the entity hosting the [Index] rebuilt by this [AbstractIndexRebuilder].
     */
    @Synchronized
    override fun didCommit(txId: TransactionId, events: List<Event>) {
        for (event in events) {
            when(event) {
                is DataEvent.Insert -> {
                    require(event.entity == this.index.name.entity()) { "DataEvent $event received that does not concern this index. This is a programmer's error!" }
                    this.events.add(event)
                }
                is DataEvent.Update -> {
                    require(event.entity == this.index.name.entity()) { "DataEvent $event received that does not concern this index. This is a programmer's error!" }
                    this.events.removeIf { it.tupleId == event.tupleId } /* Reconciliation: UPDATES supersede all previous events. */
                    this.events.add(event)
                }
                is DataEvent.Delete -> {
                    require(event.entity == this.index.name.entity()) { "DataEvent $event received that does not concern this index. This is a programmer's error!" }
                    this.events.removeIf { it.tupleId == event.tupleId } /* Reconciliation: DELETES supersede all previous events. */
                    this.events.add(event)
                }
                else -> continue
            }
        }
    }

    /** No op. */
    @Synchronized
    override fun didAbort(txId: TransactionId, events: List<Event>) {
        /* No op. */
    }
}