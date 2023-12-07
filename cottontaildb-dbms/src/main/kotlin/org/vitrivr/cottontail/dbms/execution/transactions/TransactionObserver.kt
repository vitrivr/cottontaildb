package org.vitrivr.cottontail.dbms.execution.transactions

import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.events.Event

/**
 * An observer of a [TransactionMetadata].
 *
 * [TransactionObserver]s can be used to act upon changes that were made through a [TransactionMetadata] once it concludes.
 * Once a [TransactionMetadata] finalizes, a [TransactionObserver] receives a summary of all [Event]s that were logged during the [TransactionMetadata].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface TransactionObserver {
    /**
     * Called to determine, whether this [TransactionObserver] is interested in the given [Event].
     *
     * @param event [Event] to check.
     * @return True if this [TransactionObserver] is interested in the given [Event]
     */
    fun isRelevant(event: Event): Boolean

    /**
     * Called when the [TransactionMetadata] with the [TransactionId] committed and all [Event]s have manifested.
     *
     * @param txId [TransactionId] that signals its [Event]s.
     * @param events The list of [Event]s in order at which they were applied.
     */
    fun onCommit(txId: TransactionId, events: List<Event>)

    /**
     * Called when the [TransactionMetadata] with the [TransactionId] failed to deliver all [Event]s.
     * Usually, this can be attributed to an OOM situation
     *
     * @param txId [TransactionId] that signals failure to deliver
     */
    fun onDeliveryFailure(txId: TransactionId)
}