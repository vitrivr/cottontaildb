package org.vitrivr.cottontail.dbms.execution.transactions

import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.events.Event

/**
 * An observer of a [Transaction].
 *
 * [TransactionObserver]s can be used to act upon changes that were made through a [Transaction] once it concludes.
 * Once a [Transaction] finalizes, a [TransactionObserver] receives a summary of all [Event]s that were logged during the [Transaction].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface TransactionObserver {
    /**
     * Called when the [Transaction] with the [TransactionId] committed and all [Event]s have manifested.
     *
     * @param txId [TransactionId] that signals its [Event]s.
     * @param events The list of [Event]s in order at which they were applied.
     */
    fun didCommit(txId: TransactionId, events: List<Event>)

    /**
     * Called when the [Transaction] with the [TransactionId] aborts and all [Event]s were dropped.
     *
     * @param txId [TransactionId] that signals its [Event]s.
     * @param events The list of [Event]s in order at which they were applied.
     */
    fun didAbort(txId: TransactionId, events: List<Event>)
}