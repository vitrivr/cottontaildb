package org.vitrivr.cottontail.dbms.exceptions

import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction

/**
 * These [Exception]s are thrown whenever a [Transaction] or a [SubTransaction] making up a [Transaction] fails for some reason.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class TransactionException(message: String) : DatabaseException(message) {

    /**
     * COMMIT could not be executed because.
     *
     * @param tid The [TransactionId] of the [SubTransaction] in which this error occurred.
     * @param message Description of the validation error.
     */
    class Commit(tid: TransactionId, override val message: String?) : TransactionException("Transaction $tid could not be committed: $message")

    /**
     * ROLLBACK could not be executed because.
     *
     * @param tid The [TransactionId] of the [SubTransaction] in which this error occurred.
     * @param message Description of the validation error.
     */
    class Rollback(tid: TransactionId, override val message: String?) : TransactionException("Transaction $tid could not be rolled back: $message")

    /**
     * Thrown if a [Transaction] could not be committed, because it is in conflict with another [Transaction].
     *
     * @param tid The [TransactionId] of the [SubTransaction] in which this error occurred.
     */
    class InConflict(tid: TransactionId) : TransactionException("Transaction $tid could not be committed because of conflict with another transaction.")

    /**
     * Thrown if a [Transaction] could not be started due to a timeout while waiting for the lock.
     *
     * @param tid The [TransactionId] of the [SubTransaction] in which this error occurred.
     */
    class Timeout(tid: TransactionId) : TransactionException("Transaction $tid could not be started because of a timeout while waiting for the lock.")
}