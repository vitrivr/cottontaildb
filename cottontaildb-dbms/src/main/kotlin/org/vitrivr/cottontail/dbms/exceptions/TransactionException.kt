package org.vitrivr.cottontail.dbms.exceptions

import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.Tx

/**
 * These [Exception]s are thrown whenever a [Transaction] or a [Tx] making up a [Transaction] fails for some reason.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class TransactionException(message: String) : DatabaseException(message) {

    /** Thrown if [Tx] object could not be created because enclosing DBO was closed.
     *
     * @param tid The [TransactionId] of the [Tx] in which this error occurred.
     */
    class DBOClosed(tid: TransactionId, dbo: DBO) : TransactionException("Tx object for transaction $tid could not be created for DBO '${dbo.name}': Enclosing DBO was closed.")

    /**
     * COMMIT could not be executed because.
     *
     * @param tid The [TransactionId] of the [Tx] in which this error occurred.
     * @param message Description of the validation error.
     */
    class Commit(tid: TransactionId, override val message: String?) : TransactionException("Transaction $tid could not be committed: $message")

    /**
     * ROLLBACK could not be executed because.
     *
     * @param tid The [TransactionId] of the [Tx] in which this error occurred.
     * @param message Description of the validation error.
     */
    class Rollback(tid: TransactionId, override val message: String?) : TransactionException("Transaction $tid could not be rolled back: $message")

    /**
     * Thrown if a [Transaction] could not be committed, because it is in conflict with another [Transaction].
     *
     * @param tid The [TransactionId] of the [Tx] in which this error occurred.
     */
    class InConflict(tid: TransactionId) : TransactionException("Transaction $tid could not be committed because of conflict with another transaction.")
}