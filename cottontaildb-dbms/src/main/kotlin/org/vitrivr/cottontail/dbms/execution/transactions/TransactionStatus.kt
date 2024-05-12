package org.vitrivr.cottontail.dbms.execution.transactions

/**
 * Status of the [TransactionMetadata].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
enum class TransactionStatus(val canCommit: Boolean, val canRollback: Boolean, val canExecute: Boolean) {
    /** [TransactionMetadata] is idle and no query is being processed. */
    IDLE(true, true, true),

    /** [TransactionMetadata] is running and therefore COMMITS and ROLLBACK have to wait. */
    RUNNING(false, false, true),

    /** [TransactionMetadata] was marked for rollback due to an execution error. */
    ERROR(false, true, false),

    /** [TransactionMetadata] is committing or rolling back. Query execution not possible. */
    PREPARE(false, false, false),

    /** [TransactionMetadata] was committed. Query execution not possible. */
    COMMIT(false, false, false),

    /** [TransactionMetadata] was committed. Query execution not possible. */
    ABORT(false, false, false),

    /** [TransactionMetadata] was committed. Query execution not possible. */
    COMMITTED(false, false, false),

    /** [TransactionMetadata] was rolled back. Query execution not possible.  */
    ABORTED(false, false, false)
}