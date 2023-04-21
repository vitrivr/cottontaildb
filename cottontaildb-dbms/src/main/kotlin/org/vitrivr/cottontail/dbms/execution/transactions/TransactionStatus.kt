package org.vitrivr.cottontail.dbms.execution.transactions

/**
 * Status of the [Transaction].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
enum class TransactionStatus(val canCommit: Boolean, val canRollback: Boolean, val canExecute: Boolean) {
    /** [Transaction] is idle and no query is being processed. */
    IDLE(true, true, true),

    /** [Transaction] is running and therefore COMMITS and ROLLBACK have to wait. */
    RUNNING(false, false, true),

    /** [Transaction] was marked for rollback due to an execution error. */
    ERROR(false, true, false),

    /** [Transaction] is committing or rolling back. Query execution not possible. */
    FINALIZING(false, false, false),

    /** [Transaction] was committed. Query execution not possible. */
    COMMIT(false, false, false),

    /** [Transaction] was rolled back. Query execution not possible.  */
    ROLLBACK(false, false, false),
}