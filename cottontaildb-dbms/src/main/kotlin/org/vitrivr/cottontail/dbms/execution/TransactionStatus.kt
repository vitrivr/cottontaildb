package org.vitrivr.cottontail.dbms.execution

/**
 * Status of the [TransactionManager.TransactionImpl].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
enum class TransactionStatus(val canCommit: Boolean, val canRollback: Boolean, val canExecute: Boolean) {
    /** [TransactionManager.TransactionImpl] is idle and no query is being processed. */
    IDLE(true, true, true),

    /** [TransactionManager.TransactionImpl] is running and therefore COMMITS and ROLLBACK have to wait. */
    RUNNING(false, false, true),

    /** [TransactionManager.TransactionImpl] was marked for rollback due to an execution error. */
    ERROR(false, true, false),

    /** [TransactionManager.TransactionImpl] is committing or rolling back. Query execution not possible. */
    FINALIZING(false, false, false),

    /** [TransactionManager.TransactionImpl] was committed. Query execution not possible. */
    COMMIT(false, false, false),

    /** [TransactionManager.TransactionImpl] was rolled back. Query execution not possible.  */
    ROLLBACK(false, false, false),
}