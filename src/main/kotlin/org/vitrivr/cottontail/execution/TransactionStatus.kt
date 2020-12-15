package org.vitrivr.cottontail.execution

/**
 * Status of the [TransactionManager.Transaction].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
enum class TransactionStatus {
    OPEN,  /** [TransactionManager.Transaction] is running and able to process queries. */
    ERROR, /** [TransactionManager.Transaction] marked for rollback due to an execution error. */
    ROLLBACK, /** [TransactionManager.Transaction] was marked for rollback. */
    COMMIT, /** [TransactionManager.Transaction] was marked for commit. */
}