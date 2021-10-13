package org.vitrivr.cottontail.execution

/**
 * Status of the [TransactionManager.TransactionImpl].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
enum class TransactionStatus {
    /** [TransactionManager.TransactionImpl] is ready and able to process queries. */
    READY,

    /** [TransactionManager.TransactionImpl] is running and therefore new queries have to wait. */
    RUNNING,

    /** [TransactionManager.TransactionImpl] is committing or rolling back and therefore new queries have to wait. */
    ERROR,

    /** [TransactionManager.TransactionImpl] was marked for rollback due to an execution error. */
    FINALIZING,

    /** [TransactionManager.TransactionImpl] was rolled back. Execution not possible. */
    ROLLBACK,

    /** [TransactionManager.TransactionImpl] was committed. Execution not possible. */
    COMMIT,

    /** [TransactionManager.TransactionImpl] was killed. Execution not possible. */
    KILLED
}