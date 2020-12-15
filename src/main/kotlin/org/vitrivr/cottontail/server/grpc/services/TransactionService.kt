package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionStatus
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A facility common to all service that handle [TransactionManager.Transaction]s over gRPC.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface TransactionService {
    /** The [TransactionManager] reference for this [TransactionService]. */
    val manager: TransactionManager

    /**
     * Tries to resume the [TransactionManager.Transaction] with the [CottontailGrpc.TransactionId].
     *
     * @param txId The [CottontailGrpc.TransactionId] to resume the [TransactionManager.Transaction] for.
     * @param responseObserver [StreamObserver] that should be notified if resuming [TransactionManager.Transaction] failed.
     * @return [TransactionManager.Transaction] or null, if none exists.
     */
    fun resumeTransaction(txId: CottontailGrpc.TransactionId, responseObserver: StreamObserver<*>): TransactionManager.Transaction? {
        val txn = this.manager[txId.value]
        if (txn === null || txn.state !== TransactionStatus.OPEN) {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription("Could not resume transaction ${txId.value} because it either doesn't exist or is in wrong state.").asException())
            return null
        }
        return txn
    }
}