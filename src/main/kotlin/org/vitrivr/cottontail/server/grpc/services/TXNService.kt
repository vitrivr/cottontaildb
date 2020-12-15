package org.vitrivr.cottontail.server.grpc.services

import com.google.protobuf.Empty

import io.grpc.stub.StreamObserver

import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.TXNGrpc

/**
 * Implementation of [TXNGrpc.TXNImplBase], the gRPC endpoint for managing [TransactionManager.Transaction]s
 * in Cottontail DB
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class TXNService(override val manager: TransactionManager): TXNGrpc.TXNImplBase(), TransactionService {
    /**
     * gRPC endpoint for beginning an new [TransactionManager.Transaction].
     */
    override fun begin(request: Empty, responseObserver: StreamObserver<CottontailGrpc.TransactionId>) {
        val txn = this.manager.Transaction()
        val txId = CottontailGrpc.TransactionId.newBuilder().setValue(txn.txId).build()
        responseObserver.onNext(txId)
        responseObserver.onCompleted()
    }

    /**
     * gRPC for committing a [TransactionManager.Transaction].
     */
    override fun commit(request: CottontailGrpc.TransactionId, responseObserver: StreamObserver<Empty>) {
        val txn = this.resumeTransaction(request, responseObserver) ?: return
        txn.commit()
        responseObserver.onCompleted()
    }

    /**
     * gRPC for rolling back a [TransactionManager.Transaction].
     */
    override fun rollback(request: CottontailGrpc.TransactionId, responseObserver: StreamObserver<Empty>) {
        val txn = this.resumeTransaction(request, responseObserver) ?: return
        txn.rollback()
        responseObserver.onCompleted()
    }

}