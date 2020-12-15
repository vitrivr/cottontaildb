package org.vitrivr.cottontail.server.grpc.services

import com.google.protobuf.Empty
import io.grpc.Status

import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory

import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.execution.operators.system.ListTransactionsOperator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.TXNGrpc
import org.vitrivr.cottontail.model.exceptions.TransactionException
import org.vitrivr.cottontail.server.grpc.operators.SpoolerSinkOperator
import java.util.*

/**
 * Implementation of [TXNGrpc.TXNImplBase], the gRPC endpoint for managing [TransactionManager.Transaction]s
 * in Cottontail DB
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class TXNService(override val manager: TransactionManager): TXNGrpc.TXNImplBase(), TransactionService {

    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(TXNService::class.java)
    }

    /**
     * gRPC endpoint for beginning an new [TransactionManager.Transaction].
     */
    override fun begin(request: Empty, responseObserver: StreamObserver<CottontailGrpc.TransactionId>) {
        val txn = this.manager.Transaction(TransactionType.USER)
        val txId = CottontailGrpc.TransactionId.newBuilder().setValue(txn.txId).build()
        responseObserver.onNext(txId)
        responseObserver.onCompleted()
    }

    /**
     * gRPC for committing a [TransactionManager.Transaction].
     */
    override fun commit(request: CottontailGrpc.TransactionId, responseObserver: StreamObserver<Empty>) = this.withTransactionContext(request) { tx ->
        try {
            tx.commit()
            responseObserver.onCompleted()
        } catch (e: TransactionException.TransactionNotFoundException) {
            LOGGER.info(e.message)
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.message).asException())
        }
    }

    /**
     * gRPC for rolling back a [TransactionManager.Transaction].
     */
    override fun rollback(request: CottontailGrpc.TransactionId, responseObserver: StreamObserver<Empty>) = this.withTransactionContext(request) { tx ->
        try {
            tx.rollback()
            responseObserver.onCompleted()
        } catch (e: TransactionException.TransactionNotFoundException) {
            LOGGER.info(e.message)
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.message).asException())
        }
    }

    /**
     * gRPC for listing all [TransactionManager.Transaction]s.
     */
    override fun listTransactions(request: Empty, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(CottontailGrpc.TransactionId.getDefaultInstance()) function@{ tx ->
        val queryId = UUID.randomUUID().toString()
        try {
            val operator = SpoolerSinkOperator(ListTransactionsOperator(this.manager), queryId, 0, responseObserver)
            tx.execute(operator)
            responseObserver.onCompleted()
        } catch (e: TransactionException.TransactionNotFoundException) {
            LOGGER.info(e.message)
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.message).asException())
        }
    }
}