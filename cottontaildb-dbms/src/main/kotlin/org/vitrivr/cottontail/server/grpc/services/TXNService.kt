package org.vitrivr.cottontail.server.grpc.services

import com.google.protobuf.Empty
import io.grpc.Status
import jetbrains.exodus.ExodusException
import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.queries.operators.physical.system.ListLocksPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.system.ListTransactionsPhysicalOperatorNode
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.TXNGrpc
import org.vitrivr.cottontail.grpc.TXNGrpcKt
import kotlin.time.ExperimentalTime

/**
 * Implementation of [TXNGrpc.TXNImplBase], the gRPC endpoint for managing [TransactionManager.TransactionImpl]s in Cottontail DB
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
@ExperimentalTime
class TXNService constructor(override val catalogue: Catalogue, override val manager: TransactionManager) : TXNGrpcKt.TXNCoroutineImplBase(), TransactionalGrpcService {
    /**
     * gRPC endpoint for beginning an new [TransactionManager.TransactionImpl].
     */
    override suspend fun begin(request: CottontailGrpc.BeginTransaction): CottontailGrpc.ResponseMetadata {
        try {
            val txn = when(request.mode) {
                CottontailGrpc.TransactionMode.READONLY -> this.manager.TransactionImpl(TransactionType.USER_READONLY)
                else -> this.manager.TransactionImpl(TransactionType.USER_EXCLUSIVE)
            }
            return CottontailGrpc.ResponseMetadata.newBuilder().setTransactionId(txn.txId).setTransactionMode(request.mode).build()
        } catch (e: ExodusException) {
            throw Status.RESOURCE_EXHAUSTED.withCause(e).withDescription("Could not start transaction. Please try again later!").asException()
        }
    }

    /**
     * gRPC for committing a [TransactionManager.TransactionImpl].
     */
    override suspend fun commit(request: CottontailGrpc.RequestMetadata): Empty {
        if (request.transactionId <= 0L)
            throw Status.INVALID_ARGUMENT.withDescription("Failed to execute COMMIT: Invalid transaction identifier ${request.transactionId }!").asException()
        val ctx = this.queryContextFromMetadata(request, false) ?: throw Status.FAILED_PRECONDITION.withDescription("Failed to execute COMMIT: Transaction ${request.transactionId} could not be resumed.").asException()
        try {
            ctx.txn.commit()
            return Empty.getDefaultInstance()
        } catch (e: TransactionException.InConflict) {
            throw Status.ABORTED.withCause(e).withDescription("[${ctx.txn.txId}, ${ctx.queryId}] Failed to execute COMMIT because transaction is in conflict.").asException()
        } catch (e: Throwable) {
            throw Status.INTERNAL.withDescription("[${ctx.txn.txId}, ${ctx.queryId}] Failed to execute COMMIT due to unexpected error: ${e.message}").asException()
        }
    }

    /**
     * gRPC for rolling back a [TransactionManager.TransactionImpl].
     */
    override suspend fun rollback(request: CottontailGrpc.RequestMetadata): Empty {
        if (request.transactionId <= 0L)
            throw Status.INVALID_ARGUMENT.withDescription("Failed to execute ROLLBACK: Invalid transaction identifier ${request.transactionId }!").asException()
        val ctx = this.queryContextFromMetadata(request, false) ?: throw Status.FAILED_PRECONDITION.withDescription("Failed to execute ROLLBACK: Transaction ${request.transactionId} could not be resumed.").asException()
        try {
            ctx.txn.rollback()
            return Empty.getDefaultInstance()
        } catch (e: Throwable) {
            throw Status.INTERNAL.withDescription("Failed to execute COMMIT due to unexpected error: ${e.message}").asException()
        }
    }

    /**
     * gRPC for killing a [TransactionManager.TransactionImpl].
     */
    override suspend fun kill(request: CottontailGrpc.RequestMetadata): Empty {
        if (request.transactionId <= 0L)
            throw Status.INVALID_ARGUMENT.withDescription("Failed to execute KILL: Invalid transaction identifier ${request.transactionId }!").asException()
        val ctx = this.queryContextFromMetadata(request, false) ?: throw Status.FAILED_PRECONDITION.withDescription("Failed to execute KILL: Transaction ${request.transactionId} could not be resumed.").asException()
        try {
            ctx.txn.kill()
            return Empty.getDefaultInstance()
        } catch (e: Throwable) {
            throw Status.INTERNAL.withDescription("Failed to execute KILL due to unexpected error: ${e.message}").asException()
        }
    }

    /**
     * gRPC for listing all [TransactionManager.TransactionImpl]s.
     */
    override fun listTransactions(request: Empty): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(CottontailGrpc.RequestMetadata.getDefaultInstance(), true) { ctx ->
        ctx.assign(ListTransactionsPhysicalOperatorNode(this.manager))
        ctx.toOperatorTree()
    }

    /**
     * gRPC for listing all active locks.
     */
    override fun listLocks(request: Empty): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(CottontailGrpc.RequestMetadata.getDefaultInstance(), true) { ctx ->
        ctx.assign(ListLocksPhysicalOperatorNode(this.manager.lockManager))
        ctx.toOperatorTree()
    }
}