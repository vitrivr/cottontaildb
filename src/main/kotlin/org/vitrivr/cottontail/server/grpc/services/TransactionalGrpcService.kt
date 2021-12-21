package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.transform
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.client.language.basics.Constants
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.locking.DeadlockException
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.extensions.proto
import org.vitrivr.cottontail.database.queries.binding.extensions.toLiteral
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.ExecutionException
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource

/**
 * A facility common to all service that handle [TransactionManager.TransactionImpl]s over gRPC.
 *
 * @author Ralph Gasser
 * @version 1.3.1
 */
@ExperimentalTime
internal interface TransactionalGrpcService {

    companion object {
        val LOGGER = LoggerFactory.getLogger(TransactionalGrpcService::class.java)
    }

    /** The [Catalogue] instance used by this [TransactionalGrpcService]. */
    val catalogue: Catalogue

    /** The [TransactionManager] instance used by this [TransactionalGrpcService]. */
    val manager: TransactionManager

    /**
     * Generates and returns a new [QueryContext] for the given [CottontailGrpc.Metadata].
     *
     * @param metadata The [CottontailGrpc.Metadata] to process.
     * @return [QueryContext]
     */
    fun queryContextFromMetadata(metadata: CottontailGrpc.Metadata): QueryContext? {
        val queryId = if (metadata.queryId.isNullOrEmpty()) {
            UUID.randomUUID().toString()
        } else {
            metadata.queryId
        }
        val transactionContext = if (metadata.transactionId <= 0L) {
            this.manager.TransactionImpl(TransactionType.USER_IMPLICIT) /* Start new transaction. */
        } else {
            val txn = this.manager[metadata.transactionId] /* Reuse existing transaction. */
            if (txn === null || txn.type !== TransactionType.USER) {
                return null
            }
            txn
        }
       return QueryContext(queryId, catalogue, transactionContext)
    }

    /**
     * Prepares and executes a query using the [QueryContext] specified by the [CottontailGrpc.Metadata] object.
     *
     * @param metadata The [CottontailGrpc.Metadata] that identifies the [QueryContext]
     * @param prepare The action that prepares the query [Operator]
     * @return [Flow] of [CottontailGrpc.QueryResponseMessage]
     */
    fun prepareAndExecute(metadata: CottontailGrpc.Metadata, prepare: (ctx: QueryContext) -> Operator): Flow<CottontailGrpc.QueryResponseMessage> {
        /* Phase 1a: Obtain query context. */
        val m1 = TimeSource.Monotonic.markNow()
        val context = this.queryContextFromMetadata(metadata) ?: return flow {
            val message = "Execution failed because transaction ${metadata.transactionId} could not be resumed."
            LOGGER.warn(message)
            throw Status.FAILED_PRECONDITION.withDescription(message).asException()
        }

        try {
            /* Phase 1b: Obtain operator by means of query parsing, binding and planning. */
            val operator = prepare(context)
            m1.elapsedNow()
            LOGGER.debug("[${context.txn.txId}, ${context.queryId}] Preparation of ${context.physical?.name} completed successfully in ${m1.elapsedNow()}.")

            /* Phase 2a: Build query response message. */
            val m2 = TimeSource.Monotonic.markNow()
            val responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setMetadata(CottontailGrpc.Metadata.newBuilder().setQueryId(context.queryId).setTransactionId(context.txn.txId))
            for (c in operator.columns) {
                val builder = responseBuilder.addColumnsBuilder()
                builder.name = c.name.proto()
                builder.nullable = c.nullable
                builder.primary = c.primary
                builder.type = c.type.proto()
            }

            /* Contextual information used by Flow. */
            val headerSize = responseBuilder.build().serializedSize
            var accumulatedSize = headerSize
            var results = 0

            /* Phase 2b: Execute query and stream back results. */
            return context.txn.execute(operator).transform<Record, CottontailGrpc.QueryResponseMessage> {
                val tuple = it.toTuple()
                results += 1
                if (accumulatedSize + tuple.serializedSize >= Constants.MAX_PAGE_SIZE_BYTES) {
                    emit(responseBuilder.build())
                    responseBuilder.clearTuples()
                    accumulatedSize = headerSize
                }

                /* Add entry to page and increment counter. */
                responseBuilder.addTuples(tuple)
                accumulatedSize += tuple.serializedSize
            }.onCompletion {
                if (it == null) {
                    if (results == 0 || responseBuilder.tuplesCount > 0) emit(responseBuilder.build()) /* Emit final response. */
                    if (context.txn.type.autoCommit) context.txn.commit() /* Handle auto-commit. */
                    LOGGER.info("[${context.txn.txId}, ${context.queryId}] Execution of ${context.physical?.name} completed successfully in ${m2.elapsedNow()}.")
                } else {
                    val e = context.toStatusException(it)
                    if (context.txn.type.autoRollback) context.txn.rollback() /* Handle auto-rollback. */
                    LOGGER.error("[${context.txn.txId}, ${context.queryId}] Execution of ${context.physical?.name} failed: ${e.message}")
                    throw e
                }
            }
        } catch (e: Throwable) {
            LOGGER.error("[${context.txn.txId}, ${context.queryId}] Preparation of ${context.physical?.name} failed: ${e.message}")
            if (context.txn.type.autoRollback) context.txn.rollback() /* Handle auto-rollback. */
            return flow { throw context.toStatusException(e) }
        }
    }

    fun QueryContext.toStatusException(e: Throwable): StatusException = when (e) {
        is DatabaseException.SchemaDoesNotExistException,
        is DatabaseException.EntityDoesNotExistException,
        is DatabaseException.ColumnDoesNotExistException,
        is DatabaseException.IndexDoesNotExistException -> Status.NOT_FOUND.withCause(e)
        is DatabaseException.SchemaAlreadyExistsException,
        is DatabaseException.EntityAlreadyExistsException,
        is DatabaseException.IndexAlreadyExistsException -> Status.ALREADY_EXISTS.withCause(e)
        is DeadlockException -> Status.ABORTED.withCause(e)
        is ExecutionException,
        is DatabaseException -> Status.INTERNAL.withCause(e)
        is CancellationException -> Status.CANCELLED.withCause(e)
        else -> Status.UNKNOWN.withCause(e)
    }.withDescription("[${this.txn.txId}, ${this.queryId}] Execution of ${this.physical?.name} failed: ${e.message}").asException()

    /**
     * Converts a [Record] to a [CottontailGrpc.QueryResponseMessage.Tuple]
     */
    private fun Record.toTuple(): CottontailGrpc.QueryResponseMessage.Tuple {
        val tuple = CottontailGrpc.QueryResponseMessage.Tuple.newBuilder()
        this.forEach { _, v -> tuple.addData(v?.toLiteral() ?: CottontailGrpc.Literal.newBuilder().build()) }
        return tuple.build()
    }
}