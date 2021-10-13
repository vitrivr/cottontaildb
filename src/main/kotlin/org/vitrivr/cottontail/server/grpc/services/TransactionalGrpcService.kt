package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.*
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.client.language.basics.Constants
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.locking.DeadlockException
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.extensions.toLiteral
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.ExecutionException
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource

/**
 * A facility common to all service that handle [TransactionManager.TransactionImpl]s over gRPC.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
@ExperimentalTime
internal interface TransactionalGrpcService {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(TransactionalGrpcService::class.java)
    }

    /** The [Catalogue] instance used by this [TransactionalGrpcService]. */
    val catalogue: Catalogue

    /** The [TransactionManager] instance used by this [TransactionalGrpcService]. */
    val manager: TransactionManager

    /**
     * Retrieves and returns the [TransactionContext] for the provided [CottontailGrpc.Metadata].
     *
     * @param metadata The [CottontailGrpc.Metadata] to process.
     * @return [TransactionContext]
     */
    fun transactionContext(metadata: CottontailGrpc.Metadata): TransactionManager.TransactionImpl = if (metadata.transactionId == 0L) {
        this.manager.TransactionImpl(TransactionType.USER_IMPLICIT) /* Start new transaction. */
    } else {
        val txn = this.manager[metadata.transactionId] /* Reuse existing transaction. */
        if (txn === null || txn.type !== TransactionType.USER) {
            val message = "Execution failed because transaction ${metadata.transactionId} could not be resumed."
            LOGGER.warn(message)
            throw Status.FAILED_PRECONDITION.withDescription(message).asException()
        }
        txn
    }

    /**
     * Generates and returns a new [TransactionContext] for the given [CottontailGrpc.Metadata].
     *
     * @param metadata The [CottontailGrpc.Metadata] to process.
     * @return [QueryContext]
     */
    fun queryContext(metadata: CottontailGrpc.Metadata): QueryContext = if (metadata.queryId.isNullOrEmpty()) {
        QueryContext(UUID.randomUUID().toString(), catalogue, transactionContext(metadata))
    } else {
        QueryContext(metadata.queryId, catalogue, transactionContext(metadata))
    }

    /**
     * Executes the given [Operator] and materializes the results as [CottontailGrpc.QueryResponseMessage].
     *
     * @param context The [TransactionContext] to operate in.
     * @param operator The [Operator] to execute.
     * @param queryIndex The query index.
     * @return [Flow] of [CottontailGrpc.QueryResponseMessage]
     */
    fun executeAndMaterialize(context: QueryContext, operator: Operator, queryIndex: Int = 0): Flow<CottontailGrpc.QueryResponseMessage> {
        /* Prepare columns for transmission by flow. */
        val mark = TimeSource.Monotonic.markNow()
        val columns = operator.columns.map {
            val name = CottontailGrpc.ColumnName.newBuilder().setName(it.name.simple)
            val entityName = it.name.entity()
            if (entityName != null) {
                name.setEntity(CottontailGrpc.EntityName.newBuilder().setName(entityName.simple).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(entityName.schema().simple)))
            }
            name.build()
        }

        /* Contextual information used by Flow. */
        val responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setTid(CottontailGrpc.Metadata.newBuilder().setQueryId(context.queryId).setTransactionId(context.txn.txId)).addAllColumns(columns)
        val headerSize = responseBuilder.build().serializedSize
        var accumulatedSize = headerSize
        var results = 0

        /* Execute query and transform resulting Flow. */
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
        }.catch {
            throw context.toStatusException(it)
        }.onCompletion {
            when(it) {
                null -> {
                    if (results == 0 || responseBuilder.tuplesCount > 0) emit(responseBuilder.build())
                    LOGGER.info(context.formatSuccessMessage(mark.elapsedNow()))
                }
                is CancellationException -> LOGGER.warn(context.formatErrorMessage("Operation was interrupted by user after ${mark.elapsedNow()}."))
                else -> LOGGER.error(it.message)
            }
        }
    }

    /**
     * Converts a [Record] to a [CottontailGrpc.QueryResponseMessage.Tuple]
     */
    private fun Record.toTuple(): CottontailGrpc.QueryResponseMessage.Tuple {
        val tuple = CottontailGrpc.QueryResponseMessage.Tuple.newBuilder()
        this.forEach { _, v -> tuple.addData(v?.toLiteral() ?: CottontailGrpc.Literal.newBuilder().build()) }
        return tuple.build()
    }

    /**
     * Converts the provided [Throwable] to the appropriate [StatusException] and returns it.
     *
     * @return [StatusException]
     */
    fun QueryContext.toStatusException(e: Throwable): StatusException = when (e) {
        is DatabaseException.SchemaDoesNotExistException -> Status.NOT_FOUND.withCause(e).withDescription(this.formatErrorMessage(e.message!!)).asException()
        is DatabaseException.SchemaAlreadyExistsException -> Status.ALREADY_EXISTS.withDescription(this.formatErrorMessage(e.message!!)).asException()
        is DatabaseException.EntityDoesNotExistException -> Status.NOT_FOUND.withCause(e).withDescription(this.formatErrorMessage(e.message!!)).asException()
        is DatabaseException.EntityAlreadyExistsException -> Status.ALREADY_EXISTS.withCause(e).withDescription(this.formatErrorMessage(e.message!!)).asException()
        is DatabaseException.ColumnDoesNotExistException -> Status.NOT_FOUND.withCause(e).withDescription(this.formatErrorMessage(e.message!!)).asException()
        is DatabaseException.IndexDoesNotExistException -> Status.NOT_FOUND.withCause(e).withDescription(this.formatErrorMessage(e.message!!)).asException()
        is DatabaseException.IndexAlreadyExistsException -> Status.ALREADY_EXISTS.withCause(e).withDescription(this.formatErrorMessage(e.message!!)).asException()
        is DeadlockException -> Status.ABORTED.withCause(e).withDescription(this.formatErrorMessage(e.message!!)).asException()
        is DatabaseException -> Status.INTERNAL.withCause(e).withDescription(this.formatErrorMessage(e.message!!)).asException()
        is ExecutionException -> Status.INTERNAL.withCause(e).withDescription(this.formatErrorMessage(e.message!!)).asException()
        else -> Status.UNKNOWN.withCause(e).withDescription(this.formatErrorMessage(e.message ?: "Reason unknown!")).asException()
    }

    /**
     * Formats a default success output message for this [QueryContext].
     *
     * @param duration The message to display.
     * @return [String] Formatted success message.
     */
    private fun QueryContext.formatSuccessMessage(duration: Duration) = "[${this.txn.txId}, ${this.queryId}] Execution of ${this.physical?.name} completed successfully in $duration."

    /**
     * Formats a default success output message for this [QueryContext].
     *
     * @param message The message to display.
     * @return [String] Formatted error message.
     */
    private fun QueryContext.formatErrorMessage(message: String) = "[${this.txn.txId}, ${this.queryId}] Execution of ${this.physical?.name} failed: $message"
}