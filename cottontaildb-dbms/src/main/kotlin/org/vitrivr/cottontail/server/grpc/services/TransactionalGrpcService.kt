package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.transform
import org.apache.commons.lang3.builder.ReflectionToStringBuilder
import org.apache.commons.lang3.builder.ToStringStyle
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.client.language.basics.Constants
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.queries.QueryHint
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.ExecutionException
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.locking.DeadlockException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.extensions.proto
import org.vitrivr.cottontail.utilities.extensions.toLiteral
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource

/**
 * A facility common to all service that handle [TransactionManager.TransactionImpl]s over gRPC.
 *
 * @author Ralph Gasser
 * @version 1.4.0
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
     * Generates and returns a new [DefaultQueryContext] for the given [CottontailGrpc.Metadata].
     *
     * @param metadata The [CottontailGrpc.Metadata] to process.
     * @return [DefaultQueryContext]
     */
    fun queryContextFromMetadata(metadata: CottontailGrpc.Metadata): DefaultQueryContext? {
        val queryId = if (metadata.queryId.isNullOrEmpty()) {
            UUID.randomUUID().toString()
        } else {
            metadata.queryId
        }

        /* Obtain transaction context. */
        val transactionContext = if (metadata.transactionId <= 0L) {
            this.manager.TransactionImpl(TransactionType.USER_IMPLICIT) /* Start new transaction. */
        } else {
            val txn = this.manager[metadata.transactionId] /* Reuse existing transaction. */
            if (txn === null || txn.type !== TransactionType.USER) {
                return null
            }
            txn
        }

        /* Parse all the query hints provided by the user. */
//        val hints = metadata.hintList.mapNotNull {
//            when (it.hintCase) {
//                CottontailGrpc.Hint.HintCase.NOINDEXHINT -> QueryHint.NoIndex
//                CottontailGrpc.Hint.HintCase.PARALLELINDEXHINT -> QueryHint.NoParallel
//                CottontailGrpc.Hint.HintCase.POLICYHINT -> QueryHint.CostPolicy(
//                    it.policyHint.weightIo,
//                    it.policyHint.weightCpu,
//                    it.policyHint.weightMemory,
//                    it.policyHint.weightAccuracy,
//                    this.catalogue.config.cost.speedupPerWorker, /* Setting is inherited from global config. */
//                    this.catalogue.config.cost.nonParallelisableIO /* Setting is inherited from global config. */
//                )
//                CottontailGrpc.Hint.HintCase.NAMEINDEXHINT -> TODO()
//                else -> null
//            }
//        }.toSet()

        val hints = mutableSetOf<QueryHint>()

        //TODO parse new hint format

        return DefaultQueryContext(queryId, this.catalogue, transactionContext, hints)
    }

    /**
     * Prepares and executes a query using the [DefaultQueryContext] specified by the [CottontailGrpc.Metadata] object.
     *
     * @param metadata The [CottontailGrpc.Metadata] that identifies the [DefaultQueryContext]
     * @param prepare The action that prepares the query [Operator]
     * @return [Flow] of [CottontailGrpc.QueryResponseMessage]
     */
    fun prepareAndExecute(metadata: CottontailGrpc.Metadata, prepare: (ctx: DefaultQueryContext) -> Operator): Flow<CottontailGrpc.QueryResponseMessage> {
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
                    try {
                        if (context.txn.type.autoCommit) {
                            context.txn.commit() /* Handle auto-commit. */
                        }
                        LOGGER.info("[${context.txn.txId}, ${context.queryId}] Execution of ${context.physical?.name} completed successfully in ${m2.elapsedNow()}.")
                    } catch (e: Throwable) {
                        val wrapped = context.toStatusException(e, true)
                        LOGGER.error("[${context.txn.txId}, ${context.queryId}] Execution of ${context.physical?.name} failed: ${wrapped.message}")
                        throw wrapped
                    }
                } else {
                    val wrapped = context.toStatusException(it, true)
                    if (context.txn.type.autoRollback) context.txn.rollback() /* Handle auto-rollback. */
                    LOGGER.error("[${context.txn.txId}, ${context.queryId}] Execution of ${context.physical?.name} failed: ${wrapped.message}")
                    throw wrapped
                }
            }
        } catch (e: Throwable) {
            LOGGER.error("[${context.txn.txId}, ${context.queryId}] Preparation of query ${ReflectionToStringBuilder.toString(context, ToStringStyle.JSON_STYLE)} failed: ${e.message}")
            e.printStackTrace()
            if (context.txn.type.autoRollback) context.txn.rollback() /* Handle auto-rollback. */
            return flow { throw context.toStatusException(e, false) }
        }
    }

    /**
     *  Converts the provided [Throwable] to a [StatusException] that can be returned to the caller. The
     *  exception will contain all the information about this [DefaultQueryContext].
     *
     *  @param e The [Throwable] to convert.
     *  @param execution Flag indicating whether error occured during execution.
     */
    fun DefaultQueryContext.toStatusException(e: Throwable, execution: Boolean): StatusException {
        val text = if (execution) {
            "[${this.txn.txId}, ${this.queryId}] Execution of ${this.physical?.name} query failed: ${e.message}"
        } else {
            "[${this.txn.txId}, ${this.queryId}] Preparation of query failed: ${e.message}"
        }
        return when (e) {
            is DatabaseException.SchemaDoesNotExistException,
            is DatabaseException.EntityDoesNotExistException,
            is DatabaseException.ColumnDoesNotExistException,
            is DatabaseException.IndexDoesNotExistException -> Status.NOT_FOUND.withCause(e)
            is DatabaseException.SchemaAlreadyExistsException,
            is DatabaseException.EntityAlreadyExistsException,
            is DatabaseException.IndexAlreadyExistsException -> Status.ALREADY_EXISTS.withCause(e)
            is DatabaseException.NoColumnException,
            is DatabaseException.DuplicateColumnException,
            is DatabaseException.ValidationException -> Status.INVALID_ARGUMENT.withCause(e)
            is DeadlockException,
            is TransactionException.InConflict -> Status.ABORTED.withCause(e)
            is ExecutionException,
            is DatabaseException -> Status.INTERNAL.withCause(e)
            is CancellationException -> Status.CANCELLED.withCause(e)
            else -> Status.UNKNOWN.withCause(e)
        }.withDescription(text).asException()
    }

    /**
     * Converts a [Record] to a [CottontailGrpc.QueryResponseMessage.Tuple]
     */
    private fun Record.toTuple(): CottontailGrpc.QueryResponseMessage.Tuple {
        val tuple = CottontailGrpc.QueryResponseMessage.Tuple.newBuilder()
        for (i in 0 until this.size) {
            tuple.addData(this[i]?.toLiteral() ?: CottontailGrpc.Literal.newBuilder().build())
        }
        return tuple.build()
    }
}