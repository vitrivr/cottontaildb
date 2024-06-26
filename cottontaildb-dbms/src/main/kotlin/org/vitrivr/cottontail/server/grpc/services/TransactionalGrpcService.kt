package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.StatusException
import jetbrains.exodus.ExodusException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.client.language.basics.Constants
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.proto
import org.vitrivr.cottontail.core.toTuple
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.ExecutionException
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.locking.DeadlockException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.QueryHint
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource

/**
 * A facility common to all service that handle [TransactionManager.AbstractTransaction]s over gRPC.
 *
 * @author Ralph Gasser
 * @version 1.5.0
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
        get() = this.catalogue.transactionManager

    /**
     * Generates and returns a new [DefaultQueryContext] for the given [CottontailGrpc.RequestMetadata].
     *
     * @param metadata The [CottontailGrpc.RequestMetadata] to process.
     * @param readOnly Flag indicating whether the query that requested the [DefaultQueryContext] is a readonly query.
     * @return [DefaultQueryContext]
     */
    fun queryContextFromMetadata(metadata: CottontailGrpc.RequestMetadata, readOnly: Boolean): DefaultQueryContext {
        val queryId = if (metadata.queryId.isNullOrEmpty()) {
            UUID.randomUUID().toString()
        } else {
            metadata.queryId
        }

        /* Parse all the query hints provided by the user. */
        val hints = mutableSetOf<QueryHint>()
        if (metadata.noOptimiseHint) {
            hints.add(QueryHint.NoOptimisation)
        }
        if (metadata.hasParallelHint()) {
            hints.add(QueryHint.Parallelism(metadata.parallelHint.limit))
        }
        if (metadata.hasIndexHint()) {
            if (metadata.indexHint.hasDisallow()) {
                hints.add(QueryHint.IndexHint.None)
            } else if (metadata.indexHint.hasName()) {
                hints.add(QueryHint.IndexHint.Name(metadata.indexHint.name))
            } else if (metadata.indexHint.hasType()) {
                hints.add(QueryHint.IndexHint.Type(metadata.indexHint.type.let { IndexType.valueOf(it.name) }))
            }
        }
        if (metadata.hasPolicyHint()) {
            hints.add(QueryHint.CostPolicy(
                metadata.policyHint.weightIo,
                metadata.policyHint.weightCpu,
                metadata.policyHint.weightMemory,
                metadata.policyHint.weightAccuracy,
                this.catalogue.config.cost.speedupPerWorker, /* Setting inherited from global config. */
                this.catalogue.config.cost.parallelisableIO /* Setting inherited from global config. */
            ))
        }

        /* Obtain transaction context. */
        val transactionContext = if (metadata.transactionId <= 0L) {
            if (readOnly) { /* Start new transaction. */
                this.manager.startTransaction(TransactionType.USER_IMPLICIT_READONLY)
            } else {
                this.manager.startTransaction(TransactionType.USER_IMPLICIT_EXCLUSIVE)
            }
        } else { /* Reuse existing transaction. */
            val txn = this.manager[metadata.transactionId]
            if (txn === null || txn.type.autoCommit) {
                throw Status.FAILED_PRECONDITION.withDescription( "Execution failed because transaction ${metadata.transactionId} could not be resumed because it doesn't exist or has the wrong type.").asException()
            }
            txn
        }

        /* Return query context. */
        return DefaultQueryContext(queryId, this.catalogue, transactionContext, hints)
    }

    /**
     * Prepares and executes a query using the [DefaultQueryContext] specified by the [CottontailGrpc.RequestMetadata] object.
     *
     * @param metadata The [CottontailGrpc.RequestMetadata] that identifies the [DefaultQueryContext]
     * @param readOnly Flag indicating, whether the query prepared is a read-only query.
     * @param prepare The action that prepares the query [Operator]
     * @return [Flow] of [CottontailGrpc.QueryResponseMessage]
     */
    fun prepareAndExecute(metadata: CottontailGrpc.RequestMetadata, readOnly: Boolean, prepare: (ctx: DefaultQueryContext) -> Operator): Flow<CottontailGrpc.QueryResponseMessage> = flow {
        /* Phase 1a: Obtain query context. */
        val m1 = TimeSource.Monotonic.markNow()
        val context = try {
            this@TransactionalGrpcService.queryContextFromMetadata(metadata, readOnly)
        } catch (e: ExodusException) {
            throw Status.RESOURCE_EXHAUSTED.withCause(e).withDescription("Could not start transaction due to resource exhaustion. Please try again later!").asException()
        } catch (e: Throwable) {
            throw Status.INTERNAL.withCause(e).withDescription("Could not start transaction due to an unknown error!").asException()
        }

        /* Phase 1b: Obtain operator by means of query parsing, binding and planning. */
        val (operator, planDuration) = try {
            val p = prepare(context)
            val d = m1.elapsedNow()
            LOGGER.debug("[${context.txn.transactionId}, ${context.queryId}] Preparation of ${context.physical.firstOrNull()?.name} completed successfully in $d.")
            p to d
        }  catch (e: Throwable) {
            /* Rollback transaction */
            if (context.txn.type.autoRollback) {
                context.txn.rollback()
            }

            /* Handle error. */
            throw context.handleError(e, false)
        }

        /* Phase 2a: Build query response message. */
        val m2 = TimeSource.Monotonic.markNow()
        val responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder()
            .setMetadata(CottontailGrpc.ResponseMetadata.newBuilder()
                .setQueryId(context.queryId)
                .setTransactionId(context.txn.transactionId)
                .setQueryDuration(0L)
                .setPlanDuration(planDuration.toLong(DurationUnit.MILLISECONDS))
            )

        for (c in operator.columns) {
            val builder = responseBuilder.addColumnsBuilder()
            builder.name = c.name.proto()
            builder.nullable = c.nullable
            builder.primary = c.primary
            builder.type = c.type.proto()
            builder.length = c.type.logicalSize
        }

        /* Contextual information used by Flow. */
        val headerSize = responseBuilder.build().serializedSize
        var accumulatedSize = headerSize
        var results = 0

        /* Phase 2b: Execute query and stream back results. */
        context.txn.execute(operator).onCompletion {
            /* Handle potential error & associated auto-rollback (if applicable). */
            if (it != null) {
                if (context.txn.type.autoRollback) {
                    context.txn.rollback()
                }
                throw context.handleError(it, true)
            }

            /* Handle auto-commit (if applicable)*/
            if (context.txn.type.autoCommit) {
                try {
                    context.txn.commit()
                }  catch (e: Throwable) {
                    context.txn.rollback()
                    throw context.handleError(e, true)
                }
            }

            /* Flush remaining tuples (final transmission). */
            if (results == 0 || responseBuilder.tuplesCount > 0) {
                responseBuilder.metadataBuilder.planDuration = m2.elapsedNow().toLong(DurationUnit.MILLISECONDS)
                emit(responseBuilder.build())
            }

            /* Debug message. */
            LOGGER.info("[{}, {}] Execution of {} completed successfully in {}.", context.txn.transactionId, context.queryId, context.physical.firstOrNull()?.name, m2.elapsedNow())
        }.collect {
            val tuple = it.toTuple()
            val tupleSize = tuple.serializedSize
            if (accumulatedSize + tupleSize >= Constants.MAX_PAGE_SIZE_BYTES) {
                responseBuilder.metadataBuilder.planDuration = m2.elapsedNow().toLong(DurationUnit.MILLISECONDS) /* Query duration is, re-evaluated for every batch. */
                emit(responseBuilder.build())
                responseBuilder.clearTuples()
                accumulatedSize = headerSize
            }

            /* Add entry to page and increment counter. */
            responseBuilder.addTuples(tuple)
            accumulatedSize += tupleSize
            results += 1
        }
    }

    /**
     *  Converts the provided [Throwable] to a [StatusException] that can be returned to the caller. The
     *  exception will contain all the information about this [DefaultQueryContext].
     *
     *  @param e The [Throwable] to convert.
     *  @param execution Flag indicating whether error occurred during execution.
     */
    fun DefaultQueryContext.handleError(e: Throwable, execution: Boolean): StatusException {
        val text = if (execution) {
            "[${this.txn.transactionId}, ${this.queryId}] Execution of ${this.physical.firstOrNull()?.name} query failed: ${e.message}"
        } else {
            "[${this.txn.transactionId}, ${this.queryId}] Preparation of query failed: ${e.message}"
        }

        /* Log error. */
        LOGGER.error(text)
        LOGGER.error(e.stackTraceToString())

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
}