package org.vitrivr.cottontail.server.grpc.services

import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.locking.DeadlockException
import org.vitrivr.cottontail.database.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.database.queries.planning.rules.logical.DeferredFetchAfterFilterRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.logical.DeferredFetchAfterKnnRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.logical.LeftConjunctionRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.logical.RightConjunctionRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation.*
import org.vitrivr.cottontail.database.queries.planning.rules.physical.index.KnnIndexScanRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.pushdown.CountPushdownRule
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.exceptions.ExecutionException
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.exceptions.TransactionException
import org.vitrivr.cottontail.server.grpc.helper.GrpcQueryBinder
import org.vitrivr.cottontail.server.grpc.operators.SpoolerSinkOperator
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

/**
 * Implementation of [DQLGrpc.DQLImplBase], the gRPC endpoint for querying data in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
@ExperimentalTime
class DQLService(val catalogue: Catalogue, override val manager: TransactionManager) : DQLGrpc.DQLImplBase(), TransactionService {

    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(DQLService::class.java)
    }

    /** [GrpcQueryBinder] used to generate [org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression] tree from a gRPC query. */
    private val binder = GrpcQueryBinder(catalogue = this@DQLService.catalogue)

    /** [CottontailQueryPlanner] used to generate execution plans from query definitions. */
    private val planner = CottontailQueryPlanner(
        logicalRewriteRules = listOf(
            LeftConjunctionRewriteRule,
            RightConjunctionRewriteRule,
            DeferredFetchAfterFilterRewriteRule,
            DeferredFetchAfterKnnRewriteRule
        ),
        physicalRewriteRules = listOf(
            KnnIndexScanRule,
            CountPushdownRule,
            EntityScanImplementationRule,
            FilterImplementationRule,
            KnnImplementationRule,
            LimitImplementationRule,
            ProjectionImplementationRule,
            FetchImplementationRule
        )
    )

    /**
     * gRPC endpoint for executing queries.
     */
    override fun query(request: CottontailGrpc.QueryMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId) function@{ tx ->
        /* Determine query ID. */
        val queryId = request.queryId.ifBlank { UUID.randomUUID().toString() }

        try {
            /* Start query execution. */
            val totalDuration = measureTime {
                /* Bind query and create logical plan. */
                val bindTimedValue = measureTimedValue {
                    this.binder.parseAndBindQuery(request.query, tx)
                }
                LOGGER.trace("Parsing & binding query $queryId took ${bindTimedValue.duration}.")

                /* Plan query and create execution plan. */
                val planTimedValue = measureTimedValue {
                    val candidates = this.planner.plan(bindTimedValue.value)
                    if (candidates.isEmpty()) {
                        responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed because no valid execution plan could be produced").asException())
                        return@function
                    }
                    val operator = candidates.minByOrNull { it.totalCost }!!.toOperator(this.manager)
                    SpoolerSinkOperator(operator, queryId, 0, responseObserver)
                }
                LOGGER.trace("Planning query $queryId took ${planTimedValue.duration}.")

                /* Execute query in transaction context. */
                tx.execute(planTimedValue.value)
            }

            /* Finalize transaction. */
            LOGGER.trace("Executing query $queryId took $totalDuration to complete.")
            responseObserver.onCompleted()
        } catch (e: TransactionException.TransactionNotFoundException) {
            val message = "Could not execute query $queryId because transaction ${e.txId} could not be resumed."
            LOGGER.info(message)
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(message).asException())
        } catch (e: QueryException.QuerySyntaxException) {
            val message = "Could not execute query $queryId due to syntax error: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
        } catch (e: QueryException.QueryBindException) {
            val message = "Could not execute query $queryId because DBO could not be found: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
        } catch (e: DeadlockException) {
            val message = "Could not execute query $queryId due to deadlock with other transaction: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.ABORTED.withDescription(message).asException())
        } catch (e: ExecutionException) {
            val message = "Could not execute query $queryId due to an unhandled execution error: ${e.message}"
            LOGGER.error(message, e)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
        } catch (e: Throwable) {
            val message = "Could not execute query $queryId due to an unhandled error: ${e.message}"
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).asException())
        }
    }

    /**
     * gRPC endpoint for explaining queries.
     */
    override fun explain(request: CottontailGrpc.QueryMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId) function@{ tx ->
        /* Determine query ID. */
        val queryId = request.queryId.ifBlank { UUID.randomUUID().toString() }

        try {
            /* Start query execution. */
            val totalDuration = measureTime {
                /* Bind query and create logical plan. */
                val bindTimedValue = measureTimedValue {
                    this.binder.parseAndBindQuery(request.query, tx)
                }
                LOGGER.trace("Parsing & binding query $queryId took ${bindTimedValue.duration}.")

                /* Plan query and create execution plan. */
                val planTimedValue = measureTimedValue {
                    val candidates = this.planner.plan(bindTimedValue.value)
                    if (candidates.isEmpty()) {
                        responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed because no valid execution plan could be produced").asException())
                        return@function
                    }

                    /* Select candidate with lowest cos. */
                    val selected = candidates.minByOrNull { it.totalCost }!!

                    /* ToDo: Return explanation. */

                }
                LOGGER.trace("Planning query $queryId took ${planTimedValue.duration}.")
            }

            LOGGER.trace("Explaining query $queryId took $totalDuration to complete.")
            responseObserver.onCompleted()
        } catch (e: TransactionException.TransactionNotFoundException) {
            val message = "Could not explain query $queryId because transaction ${e.txId} could not be resumed."
            LOGGER.info(message)
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(message).asException())
        } catch (e: QueryException.QuerySyntaxException) {
            val message = "Could not explain query $queryId due to syntax error: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
        } catch (e: QueryException.QueryBindException) {
            val message = "Could not explain query $queryId because DBO could not be found: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
        } catch (e: DeadlockException) {
            val message = "Could not explain query $queryId due to deadlock with other transaction: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.ABORTED.withDescription(message).asException())
        } catch (e: ExecutionException) {
            val message = "Could not explain query $queryId due to an unhandled execution error: ${e.message}"
            LOGGER.error(message, e)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
        } catch (e: Throwable) {
            val message = "Could not explain query $queryId due to an unhandled error: ${e.message}"
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).asException())
        }
    }

    /**
     * gRPC endpoint for handling PING requests.
     */
    override fun ping(request: Empty, responseObserver: StreamObserver<Empty>) {
        responseObserver.onCompleted()
    }
}
