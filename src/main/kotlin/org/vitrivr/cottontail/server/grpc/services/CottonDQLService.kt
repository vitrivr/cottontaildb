package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.ExecutionPlan
import org.vitrivr.cottontail.execution.tasks.ExecutionPlanException
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.server.grpc.helper.DataHelper
import org.vitrivr.cottontail.server.grpc.helper.GrpcQueryBinder
import org.vitrivr.cottontail.utilities.math.BitUtil
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

/**
 * Implementation of [CottonDQLGrpc.CottonDQLImplBase], the gRPC endpoint for quering data in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.2
 */
@ExperimentalTime
class CottonDQLService(val catalogue: Catalogue, val engine: ExecutionEngine) : CottonDQLGrpc.CottonDQLImplBase() {


    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(CottonDQLService::class.java)
    }

    /** [GrpcQueryBinder] used to generate logical [NodeExpression] tree from a gRPC query. */
    private val binder = GrpcQueryBinder(catalogue = this@CottonDQLService.catalogue)

    /** [ExecutionPlanFactor] used to generate [ExecutionPlan]s from query definitions. */
    private val planner = CottontailQueryPlanner()

    /**
     * gRPC endpoint for handling simple queries.
     */
    override fun query(request: CottontailGrpc.QueryMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = try {
        /* Start the query by giving the start signal. */
        val queryId = if (request.queryId == null || request.queryId == "") {
            UUID.randomUUID().toString()
        } else {
            request.queryId
        }

        val totalDuration = measureTime {
            /* Bind query and create logical plan. */
            val bindTimedValue = measureTimedValue {
                this.binder.parseAndBind(request.query)
            }
            LOGGER.trace("Parsing & binding query $queryId took ${bindTimedValue.duration}.")

            /* Plan query and create execution plan. */
            val planTimedValue = measureTimedValue {
                val candidates = this.planner.optimize(bindTimedValue.value, 3, 3)
                val plan = this.engine.newExecutionPlan()
                plan.compileStage(candidates.minBy { it.totalCost }!!.toStage(QueryPlannerContext(this.engine.availableThreads)))
                plan
            }
            LOGGER.trace("Planning query $queryId took ${planTimedValue.duration}.")

            /* Execute query. */
            val resultsTimedValue = measureTimedValue {
                planTimedValue.value.execute()
            }
            LOGGER.trace("Executing query $queryId took ${resultsTimedValue.duration}.")

            /* Send back results. */
            this.spoolResults(queryId, resultsTimedValue.value, responseObserver)

            /* Complete query. */
            responseObserver.onCompleted()
        }

        LOGGER.info("Query $queryId took $totalDuration.")
    } catch (e: QueryException.QuerySyntaxException) {
        LOGGER.error("Error while executing query $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Query syntax is invalid: ${e.message}").asException())
    } catch (e: QueryException.QueryBindException) {
        LOGGER.error("Error while executing query $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Query binding failed: ${e.message}").asException())
    } catch (e: ExecutionPlanException) {
        LOGGER.error("Error while executing query $request", e)
        responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed because execution engine signaled an error: ${e.message}").asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while executing query $request", e)
        responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed because of a database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while executing query $request", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Query execution failed failed because of a unknown error: ${e.message}").asException())
    }

    /**
     *  gRPC endpoint for handling batched queries.
     */
    override fun batchedQuery(request: CottontailGrpc.BatchedQueryMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = try {
        /* Start the query by giving the start signal. */
        val queryId = if (request.queryId == null || request.queryId == "") {
            UUID.randomUUID().toString()
        } else {
            request.queryId
        }

        val totalDuration = measureTime {
            request.queriesList.forEachIndexed { index, query ->
                /* Bind query and create logical plan. */
                val bindTimedValue = measureTimedValue {
                    this.binder.parseAndBind(query)
                }
                LOGGER.trace("Parsing & binding query: $queryId, index: $index took ${bindTimedValue.duration}.")

                /* Plan query and create execution plan. */
                val planTimedValue = measureTimedValue {
                    val candidates = this.planner.optimize(bindTimedValue.value, 3, 3)
                    val plan = this.engine.newExecutionPlan()
                    plan.compileStage(candidates.minBy { it.totalCost }!!.toStage(QueryPlannerContext(this.engine.availableThreads)))
                    plan
                }
                LOGGER.trace("Planning query: $queryId, index: $index took ${planTimedValue.duration}.")

                /* Execute query. */
                val resultsTimedValue = measureTimedValue {
                    planTimedValue.value.execute()
                }
                LOGGER.trace("Executing query: $queryId, index: $index took ${resultsTimedValue.duration}.")

                /* Send back results. */
                this.spoolResults(queryId, resultsTimedValue.value, responseObserver, index)
            }

            /* Send onCompleted() signal. */
            responseObserver.onCompleted()
        }

        /* Complete query. */
        LOGGER.info("Batched query $queryId took $totalDuration to complete.")
    } catch (e: QueryException.QuerySyntaxException) {
        LOGGER.error("Error while executing batched query $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Query syntax is invalid: ${e.message}").asException())
    } catch (e: QueryException.QueryBindException) {
        LOGGER.error("Error while executing batched query $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Query binding failed: ${e.message}").asException())
    } catch (e: ExecutionPlanException) {
        LOGGER.error("Error while executing batched query $request", e)
        responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed because execution engine signaled an error: ${e.message}").asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while executing batched query $request", e)
        responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed because of a database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while executing batched query $request", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Query execution failed failed because of a unknown error: ${e.message}").asException())
    }

    /**
     * gRPC endpoint for handling PING requests.
     */
    override fun ping(request: CottontailGrpc.Empty, responseObserver: StreamObserver<CottontailGrpc.SuccessStatus>) {
        responseObserver.onNext(CottontailGrpc.SuccessStatus.newBuilder().setTimestamp(System.currentTimeMillis()).build())
        responseObserver.onCompleted()
    }

    /**
     * Spools the results in the given [Recordset] to gRPC
     *
     * @param queryId The ID of the query.
     * @param results [Recordset] containing the results.
     * @param responseObserver [StreamObserver] used to send back the results.
     * @param index Optional index of the result (for batched queries).
     */
    private fun spoolResults(queryId: String, results: Recordset, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>, index: Int = 0) {
        if (results.rowCount > 0) {
            val first = results.first()!!
            val exampleSize = BitUtil.nextPowerOfTwo(recordToTuple(first).build().serializedSize)
            val pageSize = (4_194_000_000 / exampleSize).toInt()
            val maxPages = Math.floorDiv(results.rowCount, pageSize)

            /* Return results. */
            val duration = measureTime {
                val iterator = results.iterator()
                for (i in 0..maxPages) {
                    val responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setPageSize(pageSize).setPage(i).setMaxPage(maxPages).setTotalHits(results.rowCount)
                    for (j in i * pageSize until kotlin.math.min(results.rowCount, (i * pageSize + pageSize))) {
                        responseBuilder.addResults(recordToTuple(iterator.next()))
                    }
                    responseObserver.onNext(responseBuilder.build())
                }
            }

            LOGGER.trace("Sending back ${results.rowCount} rows for position $index of query $queryId took $duration.")
        } else {
            responseObserver.onNext(CottontailGrpc.QueryResponseMessage.newBuilder().setPage(0).setMaxPage(0).setTotalHits(0).setPageSize(0).setQueryId(queryId).build())
            LOGGER.trace("Position $index of query $queryId yielded no results.")
        }
    }

    /**
     * Generates a new [CottontailGrpc.Tuple.Builder] from a given [Record].
     *
     * @param record [Record] to create a [CottontailGrpc.Tuple.Builder] from.
     * @return Resulting [CottontailGrpc.Tuple.Builder]
     */
    private fun recordToTuple(record: Record): CottontailGrpc.Tuple.Builder = CottontailGrpc.Tuple.newBuilder().putAllData(record.toMap().map { it.key.toString() to DataHelper.toData(it.value) }.toMap())
}
