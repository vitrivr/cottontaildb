package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.tasks.ExecutionPlanException
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.server.grpc.helper.DataHelper
import org.vitrivr.cottontail.server.grpc.helper.GrpcQueryBinder
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

/**
 * Implementation of [CottonDQLGrpc.CottonDQLImplBase], the gRPC endpoint for quering data in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
@ExperimentalTime
class CottonDQLService(val catalogue: Catalogue, val engine: ExecutionEngine) : CottonDQLGrpc.CottonDQLImplBase() {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(CottonDQLService::class.java)
    }

    /**
     * gRPC endpoint for handling simple queries.
     */
    override fun query(request: CottontailGrpc.QueryMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResultMessage>) = try {
        /* Start the query by giving the start signal. */
        val queryId = if (request.queryId == null || request.queryId == "") {
            UUID.randomUUID().toString()
        } else {
            request.queryId
        }

        /* Bind query and generate execution plan */
        val totalDuration = measureTime {
            /* Bind query and generate execution plan */
            val planTimedValue = measureTimedValue {
                val binder = GrpcQueryBinder(catalogue = this@CottonDQLService.catalogue, engine = this@CottonDQLService.engine)
                binder.parseAndBind(request.query)
            }
            LOGGER.trace("Parsing & binding query $queryId took ${planTimedValue.duration}.")

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
    override fun batchedQuery(request: CottontailGrpc.BatchedQueryMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResultMessage>) = try {
        /* Start the query by giving the start signal. */
        val queryId = if (request.queryId == null || request.queryId == "") {
            UUID.randomUUID().toString()
        } else {
            request.queryId
        }

        val totalDuration = measureTime {
            request.queriesList.forEachIndexed { index, query ->
                /* Bind query and generate execution plan */
                val planTimedValue = measureTimedValue {
                    val binder = GrpcQueryBinder(catalogue = this@CottonDQLService.catalogue, engine = this@CottonDQLService.engine)
                    binder.parseAndBind(query)
                }
                LOGGER.trace("Parsing & binding query $index of batch $queryId took ${planTimedValue.duration}.")

                /* Execute query. */
                val resultsTimedValue = measureTimedValue {
                    planTimedValue.value.execute()
                }
                LOGGER.trace("Executing query $index of batch $queryId took ${resultsTimedValue.duration}.")

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
    private fun spoolResults(queryId: String, results: Recordset, responseObserver: StreamObserver<CottontailGrpc.QueryResultMessage>, index: Int = 0) {
        if (results.rowCount > 0) {
            val duration = measureTime {
                /* Create template for QueryResultMessage. */
                val template = CottontailGrpc.QueryResultMessage.newBuilder().setQueryId(queryId).setHits(results.rowCount)

                /* Iterate over results and send them back. */
                val iterator = results.iterator()
                for (record in iterator) {
                    responseObserver.onNext(template.clone().setTuple(recordToTuple(record)).build())
                }
            }

            LOGGER.trace("Sending back ${results.rowCount} rows for position $index of query $queryId took $duration.")
        } else {
            responseObserver.onNext(CottontailGrpc.QueryResultMessage.newBuilder().setHits(0).setQueryId(queryId).build())
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
