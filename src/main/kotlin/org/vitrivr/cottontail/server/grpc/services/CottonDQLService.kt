package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.exceptions.OperatorExecutionException
import org.vitrivr.cottontail.execution.operators.basics.ProducingOperator
import org.vitrivr.cottontail.execution.operators.basics.SinkOperator
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.server.grpc.helper.DataHelper
import org.vitrivr.cottontail.server.grpc.helper.GrpcQueryBinder
import kotlin.time.*

/**
 * Implementation of [CottonDQLGrpc.CottonDQLImplBase], the gRPC endpoint for querying data in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.3
 */
@ExperimentalTime
class CottonDQLService(val catalogue: Catalogue, val engine: ExecutionEngine) : CottonDQLGrpc.CottonDQLImplBase() {


    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(CottonDQLService::class.java)
    }

    /** [GrpcQueryBinder] used to generate [org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression] tree from a gRPC query. */
    private val binder = GrpcQueryBinder(catalogue = this@CottonDQLService.catalogue)

    /** [CottontailQueryPlanner] used to generate execution plans from query definitions. */
    private val planner = CottontailQueryPlanner()

    /**
     * gRPC endpoint for handling simple queries.
     */
    override fun query(request: CottontailGrpc.QueryMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = try {
        /* Create a new execution context for the query. */
        val context = this.engine.ExecutionContext()
        val queryId = request.queryId.ifBlank { context.uuid.toString() }

        /* Bind query and create logical plan. */
        val bindTimedValue = measureTimedValue {
            this.binder.parseAndBind(request.query)
        }
        LOGGER.trace("Parsing & binding query ${context.uuid} took ${bindTimedValue.duration}.")

        /* Plan query and create execution plan. */
        val planningTime = measureTime {
            val candidates = this.planner.plan(bindTimedValue.value, 3, 3)
            if (candidates.isEmpty()) {
                responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed because no valid execution plan could be produced").asException())
                return
            }
            val operator = candidates.minByOrNull { it.totalCost }!!.toOperator(context)
            context.addOperator(ResultsSpoolerOperator(operator, context, queryId, 0, responseObserver))
        }
        LOGGER.trace("Planning query ${context.uuid} took $planningTime.")

        /* Schedule query for execution. */
        context.schedule()
    } catch (e: QueryException.QuerySyntaxException) {
        LOGGER.error("Error while executing query $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Query syntax is invalid: ${e.message}").asException())
    } catch (e: QueryException.QueryBindException) {
        LOGGER.error("Error while executing query $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Query binding failed: ${e.message}").asException())
    } catch (e: OperatorExecutionException) {
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
        /* Create a new execution context for the query. */
        val context = this.engine.ExecutionContext()
        val queryId = request.queryId.ifBlank { context.uuid.toString() }

        val totalDuration = measureTime {
            request.queriesList.forEachIndexed { index, query ->
                /* Bind query and create logical plan. */
                val bindTimedValue = measureTimedValue {
                    this.binder.parseAndBind(query)
                }
                LOGGER.trace("Parsing & binding query: $queryId, index: $index took ${bindTimedValue.duration}.")

                /* Plan query and create execution plan. */
                val planTimedValue = measureTimedValue {
                    val candidates = this.planner.plan(bindTimedValue.value, 3, 3)
                    val operator = candidates.minByOrNull { it.totalCost }!!.toOperator(context)
                    context.addOperator(ResultsSpoolerOperator(operator, context, queryId, index, responseObserver))
                }
                LOGGER.trace("Planning query: $queryId, index: $index took ${planTimedValue.duration}.")

                /* Schedule query for execution. */
                context.schedule()
            }
        }

        /* Complete query. */
        LOGGER.info("Batched query $queryId took $totalDuration to complete.")
    } catch (e: QueryException.QuerySyntaxException) {
        LOGGER.error("Error while executing batched query $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Query syntax is invalid: ${e.message}").asException())
    } catch (e: QueryException.QueryBindException) {
        LOGGER.error("Error while executing batched query $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Query binding failed: ${e.message}").asException())
    } catch (e: OperatorExecutionException) {
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
     * A [SinkOperator] that spools the results in the given [Recordset] to the gRPC [StreamObserver].
     *
     * @param parent [ProducingOperator] that produces the results.
     * @param context [ExecutionEngine.ExecutionContext] the [ExecutionEngine.ExecutionContext]
     * @param responseObserver [StreamObserver] used to send back the results.
     * @param index Optional index of the result (for batched queries).
     */
    class ResultsSpoolerOperator(parent: ProducingOperator, context: ExecutionEngine.ExecutionContext, val queryId: String, val index: Int, val responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) : SinkOperator(parent, context) {
        /** The [ColumnDef]s returned by this [ResultsSpoolerOperator]. */
        override val columns: Array<ColumnDef<*>> = this.parent.columns

        /* Size of an individual results page based on the estimate of a single tuple's size. */
        private val pageSize: Int = StandaloneRecord(0L, this.columns, this.columns.map { it.defaultValue() }.toTypedArray()).let {
            (4_194_000_000 / recordToTuple(it).build().serializedSize).toInt()
        }

        /* Number of tuples returned so far. */
        private var counter = 0

        /* Number of tuples returned so far. */
        private var responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setQueryId(this.queryId)

        /** Timestamp of when this [ResultsSpoolerOperator] was created. */
        private var start: Long? = null

        /**
         * Called when [ResultsSpoolerOperator] received a new [Record].
         */
        override fun process(record: Record?) {
            if (record != null) {
                val tuple = recordToTuple(record)
                if (this.counter % this.pageSize == 0) {
                    this.responseObserver.onNext(this.responseBuilder.build())
                    this.responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setQueryId(this.queryId)
                }

                /* Add entry to page and increment counter. */
                this.responseBuilder.addResults(tuple)
                this.counter++
            }
        }

        /**
         * Called when [ResultsSpoolerOperator] is opened.
         */
        override fun prepareOpen() {
            this.start = System.currentTimeMillis()
        }

        /**
         * Called when [ResultsSpoolerOperator] is closed. Sends the last results and completes transmission.
         */
        override fun prepareClose() {
            if (this.responseBuilder.resultsList.size > 0) {
                this.responseObserver.onNext(this.responseBuilder.build())
            }
            this.responseObserver.onCompleted()

            /* Log execution time. */
            val duration = (System.currentTimeMillis() - this.start!!).toDuration(DurationUnit.MILLISECONDS)
            LOGGER.trace("Executing query: $queryId, index: $index took $duration.")
        }

        /**
         * Generates a new [CottontailGrpc.Tuple.Builder] from a given [Record].
         *
         * @param record [Record] to create a [CottontailGrpc.Tuple.Builder] from.
         * @return Resulting [CottontailGrpc.Tuple.Builder]
         */
        private fun recordToTuple(record: Record): CottontailGrpc.Tuple.Builder = CottontailGrpc.Tuple.newBuilder().putAllData(record.toMap().map { it.key.toString() to DataHelper.toData(it.value) }.toMap())
    }
}
