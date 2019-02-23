package ch.unibas.dmi.dbis.cottontail.server.grpc.services

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.execution.ExecutionEngine
import ch.unibas.dmi.dbis.cottontail.execution.tasks.ExecutionPlanException
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDQLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.server.grpc.helper.DataHelper
import ch.unibas.dmi.dbis.cottontail.server.grpc.helper.GrpcQueryBinder
import ch.unibas.dmi.dbis.cottontail.utilities.math.BitUtil
import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import java.util.*

internal class CottonDQLService (val catalogue: Catalogue, val engine: ExecutionEngine, val maxMessageSize: Int): CottonDQLGrpc.CottonDQLImplBase() {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(CottonDMLService::class.java)
    }

    /**
     * GRPC endpoint for handling simple (non-batched) query requests.
     */
    override fun query(request: CottontailGrpc.QueryMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = try {
        /* Start the query by giving the start signal. */
        val queryId = if (request.queryId == null || request.queryId == "") {
            UUID.randomUUID().toString()
        } else {
            request.queryId
        }

        /* Bind query and generate execution plan */
        val startBinding = System.currentTimeMillis()
        val binder = GrpcQueryBinder(catalogue = this.catalogue, engine = this.engine)
        val plan = binder.parseAndBind(request.query)
        LOGGER.trace("Parsing & binding query $queryId took ${System.currentTimeMillis()-startBinding}ms.")

        /* Start the query by giving the start signal. */
        responseObserver.onNext(CottontailGrpc.QueryResponseMessage.newBuilder().setStart(true).setQueryId(queryId).build())

        /* Execute query. */
        val startExecution = System.currentTimeMillis()
        val results = plan.execute()
        LOGGER.trace("Executing query $queryId took ${System.currentTimeMillis()-startExecution}ms.")

        /* Calculate batch size based on an example message and the maxMessageSize. */
        val startSending = System.currentTimeMillis()
        if (results.rowCount > 0) {
            val exampleSize = BitUtil.nextPowerOfTwo(recordToTuple(results[0]).build().serializedSize)
            val pageSize = (maxMessageSize/exampleSize)
            val maxPages = Math.floorDiv(results.rowCount,pageSize)
            /* Return results. */
            val iterator = results.iterator()
            for (i in 0..maxPages) {
                val responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setStart(false).setPageSize(pageSize).setPage(i).setMaxPage(maxPages).setTotalHits(results.rowCount)
                for (j in i * pageSize until Math.min(results.rowCount, i*pageSize + pageSize)) {
                    responseBuilder.addResults(recordToTuple(iterator.next()))
                }
                responseObserver.onNext(responseBuilder.build())
            }
        }
        LOGGER.trace("Sending back ${results.rowCount} rows for query $queryId took ${System.currentTimeMillis()-startSending}ms.")

        /* Complete query. */
        LOGGER.info("Query $queryId took ${System.currentTimeMillis()-startBinding}ms.")
        responseObserver.onCompleted()
    } catch (e: QueryException.QuerySyntaxException) {
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Query syntax is invalid: ${e.message}").asException())
    } catch (e: QueryException.QueryBindException) {
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Query binding failed: ${e.message}").asException())
    } catch (e: ExecutionPlanException) {
        responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed because execution engine signaled an error: ${e.message}").asException())
    }  catch (e: DatabaseException) {
        responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed because of a database error: ${e.message}").asException())
    } catch (e: Throwable) {
        responseObserver.onError(Status.UNKNOWN.withDescription("Query execution failed failed because of a unknown error: ${e.message}").asException())
    }

    /**
     * GRPC endpoint for handling PING requests.
     */
    override fun ping(request: Empty, responseObserver: StreamObserver<CottontailGrpc.SuccessStatus>) {
        responseObserver.onNext(CottontailGrpc.SuccessStatus.newBuilder().setTimestamp(System.currentTimeMillis()).build())
        responseObserver.onCompleted()
    }

    /**
     * Generates a new [CottontailGrpc.Tuple.Builder] from a given [Record].
     *
     * @param record [Record] to create a [CottontailGrpc.Tuple.Builder] from.
     * @return Resulting [CottontailGrpc.Tuple.Builder]
     */
    private fun recordToTuple(record: Record) : CottontailGrpc.Tuple.Builder = CottontailGrpc.Tuple.newBuilder().putAllData(record.toMap().mapValues { DataHelper.toData(it.value) })
}