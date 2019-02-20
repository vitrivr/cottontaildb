package ch.unibas.dmi.dbis.cottontail.server.grpc.services

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.execution.ExecutionEngine
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDQLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.server.grpc.helper.DataHelper
import ch.unibas.dmi.dbis.cottontail.server.grpc.helper.GrpcQueryBinder
import ch.unibas.dmi.dbis.cottontail.utilities.math.BitUtil
import io.grpc.Status
import io.grpc.stub.StreamObserver
import java.util.*

internal class CottonDQLService (val catalogue: Catalogue, val engine: ExecutionEngine, val maxMessageSize: Int): CottonDQLGrpc.CottonDQLImplBase() {

    /**
     *
     */
    override fun query(request: CottontailGrpc.QueryMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = try {
        /* Bind query and generate execution plan */
        val binder = GrpcQueryBinder(catalogue = this.catalogue, engine = this.engine)
        val plan = binder.parseAndBind(request.query)

        /* Start the query by giving the start signal. */
        val queryId = request.queryId ?: UUID.randomUUID().toString()
        responseObserver.onNext(CottontailGrpc.QueryResponseMessage.newBuilder().setStart(true).setQueryId(queryId).build())

        /* Execute query. */
        val results = plan.execute()

        /* Calculate batch size based on an example message and the maxMessageSize. */
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

        /* Complete query. */
        responseObserver.onCompleted()
    } catch (e: QueryException.QuerySyntaxException) {
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("${e.message}").asException())
    } catch (e: QueryException.QueryBindException) {
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("${e.message}").asException())
    }






    /**
     * Generates a new [CottontailGrpc.Tuple.Builder] from a given [Record].
     *
     * @param record [Record] to create a [CottontailGrpc.Tuple.Builder] from.
     * @return Resulting [CottontailGrpc.Tuple.Builder]
     */
    private fun recordToTuple(record: Record) : CottontailGrpc.Tuple.Builder = CottontailGrpc.Tuple.newBuilder().putAllData(record.toMap().mapValues { DataHelper.toData(it.value) })
}