package ch.unibas.dmi.dbis.cottontail.server.grpc.services

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.execution.ExecutionEngine
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDQLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.server.grpc.helper.DataHelper
import ch.unibas.dmi.dbis.cottontail.server.grpc.helper.GrpcQueryBinder
import io.grpc.Status
import io.grpc.stub.StreamObserver
import java.util.*

internal class CottonDQLService (val catalogue: Catalogue, val engine: ExecutionEngine): CottonDQLGrpc.CottonDQLImplBase() {

    /**
     *
     */
    override fun query(request: CottontailGrpc.QueryMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = try {
        /* Bind query and generate execution plan */
        val binder = GrpcQueryBinder(catalogue = this.catalogue, engine = this.engine)
        val plan = binder.parseAndBind(request.query)
        val batchSize = 100

        /* Start the query by giving the start signal. */
        val queryId = request.queryId ?: UUID.randomUUID().toString()
        responseObserver.onNext(CottontailGrpc.QueryResponseMessage.newBuilder().setTotal(-1).setSize(batchSize).setStart(true).setQueryId(queryId).build())

        /* Execute query. */
        val results = plan.execute()
        val iterator = results.iterator()

        /* Return results. */
        for (i in 0 until (results.rowCount/batchSize)) {
            val responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setStart(false).setSize(batchSize).setTotal(results.rowCount)
            for (j in i * batchSize until Math.min(results.rowCount, i*batchSize + batchSize)) {
                val record = iterator.next()
                val tuple = CottontailGrpc.Tuple.newBuilder()
                tuple.putAllData(record.toMap().mapValues { DataHelper.toData(value = it.value) })
                responseBuilder.addResults(tuple)
            }
            responseObserver.onNext(responseBuilder.build())
        }

        /* Complete query. */
        responseObserver.onCompleted()
    } catch (e: QueryException.QuerySyntaxException) {
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("${e.message}").asException())
    } catch (e: QueryException.QueryBindException) {
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("${e.message}").asException())
    }
}