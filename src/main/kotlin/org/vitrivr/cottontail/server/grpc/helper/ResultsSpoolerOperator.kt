package org.vitrivr.cottontail.server.grpc.helper

import io.grpc.stub.StreamObserver
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord

/**
 * A [Operator.SinkOperator] used during query execution. Spools the results produced by the parent
 * operator to the gRPC [StreamObserver].
 *
 * @param parent [Operator] that produces the results.
 * @param queryId The ID of the query that produced the results
 * @param index Optional index of the result (for batched queries).
 * @param responseObserver [StreamObserver] used to send back the results.
 */
class ResultsSpoolerOperator(parent: Operator, val queryId: String, val index: Int, val responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) : Operator.SinkOperator(parent) {

    companion object {
        private const val MAX_PAGE_SIZE_BYTES = 4_000_000
    }

    /** The [ColumnDef]s returned by this [ResultsSpoolerOperator]. */
    override val columns: Array<ColumnDef<*>> = this.parent.columns

    override fun toFlow(context: ExecutionEngine.ExecutionContext): Flow<Record> {
        val parent = this.parent.toFlow(context)
        return flow {
            var responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setQueryId(this@ResultsSpoolerOperator.queryId)
            var accumulatedSize = 0L
            parent.collect {
                val tuple = recordToTuple(it).build()
                if (accumulatedSize + tuple.serializedSize >= MAX_PAGE_SIZE_BYTES) {
                    this@ResultsSpoolerOperator.responseObserver.onNext(responseBuilder.build())
                    responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setQueryId(this@ResultsSpoolerOperator.queryId)
                    accumulatedSize = 0L
                }

                /* Add entry to page and increment counter. */
                responseBuilder.addResults(tuple)
                accumulatedSize += tuple.serializedSize
            }

            /* Flush remaining tuples. */
            if (responseBuilder.resultsList.size > 0) {
                this@ResultsSpoolerOperator.responseObserver.onNext(responseBuilder.build())
            }

            /* Signal completion. */
            emit(StandaloneRecord(0L, emptyArray(), emptyArray()))
        }
    }

    /**
     * Generates a new [CottontailGrpc.Tuple.Builder] from a given [Record].
     *
     * @param record [Record] to create a [CottontailGrpc.Tuple.Builder] from.
     * @return Resulting [CottontailGrpc.Tuple.Builder]
     */
    private fun recordToTuple(record: Record): CottontailGrpc.Tuple.Builder = CottontailGrpc.Tuple
            .newBuilder()
            .putAllData(record.toMap().map {
                it.key.name.toString() to (it.value?.toData()
                        ?: CottontailGrpc.Data.newBuilder().setNullData(CottontailGrpc.Null.getDefaultInstance()).build())
            }.toMap())
}
