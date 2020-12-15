package org.vitrivr.cottontail.cli.entity

import io.grpc.StatusException
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.server.grpc.helper.protoFrom
import kotlin.time.ExperimentalTime

/**
 * Counts the number of rows in an [org.vitrivr.cottontail.database.entity.Entity].
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.2
 */
@ExperimentalTime
class CountEntityCommand(dqlStub: DQLGrpc.DQLBlockingStub) : AbstractQueryCommand(name = "count", help = "Counts the number of entries in the given entity. Usage: entity count <schema>.<entity>", stub = dqlStub) {
    override fun exec() {
        val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder()
                        .setFrom(this.entityName.protoFrom())
                        .setProjection(CottontailGrpc.Projection.newBuilder().setOp(CottontailGrpc.Projection.ProjectionOperation.COUNT).build())
        ).build()

        /* Execute and prepare table. */
        try {
            val results = this.executeAndTabulate(qm)

            /* Print. */
            println("Counting elements of  ${this.entityName} (took: ${results.duration}):")
            print(results.value)
        } catch (e: StatusException) {
            println("Failed to count elements of ${this.entityName} due to error: ${e.message}")
        }
    }
}