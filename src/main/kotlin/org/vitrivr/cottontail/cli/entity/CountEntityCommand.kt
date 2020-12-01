package org.vitrivr.cottontail.cli.entity

import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.server.grpc.helper.protoFrom
import kotlin.time.ExperimentalTime

/**
 * Counts the number of rows in an [org.vitrivr.cottontail.database.entity.Entity].
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class CountEntityCommand(dqlStub: CottonDQLGrpc.CottonDQLBlockingStub) : AbstractQueryCommand(name = "count", help = "Counts the number of entries in the given entity. Usage: entity count <schema>.<entity>", stub = dqlStub) {
    override fun exec() {
        val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder()
                        .setFrom(this.entityName.protoFrom())
                        .setProjection(CottontailGrpc.Projection.newBuilder().setOp(CottontailGrpc.Projection.Operation.COUNT).build())
        ).build()

        /* Execute and prepare table. */
        val results = this.execute(qm)
        val table = this.tabulate(results.value)

        /* Print. */
        println("Counting elements of  ${this.entityName} (took: ${results.duration}):")
        println(table)
    }
}