package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.validate
import com.github.ajalt.clikt.parameters.types.long

import org.vitrivr.cottontail.cli.MatchAll
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.server.grpc.helper.protoFrom
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime

/**
 * Command to preview a given entity
 *
 * @author Loris Sauter
 * @version 1.0.1
 */
@ExperimentalTime
class PreviewEntityCommand constructor(dqlStub: CottonDQLGrpc.CottonDQLBlockingStub) : AbstractQueryCommand(name = "preview", help = "Gives a preview of the entity specified", stub = dqlStub) {
    private val limit: Long by option("-l", "--limit", help = "Limits the amount of printed results").long().default(10).validate { require(it > 1) }
    override fun exec() {
        /* Prepare query. */
        val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder()
                        .setFrom(this.entityName.protoFrom())
                        .setProjection(MatchAll())
                        .setLimit(this.limit)
        ).build()

        /* Execute and prepare table. */
        val results = this.execute(qm)
        val table = TabulationUtilities.tabulate(results.value)

        /* Print. */
        println("Previewing ${this.limit} elements of  ${this.entityName} (took: ${results.duration}):")
        println(table)
    }
}