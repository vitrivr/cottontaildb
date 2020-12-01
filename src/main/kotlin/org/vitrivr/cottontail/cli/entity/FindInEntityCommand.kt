package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import org.vitrivr.cottontail.cli.MatchAll
import org.vitrivr.cottontail.cli.Where
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.server.grpc.helper.protoFrom
import kotlin.time.ExperimentalTime

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
@ExperimentalTime
class FindInEntityCommand(dqlStub: CottonDQLGrpc.CottonDQLBlockingStub) : AbstractQueryCommand(name = "find", help = "Find within an entity by column-value specification", stub = dqlStub) {
    val col: String by option("-c", "--column", help = "Column name").required()
    val value: String by option("-v", "--value", help = "The value").required()
    override fun exec() {
        val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder()
                        .setFrom(this.entityName.protoFrom())
                        .setProjection(MatchAll())
                        .setWhere(Where(
                                CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
                                        .setAttribute(col)
                                        .setOp(CottontailGrpc.AtomicLiteralBooleanPredicate.Operator.EQUAL)
                                        .addData(CottontailGrpc.Data.newBuilder().setStringData(value).build())
                                        .build()
                        ))
        ).build()

        /* Execute and prepare table. */
        val results = this.execute(qm)
        val table = this.tabulate(results.value)

        /* Print. */
        println("Found ${results.value.size} elements of ${this.entityName} (took: ${results.duration}):")
        println(table)
    }
}