package org.vitrivr.cottontail.cli.entity

import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import kotlin.time.ExperimentalTime
import kotlin.time.TimedValue
import kotlin.time.measureTimedValue

/**
 * Base class for commands that issue a query.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
abstract class AbstractQueryCommand(private val stub: CottonDQLGrpc.CottonDQLBlockingStub, name: String, help: String) : AbstractEntityCommand(name, help) {

    /**
     * Takes a [CottontailGrpc.QueryMessage], executes and collects all results into a [List]
     * of [CottontailGrpc.Tuple] and measures the time to execute this action.
     *
     * @param query The query to collect.
     * @return [TimedValue] of the query response.
     */
    protected fun execute(query: CottontailGrpc.QueryMessage): TimedValue<List<CottontailGrpc.Tuple>> = measureTimedValue {
        val tuples = mutableListOf<CottontailGrpc.Tuple>()
        val res = this.stub.query(query)
        res.forEach {
            it.resultsList.forEach {
                tuples.add(it)
            }
        }
        tuples
    }
}