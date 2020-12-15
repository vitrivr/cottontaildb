package org.vitrivr.cottontail.cli.entity

import com.jakewharton.picnic.Table
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import java.util.*

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
abstract class AbstractQueryCommand(private val stub: DQLGrpc.DQLBlockingStub, name: String, help: String) : AbstractEntityCommand(name, help) {

    /**
     * Takes a [CottontailGrpc.QueryMessage], executes and collects all results into a [List]
     * of [CottontailGrpc.Tuple] and measures the time to execute this action.
     *
     * @param query The query to collect.
     * @return [TimedValue] of the query response.
     */
    protected fun executeAndTabulate(query: CottontailGrpc.QueryMessage): TimedValue<Table> = measureTimedValue {
        TabulationUtilities.tabulate(this.stub.query(query))
    }
}