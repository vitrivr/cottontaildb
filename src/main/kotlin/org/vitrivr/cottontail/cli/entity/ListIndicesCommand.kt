package org.vitrivr.cottontail.cli.entity

import io.grpc.StatusException
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.server.grpc.helper.proto
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to list all indices on a specified entity in Cottontail DB.
 *
 * @author Loris Sauter
 * @version 1.0.1
 */
@ExperimentalTime
class ListIndicesCommand(private val ddlStub: DDLGrpc.DDLBlockingStub) : AbstractEntityCommand(name="list-indices", help="Lists the indices on an entity") {
    override fun exec() {
        try {
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(this.ddlStub.listIndexes(CottontailGrpc.ListIndexMessage.newBuilder().setEntity(entityName.proto()).build()))
            }
            println("Showing index information of $entityName (took ${timedTable.duration})")
            print(timedTable.value)
        } catch (e: StatusException) {
            println("Failed to load index information for entity $entityName: ${e.message}.")
        }
    }
}