package org.vitrivr.cottontail.cli.index

import io.grpc.StatusException
import org.vitrivr.cottontail.cli.basics.AbstractIndexCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.RebuildIndex
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to drop an index for a specified entity from Cottontail DB.
 *
 * @author Loris Sauter
 * @version 2.0.0
 */
@ExperimentalTime
class RebuildIndexCommand(client: SimpleClient) : AbstractIndexCommand(client, name = "rebuild", help = "Rebuilds the specified index. Usage: index rebuild <schema>.<entity>.<index>") {
    override fun exec() {
        try {
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(this.client.rebuild(RebuildIndex(this.indexName.toString())))
            }
            println("Successfully rebuilt index ${this.indexName} (in ${timedTable.duration}).")
            print(timedTable.value)
        } catch (e: StatusException) {
            println("Failed to rebuild the index ${this.indexName} due to error: ${e.message}.")
        }
    }
}