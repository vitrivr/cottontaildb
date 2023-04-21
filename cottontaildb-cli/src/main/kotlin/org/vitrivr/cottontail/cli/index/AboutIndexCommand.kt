package org.vitrivr.cottontail.cli.index

import io.grpc.StatusException
import org.vitrivr.cottontail.cli.basics.AbstractIndexCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.AboutIndex
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command for accessing and reviewing index details.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class AboutIndexCommand(client: SimpleClient): AbstractIndexCommand(client, name = "about", help = "Gives an overview of the index and its configuration.") {
    override fun exec() {
        try {
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(this.client.about(AboutIndex(this.indexName.toString())))
            }
            println("Details for index ${this.indexName} (took ${timedTable.duration}):")
            print(timedTable.value)
        } catch (e: StatusException) {
            println("Failed to load details for index ${this.indexName}: ${e.message}.")
        }
    }
}