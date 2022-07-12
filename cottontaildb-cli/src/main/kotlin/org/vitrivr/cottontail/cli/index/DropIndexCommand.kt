package org.vitrivr.cottontail.cli.index

import com.github.ajalt.clikt.output.TermUi
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import io.grpc.StatusException
import org.vitrivr.cottontail.cli.basics.AbstractIndexCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.DropIndex
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to drop an index for a specified entity from Cottontail DB.
 *
 * @author Loris Sauter
 * @version 2.1.0
 */
@ExperimentalTime
class DropIndexCommand(client: SimpleClient) : AbstractIndexCommand(client, name = "drop", help = "Drops the specified index. Usage: index drop <schema>.<entity>.<index>") {

    /** Flag that can be used to directly provide confirmation. */
    private val confirm: Boolean by option("-c", "--confirm", help = "Directly provides the confirmation option.").flag()

    override fun exec() {
        if (this.confirm || TermUi.confirm("Do you really want to drop the index ${this.indexName} [y/N]?", default = false, showDefault = false) == true) {
            try {
                val timedTable = measureTimedValue {
                    TabulationUtilities.tabulate(this.client.drop(DropIndex(this.indexName.toString())))
                }
                println("Successfully dropped index ${this.indexName} (in ${timedTable.duration}).")
                print(timedTable.value)
            } catch (e: StatusException) {
                println("Failed to remove the index ${this.indexName} due to error: ${e.message}.")
            }
        } else {
            println("Drop index aborted.")
        }
    }
}