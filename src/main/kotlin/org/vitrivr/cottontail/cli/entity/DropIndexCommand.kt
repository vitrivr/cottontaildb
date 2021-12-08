package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.output.TermUi
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import io.grpc.StatusException
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.DropIndex
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to drop an index for a specified entity from Cottontail DB.
 *
 * @author Loris Sauter
 * @version 2.0.0
 */
@ExperimentalTime
class DropIndexCommand(client: SimpleClient) : AbstractCottontailCommand.Entity(client, name = "drop-index", help = "Drops the index on an entity. Usage: entity drop-index <schema>.<entity> <index>") {

    /** Name of the index to drop. */
    private val indexName: Name.IndexName by argument(
        name = "name",
        help = "The name of the index. Ideally from a previous list-indices call"
    ).convert { this@DropIndexCommand.entityName.index(it) }

    /** Flag that can be used to directly provide confirmation. */
    private val confirm: Boolean by option(
        "-c",
        "--confirm",
        help = "Directly provides the confirmation option."
    ).flag()

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