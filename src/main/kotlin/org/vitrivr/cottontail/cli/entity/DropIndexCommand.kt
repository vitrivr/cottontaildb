package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt
import io.grpc.StatusException
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.server.grpc.helper.proto
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to drop an index for a specified entity from Cottontail DB.
 *
 * @author Loris Sauter
 * @version 1.0.3
 */
@ExperimentalTime
class DropIndexCommand(private val ddlStub: DDLGrpc.DDLBlockingStub) : AbstractEntityCommand(name = "drop-index", help = "Drops the index on an entity. Usage: entity drop-index <schema>.<entity> <index>") {

    /** Name of the index to drop. */
    private val name: Name.IndexName by argument(
        name = "name",
        help = "The name of the index. Ideally from a previous list-indices call"
    ).convert { this@DropIndexCommand.entityName.index(it) }

    /** Flag indicating whether CLI should ask for confirmation. */
    private val force: Boolean by option(
        "-f",
        "--force",
        help = "Forces the drop and does not ask for confirmation."
    ).convert { it.toBoolean() }.default(false)

    /** Prompt asking for confirmation */
    private val confirm by option().prompt(text = "Do you really want to drop the index ${this.entityName} (y/N)?")

    override fun exec() {
        try {
            if (this.force || this.confirm.toLowerCase() == "y") {
                val timedTable = measureTimedValue {
                    TabulationUtilities.tabulate(
                        this.ddlStub.dropIndex(
                            CottontailGrpc.DropIndexMessage.newBuilder().setIndex(this.name.proto())
                                .build()
                        )
                    )
                }
                println("Successfully dropped index ${this.name} (in ${timedTable.duration}).")
                print(timedTable.value)
            }
        } catch (e: StatusException) {
            println("Failed to remove the index ${this.name} due to error: ${e.message}.")
        }
    }
}