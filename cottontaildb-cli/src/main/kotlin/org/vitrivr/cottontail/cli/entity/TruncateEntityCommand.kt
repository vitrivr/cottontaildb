package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.output.TermUi
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import io.grpc.StatusException
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.TruncateEntity
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to truncate a [org.vitrivr.cottontail.dbms.entity.DefaultEntity] by its name, thus deleting all content.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class TruncateEntityCommand(client: SimpleClient) : AbstractCottontailCommand.Entity(client, name = "truncate", help = "Truncates the given entity. Usage: entity truncate <schema>.<entity>") {

    /** Flag that can be used to directly provide confirmation. */
    private val confirm: Boolean by option(
        "-c",
        "--confirm",
        help = "Directly provides the confirmation option."
    ).flag()

    override fun exec() {
        if (this.confirm || TermUi.confirm("Do you really want to truncate the entity ${this.entityName} [y/N]?", default = false, showDefault = false) == true) {
            try {
                val timedTable = measureTimedValue {
                    TabulationUtilities.tabulate(this.client.truncate(TruncateEntity(this.entityName.toString())))
                }
                println("Successfully truncated entity ${this.entityName} (took ${timedTable.duration}).")
                print(timedTable.value)
            } catch (e: StatusException) {
                println("Failed to truncate entity ${this.entityName} due to error: ${e.message}.")
            }
        } else {
            println("Truncate entity aborted.")
        }
    }
}