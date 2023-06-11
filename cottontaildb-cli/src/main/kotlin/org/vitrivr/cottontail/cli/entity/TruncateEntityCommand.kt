package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import io.grpc.StatusException
import org.vitrivr.cottontail.cli.basics.AbstractEntityCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.TruncateEntity
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to truncate a [org.vitrivr.cottontail.dbms.entity.DefaultEntity] by its name, thus deleting all content.
 *
 * @author Ralph Gasser
 * @version 2.0.1
 */
@ExperimentalTime
class TruncateEntityCommand(client: SimpleClient) : AbstractEntityCommand(client, name = "truncate", help = "Truncates the given entity. Usage: entity truncate <schema>.<entity>") {

    companion object{
        /**
         * Truncates an entity. This method has been separated from the [AboutEntityCommand] instance for testability.
         *
         * @param entityName The [Name.EntityName] of the entity to truncate.
         * @param client The [SimpleClient] to use.
         */
        fun truncate(entityName: Name.EntityName, client: SimpleClient) {
            try {
                val timedTable = measureTimedValue {
                    TabulationUtilities.tabulate(client.truncate(TruncateEntity(entityName.toString())))
                }
                println("Successfully truncated entity $entityName (took ${timedTable.duration}).")
                print(timedTable.value)
            } catch (e: StatusException) {
                println("Failed to truncate entity $entityName due to error: ${e.message}.")
            }
        }
    }

    /** Flag that can be used to directly provide confirmation. */
    private val confirm: Boolean by option(
        "-c",
        "--confirm",
        help = "Directly provides the confirmation option. Set to true, no interactive prompt is given"
    ).flag()

    override fun exec()  {
        if (this.confirm || this.confirm("Do you really want to truncate the entity $entityName [y/N]?", default = false, showDefault = false) == true) {
            truncate(this.entityName, this.client)
        } else {
            println("Truncate entity aborted.")
        }
    }
}