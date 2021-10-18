package org.vitrivr.cottontail.cli.entity

import io.grpc.StatusException
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.AboutEntity
import org.vitrivr.cottontail.database.queries.binding.extensions.proto
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command for accessing and reviewing [org.vitrivr.cottontail.database.entity.DefaultEntity] details.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class AboutEntityCommand(client: SimpleClient) : AbstractCottontailCommand.Entity(client, name = "about", help = "Gives an overview of the entity and its columns.") {
    override fun exec() {
        try {
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(this.client.about(AboutEntity(this.entityName.toString())))
            }
            println("Details for entity ${this.entityName} (took ${timedTable.duration}):")
            print(timedTable.value)
        } catch (e: StatusException) {
            println("Failed to load details for entity ${this.entityName}: ${e.message}.")
        }
    }
}