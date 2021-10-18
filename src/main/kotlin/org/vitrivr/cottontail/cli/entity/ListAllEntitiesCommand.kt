package org.vitrivr.cottontail.cli.entity


import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.ListEntities
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * List all [org.vitrivr.cottontail.database.entity.DefaultEntity] stored in Cottontail DB.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class ListAllEntitiesCommand(client: SimpleClient) : AbstractCottontailCommand.Entity(client, name = "all", help = "Lists all entities stored in Cottontail DB.") {
    override fun exec() {
        /* Execute query. */
        val timedTable = measureTimedValue {
            TabulationUtilities.tabulate(this.client.list(ListEntities()))
        }

        /* Output results. */
        println("${timedTable.value.rowCount - 1} entities found (took ${timedTable.duration}).")
        print(timedTable.value)
    }
}