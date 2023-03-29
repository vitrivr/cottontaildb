package org.vitrivr.cottontail.cli.schema

import org.vitrivr.cottontail.cli.basics.AbstractSchemaCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.TabulationUtilities
import org.vitrivr.cottontail.utilities.extensions.proto
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to list available entities and schemata
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class ListEntitiesCommand(client: SimpleClient) : AbstractSchemaCommand(client, name = "list", help = "Lists all entities for a given schema. schema list <name>") {
    override fun exec() {
        /* Execute query. */
        val timedTable = measureTimedValue {
            TabulationUtilities.tabulate(this.client.list(CottontailGrpc.ListEntityMessage.newBuilder().setSchema(this.schemaName.proto()).build()))
        }

        /* Output results. */
        println("${timedTable.value.rowCount} entities found for schema ${this.schemaName} (took ${timedTable.duration}).")
        print(timedTable.value)
    }
}