package org.vitrivr.cottontail.cli.schema

import io.grpc.StatusException
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.ListSchemas
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * List all [org.vitrivr.cottontail.database.schema.DefaultSchema] stored in Cottontail DB.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class ListAllSchemaCommand(client: SimpleClient) : AbstractCottontailCommand.Schema(client, name = "all", help = "Lists all schemas stored in Cottontail DB. Usage: schema all", expand = false) {
    override fun exec() {
        /* Execute query. */
        val timedTable = measureTimedValue {
            TabulationUtilities.tabulate(this.client.list(ListSchemas()))
        }

        /* Output results. */
        println("${timedTable.value.rowCount} schemas found (took ${timedTable.duration}).")
        print(timedTable.value)
    }
}