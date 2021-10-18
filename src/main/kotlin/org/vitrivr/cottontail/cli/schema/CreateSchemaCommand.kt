package org.vitrivr.cottontail.cli.schema

import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.CreateSchema
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to create a [org.vitrivr.cottontail.database.schema.DefaultSchema] from Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class CreateSchemaCommand(client: SimpleClient) : AbstractCottontailCommand.Schema(client, name = "create", help = "Create the schema with the given name. Usage: schema create <name>") {
    override fun exec() {
        val timedTable = measureTimedValue {
            TabulationUtilities.tabulate(this.client.create(CreateSchema(this.schemaName.toString())))
        }

        /* Output results. */
        println("Schema ${this.schemaName} created successfully (took ${timedTable.duration}).")
        print(timedTable.value)
    }
}