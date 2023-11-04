package org.vitrivr.cottontail.cli.schema

import org.vitrivr.cottontail.cli.basics.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.ListSchemas
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * List all [org.vitrivr.cottontail.dbms.schema.DefaultSchema] stored in Cottontail DB.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class ListAllSchemaCommand(private val client: SimpleClient) : AbstractCottontailCommand(name = "all", help = "Lists all schemas stored in Cottontail DB. Usage: schema all", false) {
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