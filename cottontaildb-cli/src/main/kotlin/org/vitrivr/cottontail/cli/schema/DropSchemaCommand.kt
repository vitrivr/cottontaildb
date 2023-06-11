package org.vitrivr.cottontail.cli.schema

import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import org.vitrivr.cottontail.cli.basics.AbstractSchemaCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.DropSchema
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to drop a [org.vitrivr.cottontail.dbms.schema.DefaultSchema] from Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class DropSchemaCommand(client: SimpleClient) : AbstractSchemaCommand(client, name = "drop", help = "Drops the schema with the given name. Usage: schema drop <name>") {
    /** Flag that can be used to directly provide confirmation. */
    private val confirm: Boolean by option(
        "-c",
        "--confirm",
        help = "Directly provides the confirmation option."
    ).flag()


    override fun exec() {
        if (this.confirm || this.confirm(
                "Do you really want to drop the schema ${this.schemaName} [y/N]?",
                default = false,
                showDefault = false
            ) == true
        ) {
            /* Execute query. */
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(this.client.drop(DropSchema(this.schemaName.toString())))
            }

            /* Output results. */
            println("Schema ${this.schemaName} dropped successfully (took ${timedTable.duration}).")
            print(timedTable.value)
        } else {
            println("Drop schema aborted.")
        }
    }
}