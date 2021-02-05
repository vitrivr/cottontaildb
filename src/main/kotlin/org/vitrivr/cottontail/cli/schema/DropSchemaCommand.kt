package org.vitrivr.cottontail.cli.schema

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.server.grpc.helper.proto
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to drop a [org.vitrivr.cottontail.database.schema.Schema] from Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class DropSchemaCommand(private val ddlStub: DDLGrpc.DDLBlockingStub) : AbstractSchemaCommand(name = "drop", help = "Drops the schema with the given name. Usage: schema drop <name>") {
    /** Flag indicating whether CLI should ask for confirmation. */
    private val force: Boolean by option(
        "-f",
        "--force",
        help = "Forces the drop and does not ask for confirmation."
    ).convert { it.toBoolean() }.default(false)

    /** Prompt asking for confirmation */
    private val confirm by option().prompt(text = "Do you really want to drop the schema ${this.schemaName} (y/N)?")

    override fun exec() {
        if (this.force || this.confirm.toLowerCase() == "y") {
            /* Execute query. */
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(
                    this.ddlStub.dropSchema(
                        CottontailGrpc.DropSchemaMessage.newBuilder()
                            .setSchema(this.schemaName.proto()).build()
                    )
                )
            }

            /* Output results. */
            println("Schema ${this.schemaName} dropped successfully (took ${timedTable.duration}).")
            print(timedTable.value)
        } else {
            println("Drop schema aborted.")
        }
    }
}