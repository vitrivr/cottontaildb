package org.vitrivr.cottontail.cli.schema

import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.server.grpc.helper.proto
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Command to drop a [org.vitrivr.cottontail.database.schema.Schema] from Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class DropSchemaCommand(private val ddlStub: CottonDDLGrpc.CottonDDLBlockingStub) : AbstractSchemaCommand(name = "drop", help = "Drops the schema with the given name. Usage: schema drop <name>") {

    /** Confirmation input desired from the user. */
    private val confirm by option().prompt(text = "Do you really want to drop the schema ${this.schemaName} (y/N)?")

    override fun exec() {
        if (this.confirm.toLowerCase() == "y") {
            /* Execute query. */
            val time = measureTime {
                this.ddlStub.dropSchema(this.schemaName.proto())
            }

            /* Output results. */
            println("Schema ${this.schemaName} dropped successfully (took $time).")
        } else {
            println("Drop schema aborted.")
        }
    }
}